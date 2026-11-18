use crate::KnowledgeSettings;
use nexora_platform::{InstallationService, OpenAiConfiguration, PlatformError};
use nexora_platform_store::PlatformStore;
use nexora_reasoning::{
    DailyDeveloperAnalysis, DocumentSynthesis, KnowledgeStructuring, OpenAiClient, SecretFile,
};
use nexora_runtime::DatabaseConnections;
use serde_json::json;
use std::path::Path;
use zeroize::Zeroizing;

pub struct KnowledgeWorker {
    store: PlatformStore,
    settings: KnowledgeSettings,
}

impl KnowledgeWorker {
    pub fn connect(
        connections: &DatabaseConnections,
        schema: &str,
        settings_path: &Path,
    ) -> Result<Self, PlatformError> {
        Ok(Self {
            store: PlatformStore::connect(connections, schema)?,
            settings: KnowledgeSettings::load(settings_path)?,
        })
    }

    pub fn schedule_daily(&mut self) -> Result<u64, PlatformError> {
        self.store.schedule_daily_developer_analyses()
    }

    pub fn refresh_ratings(&mut self) -> Result<u64, PlatformError> {
        self.store.refresh_developer_ratings()
    }

    pub fn run_once(&mut self) -> Result<bool, PlatformError> {
        let Some((client, effort, embedding_model)) =
            Self::client(&mut self.store, &self.settings)?
        else {
            return Ok(false);
        };
        let synthesis = Self::synthesize(&mut self.store, &client, &effort)?;
        let processing =
            Self::process_knowledge(&mut self.store, &client, &effort, &embedding_model)?;
        let daily = Self::analyze_daily(&mut self.store, &client, &effort)?;
        Ok(synthesis || processing || daily)
    }

    pub fn drain_pending(&mut self) -> Result<(), PlatformError> {
        while self.run_once()? {}
        Ok(())
    }

    fn client(
        store: &mut PlatformStore,
        settings: &KnowledgeSettings,
    ) -> Result<Option<(OpenAiClient, String, String)>, PlatformError> {
        let (key, model, effort, endpoint, embedding_model) =
            if let Some(runtime) = store.service_runtime_settings(InstallationService::Openai)? {
                let metadata: OpenAiConfiguration =
                    serde_json::from_value(runtime.settings).map_err(|_| PlatformError::Storage)?;
                if !metadata.enabled {
                    return Ok(None);
                }
                let secret = runtime.secret.ok_or(PlatformError::Unavailable)?;
                (
                    Zeroizing::new(
                        String::from_utf8(secret.to_vec()).map_err(|_| PlatformError::Storage)?,
                    ),
                    metadata.model,
                    metadata.effort,
                    metadata.endpoint,
                    metadata.embedding_model,
                )
            } else {
                (
                    SecretFile::read(&settings.key_file).map_err(|_| PlatformError::Unavailable)?,
                    settings.model.clone(),
                    settings.effort.clone(),
                    "https://api.openai.com/v1".to_owned(),
                    "text-embedding-3-small".to_owned(),
                )
            };
        let client = OpenAiClient::new(key, model)
            .and_then(|client| client.with_endpoint(&endpoint))
            .map_err(|_| PlatformError::Unavailable)?;
        Ok(Some((client, effort, embedding_model)))
    }

    fn synthesize(
        store: &mut PlatformStore,
        client: &OpenAiClient,
        effort: &str,
    ) -> Result<bool, PlatformError> {
        let Some(job) = store.claim_knowledge_synthesis()? else {
            return Ok(false);
        };
        let id = job["id"].as_str().ok_or(PlatformError::Storage)?;
        let revision = job["source_revision"]
            .as_i64()
            .ok_or(PlatformError::Storage)?;
        let body = DocumentSynthesis::run(client, &job, effort);
        store.finish_knowledge_synthesis(id, revision, body.as_deref().ok())?;
        Ok(true)
    }

    fn process_knowledge(
        store: &mut PlatformStore,
        client: &OpenAiClient,
        effort: &str,
        embedding_model: &str,
    ) -> Result<bool, PlatformError> {
        let Some(job) = store.claim_knowledge_processing()? else {
            return Ok(false);
        };
        let source = store.knowledge_processing_source(&job)?;
        let result = (|| -> Result<(), PlatformError> {
            let structure = KnowledgeStructuring::run(
                client,
                job["source_kind"].as_str().unwrap_or("knowledge"),
                &source.0,
                &source.1,
                effort,
            )
            .map_err(|_| PlatformError::Unavailable)?;
            store.finish_knowledge_processing(&job, &source.0, &source.1, source.2, &structure)?;
            store.publish_knowledge_embeddings(&job, client, embedding_model)
        })();
        if let Err(error) = result {
            store.fail_knowledge_processing(&job, &error.to_string())?;
        }
        Ok(true)
    }

    fn analyze_daily(
        store: &mut PlatformStore,
        client: &OpenAiClient,
        effort: &str,
    ) -> Result<bool, PlatformError> {
        let Some(job) = store.claim_daily_developer_analysis()? else {
            return Ok(false);
        };
        let input = json!({
            "date": job["date"],
            "timezone": job["timezone"],
            "language": job["language"],
            "evidence": {
                "items": job["evidence"]["items"],
                "counts": job["evidence"]["counts"],
                "availability": job["evidence"]["availability"]
            }
        });
        let report = DailyDeveloperAnalysis::run(client, &input, effort)
            .map_err(|_| PlatformError::Unavailable);
        store.finish_daily_developer_analysis(&job, report.as_ref().ok())?;
        report.map(|_| true)
    }
}
