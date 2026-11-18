use crate::KnowledgeSettings;
use crate::{
    DailyDeveloperAnalysis, DocumentSynthesis, KnowledgeStructuring, OpenAiClient, SecretFile,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use nexora_database_connections::DatabaseConnections;
use nexora_knowledge_store::{
    KnowledgeChunker, KnowledgeEmbeddingRecord, KnowledgeStore, KnowledgeSynthesisJob,
};
use nexora_platform_contracts::{OpenAiConfiguration, PlatformError};
use serde_json::json;
use std::path::Path;
use zeroize::Zeroizing;

pub struct KnowledgeWorker {
    knowledge_store: KnowledgeStore,
    settings: KnowledgeSettings,
}

impl KnowledgeWorker {
    pub fn connect(
        connections: &DatabaseConnections,
        schema: &str,
        settings_path: &Path,
    ) -> Result<Self, PlatformError> {
        Ok(Self {
            knowledge_store: KnowledgeStore::connect(connections, schema)?,
            settings: KnowledgeSettings::load(settings_path)?,
        })
    }

    pub fn schedule_daily(&mut self) -> Result<u64, PlatformError> {
        self.knowledge_store.schedule_daily_developer_analyses()
    }

    pub fn refresh_ratings(&mut self) -> Result<u64, PlatformError> {
        self.knowledge_store.refresh_developer_ratings()
    }

    pub fn run_once(&mut self) -> Result<bool, PlatformError> {
        let Some((client, effort, embedding_model)) =
            Self::client(&mut self.knowledge_store, &self.settings)?
        else {
            return Ok(false);
        };
        let synthesis = Self::synthesize(&mut self.knowledge_store, &client, &effort)?;
        let processing = Self::process_knowledge(
            &mut self.knowledge_store,
            &client,
            &effort,
            &embedding_model,
        )?;
        let daily = Self::analyze_daily(&mut self.knowledge_store, &client, &effort)?;
        Ok(synthesis || processing || daily)
    }

    pub fn drain_pending(&mut self) -> Result<(), PlatformError> {
        while self.run_once()? {}
        Ok(())
    }

    fn client(
        store: &mut KnowledgeStore,
        settings: &KnowledgeSettings,
    ) -> Result<Option<(OpenAiClient, String, String)>, PlatformError> {
        let (key, model, effort, endpoint, embedding_model) =
            if let Some(runtime) = store.openai_service_runtime_settings()? {
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
        store: &mut KnowledgeStore,
        client: &OpenAiClient,
        effort: &str,
    ) -> Result<bool, PlatformError> {
        let Some(job) = store.claim_knowledge_synthesis()? else {
            return Ok(false);
        };
        let input = Self::synthesis_input(&job);
        let body = DocumentSynthesis::run(client, &input, effort);
        store.finish_knowledge_synthesis(&job, body.as_deref().ok())?;
        Ok(true)
    }

    fn process_knowledge(
        store: &mut KnowledgeStore,
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
                &job.source_kind,
                &source.title,
                &source.body,
                effort,
            )
            .map_err(|_| PlatformError::Unavailable)?;
            let chunks = KnowledgeChunker::new(1800, 180)?.chunk(
                &job.resource_id,
                job.source_revision,
                &source.title,
                &source.body,
            )?;
            store.finish_knowledge_processing(&job, &structure, &chunks)?;
            for batch in chunks.chunks(64) {
                let inputs = batch
                    .iter()
                    .map(|chunk| chunk.content.clone())
                    .collect::<Vec<_>>();
                let embeddings = client
                    .embed_many(embedding_model, &inputs)
                    .map_err(|_| PlatformError::Unavailable)?;
                let records = batch
                    .iter()
                    .zip(embeddings)
                    .map(|(chunk, values)| KnowledgeEmbeddingRecord {
                        chunk_id: chunk.id.clone(),
                        content_hash: chunk.content_hash.clone(),
                        values,
                    })
                    .collect::<Vec<_>>();
                store.store_knowledge_embeddings(&job, embedding_model, &records)?;
            }
            if !store.publish_knowledge_embeddings(&job, embedding_model)? {
                return Err(PlatformError::Conflict(
                    "knowledge processing lease expired before publication",
                ));
            }
            Ok(())
        })();
        if let Err(error) = result {
            store.fail_knowledge_processing(&job, &error.to_string())?;
        }
        Ok(true)
    }

    fn analyze_daily(
        store: &mut KnowledgeStore,
        client: &OpenAiClient,
        effort: &str,
    ) -> Result<bool, PlatformError> {
        let Some(job) = store.claim_daily_developer_analysis()? else {
            return Ok(false);
        };
        let input = json!({
            "date": job.date,
            "timezone": job.timezone,
            "language": job.language,
            "evidence": {
                "items": job.evidence["items"],
                "counts": job.evidence["counts"],
                "availability": job.evidence["availability"]
            }
        });
        let report = DailyDeveloperAnalysis::run(client, &input, effort)
            .map_err(|_| PlatformError::Unavailable);
        store.finish_daily_developer_analysis(&job, report.as_ref().ok())?;
        report.map(|_| true)
    }

    fn synthesis_input(job: &KnowledgeSynthesisJob) -> serde_json::Value {
        json!({
            "id": job.id,
            "filename": job.filename,
            "mime_type": job.mime_type,
            "data_base64": STANDARD.encode(&job.payload),
            "source_revision": job.source_revision,
            "language": job.language
        })
    }
}
