use nexora_platform::PlatformError;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeSettings {
    pub key_file: PathBuf,
    pub model: String,
    pub effort: String,
}

impl KnowledgeSettings {
    pub fn load(path: &Path) -> Result<Self, PlatformError> {
        let bytes = std::fs::read(path)
            .map_err(|_| PlatformError::Validation("reasoning settings unavailable"))?;
        if bytes.len() > 8192 {
            return Err(PlatformError::Validation("reasoning settings too large"));
        }
        let settings: Self = serde_json::from_slice(&bytes)
            .map_err(|_| PlatformError::Validation("invalid reasoning settings"))?;
        if !settings.key_file.is_absolute()
            || settings.model.is_empty()
            || settings.model.len() > 128
            || !matches!(settings.effort.as_str(), "low" | "medium" | "high")
        {
            return Err(PlatformError::Validation("invalid reasoning settings"));
        }
        Ok(settings)
    }
}
