use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerJobWakeup {
    version: u16,
    job_id: String,
    kind: String,
}

impl WorkerJobWakeup {
    pub(crate) fn parse(body: &[u8]) -> Option<Self> {
        let wakeup = serde_json::from_slice::<Self>(body).ok()?;
        if wakeup.version != 1
            || wakeup.job_id.is_empty()
            || wakeup.job_id.len() > 128
            || !wakeup
                .job_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        {
            return None;
        }
        Some(wakeup)
    }

    pub(crate) fn job_id(&self) -> &str {
        &self.job_id
    }

    pub(crate) fn kind(&self) -> &str {
        &self.kind
    }
}
