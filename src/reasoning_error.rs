#[derive(Debug, thiserror::Error)]
pub enum ReasoningError {
    #[error("invalid reasoning configuration or input")]
    InvalidInput,
    #[error("private credential file unavailable or insecure")]
    Credential,
    #[error("external service transport failed")]
    Transport,
    #[error("external service returned HTTP {0}")]
    Http(u16),
    #[error("invalid, refused or incomplete model response")]
    InvalidResponse,
    #[error("reasoning budget exceeded")]
    Budget,
}
