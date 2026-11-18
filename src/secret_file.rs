use crate::ReasoningError;
use std::{io::Read, path::Path};
use zeroize::Zeroizing;
pub struct SecretFile;
impl SecretFile {
    pub fn read(path: &Path) -> Result<Zeroizing<String>, ReasoningError> {
        let meta = std::fs::symlink_metadata(path).map_err(|_| ReasoningError::Credential)?;
        if !meta.is_file() {
            return Err(ReasoningError::Credential);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                return Err(ReasoningError::Credential);
            }
        }
        let mut value = Zeroizing::new(String::new());
        std::fs::File::open(path)
            .map_err(|_| ReasoningError::Credential)?
            .take(4097)
            .read_to_string(&mut value)
            .map_err(|_| ReasoningError::Credential)?;
        let trimmed = value.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty()
            || trimmed.len() > 4096
            || !trimmed.bytes().all(|c| c.is_ascii_graphic())
        {
            return Err(ReasoningError::Credential);
        }
        Ok(Zeroizing::new(trimmed.to_owned()))
    }
}
