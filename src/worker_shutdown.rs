use nexora_platform::PlatformError;

pub(crate) struct WorkerShutdown {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl WorkerShutdown {
    pub(crate) fn new() -> Result<Self, PlatformError> {
        #[cfg(unix)]
        {
            let terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .map_err(|_| PlatformError::Unavailable)?;
            Ok(Self { terminate })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    pub(crate) async fn wait(&mut self) {
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = self.terminate.recv() => {}
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}
