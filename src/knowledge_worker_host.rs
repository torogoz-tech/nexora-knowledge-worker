use crate::{KnowledgeWorker, RabbitMqJobWakeupConsumer, WorkerShutdown};
use nexora_platform::PlatformError;
use nexora_runtime::DatabaseConnections;
use std::{path::PathBuf, time::Duration};

pub struct KnowledgeWorkerHost;

impl KnowledgeWorkerHost {
    pub async fn run(
        connections: DatabaseConnections,
        schema: String,
        settings: PathBuf,
        once: bool,
    ) -> Result<(), PlatformError> {
        if once {
            return Self::recover(connections, schema, settings).await;
        }
        let uri = std::env::var("NEXORA_RABBITMQ_URL")
            .map_err(|_| PlatformError::Validation("NEXORA_RABBITMQ_URL is required"))?;
        if uri.trim().is_empty() {
            return Err(PlatformError::Validation(
                "NEXORA_RABBITMQ_URL cannot be empty",
            ));
        }

        let mut shutdown = WorkerShutdown::new()?;
        let mut retry_seconds = 1;
        let mut schedule = tokio::time::interval(Duration::from_secs(300));
        schedule.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if Self::recover(connections.clone(), schema.clone(), settings.clone())
                .await
                .is_err()
            {
                eprintln!("nexora-knowledge-worker: PostgreSQL recovery failed");
                if Self::wait_to_retry(&mut shutdown, retry_seconds).await {
                    return Ok(());
                }
                retry_seconds = (retry_seconds * 2).min(30);
                continue;
            }
            let mut consumer = match RabbitMqJobWakeupConsumer::connect(&uri).await {
                Ok(consumer) => consumer,
                Err(_) => {
                    eprintln!("nexora-knowledge-worker: RabbitMQ unavailable");
                    if Self::wait_to_retry(&mut shutdown, retry_seconds).await {
                        return Ok(());
                    }
                    retry_seconds = (retry_seconds * 2).min(30);
                    continue;
                }
            };
            if Self::recover(connections.clone(), schema.clone(), settings.clone())
                .await
                .is_err()
            {
                eprintln!(
                    "nexora-knowledge-worker: PostgreSQL recovery failed after RabbitMQ connection"
                );
                if Self::wait_to_retry(&mut shutdown, retry_seconds).await {
                    return Ok(());
                }
                retry_seconds = (retry_seconds * 2).min(30);
                continue;
            }
            retry_seconds = 1;

            loop {
                tokio::select! {
                    _ = shutdown.wait() => return Ok(()),
                    _ = schedule.tick() => {
                        if Self::schedule_and_recover(connections.clone(), schema.clone(), settings.clone()).await.is_err() {
                            eprintln!("nexora-knowledge-worker: daily knowledge scheduling failed");
                            break;
                        }
                    }
                    result = consumer.next() => {
                        let delivery = match result {
                            Ok(Some(delivery)) => delivery,
                            Ok(None) | Err(_) => {
                                eprintln!("nexora-knowledge-worker: RabbitMQ consumer disconnected");
                                break;
                            }
                        };
                        if Self::drain_pending(connections.clone(), schema.clone(), settings.clone()).await.is_ok() {
                            delivery.acknowledge().await?;
                        } else {
                            delivery.requeue().await?;
                            eprintln!("nexora-knowledge-worker: PostgreSQL processing failed; wake-up requeued");
                            break;
                        }
                    }
                }
            }
            if Self::wait_to_retry(&mut shutdown, retry_seconds).await {
                return Ok(());
            }
            retry_seconds = (retry_seconds * 2).min(30);
        }
    }

    async fn schedule_and_recover(
        connections: DatabaseConnections,
        schema: String,
        settings: PathBuf,
    ) -> Result<(), PlatformError> {
        tokio::task::spawn_blocking(move || {
            let mut worker = KnowledgeWorker::connect(&connections, &schema, &settings)?;
            worker.refresh_ratings()?;
            worker.schedule_daily()?;
            worker.drain_pending()?;
            Ok(())
        })
        .await
        .map_err(|_| PlatformError::Unavailable)?
    }

    async fn drain_pending(
        connections: DatabaseConnections,
        schema: String,
        settings: PathBuf,
    ) -> Result<(), PlatformError> {
        tokio::task::spawn_blocking(move || {
            let mut worker = KnowledgeWorker::connect(&connections, &schema, &settings)?;
            worker.drain_pending()
        })
        .await
        .map_err(|_| PlatformError::Unavailable)?
    }

    async fn recover(
        connections: DatabaseConnections,
        schema: String,
        settings: PathBuf,
    ) -> Result<(), PlatformError> {
        tokio::task::spawn_blocking(move || {
            let mut worker = KnowledgeWorker::connect(&connections, &schema, &settings)?;
            worker.refresh_ratings()?;
            worker.schedule_daily()?;
            worker.drain_pending()
        })
        .await
        .map_err(|_| PlatformError::Unavailable)?
    }

    async fn wait_to_retry(shutdown: &mut WorkerShutdown, seconds: u64) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(seconds)) => false,
            _ = shutdown.wait() => true,
        }
    }
}
