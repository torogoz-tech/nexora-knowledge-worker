use crate::{
    RabbitMqJobWakeupDelivery, RabbitMqJobWakeupTopology, TaskKnowledgeJobKind, WorkerJobWakeup,
};
use futures_lite::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicConsumeOptions, BasicQosOptions},
    types::FieldTable,
};
use nexora_platform_contracts::PlatformError;

const KNOWLEDGE_QUEUE: &str = "nexora.jobs.knowledge";

pub(crate) struct RabbitMqJobWakeupConsumer {
    _connection: Connection,
    consumer: lapin::Consumer,
}

impl RabbitMqJobWakeupConsumer {
    pub(crate) async fn connect(uri: &str) -> Result<Self, PlatformError> {
        let connection = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        RabbitMqJobWakeupTopology::declare(&channel).await?;
        channel
            .basic_qos(1, BasicQosOptions::default())
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        let consumer = channel
            .basic_consume(
                KNOWLEDGE_QUEUE.into(),
                "".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?;

        Ok(Self {
            _connection: connection,
            consumer,
        })
    }

    pub(crate) async fn next(
        &mut self,
    ) -> Result<Option<RabbitMqJobWakeupDelivery>, PlatformError> {
        loop {
            let Some(delivery) = self.consumer.next().await else {
                return Ok(None);
            };
            let delivery = delivery.map_err(|_| PlatformError::Unavailable)?;
            let message_id = delivery
                .properties
                .message_id()
                .as_ref()
                .map(|value| value.as_str());
            let wakeup = WorkerJobWakeup::parse(&delivery.data);
            let Some(wakeup) = wakeup else {
                delivery
                    .reject(lapin::options::BasicRejectOptions { requeue: false })
                    .await
                    .map_err(|_| PlatformError::Unavailable)?;
                continue;
            };
            if !Self::valid_wakeup(&delivery.data, message_id) {
                delivery
                    .reject(lapin::options::BasicRejectOptions { requeue: false })
                    .await
                    .map_err(|_| PlatformError::Unavailable)?;
                continue;
            }

            return Ok(Some(RabbitMqJobWakeupDelivery::new(delivery, wakeup)));
        }
    }

    fn valid_wakeup(body: &[u8], message_id: Option<&str>) -> bool {
        WorkerJobWakeup::parse(body).is_some_and(|wakeup| {
            matches!(
                wakeup.kind(),
                "knowledge_synthesis"
                    | "knowledge_processing"
                    | "daily_developer_analysis"
                    | "task_review"
            ) && message_id == Some(wakeup.job_id())
                && (wakeup.kind() != "task_review"
                    || TaskKnowledgeJobKind::from_wakeup(wakeup.job_id()).is_some())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RabbitMqJobWakeupConsumer;
    use nexora_platform_contracts::{JobKind, JobWakeup};
    use serde_json::json;

    #[test]
    fn accepts_only_knowledge_queue_wakeups_with_matching_message_ids() {
        for kind in [
            JobKind::KnowledgeSynthesis,
            JobKind::KnowledgeProcessing,
            JobKind::DailyDeveloperAnalysis,
        ] {
            let wakeup = JobWakeup::new("release_job_1".into(), kind).unwrap();
            let body = serde_json::to_vec(&wakeup).unwrap();
            assert!(RabbitMqJobWakeupConsumer::valid_wakeup(
                &body,
                Some("release_job_1")
            ));
            assert!(!RabbitMqJobWakeupConsumer::valid_wakeup(
                &body,
                Some("wrong_id")
            ));
        }

        for id in ["tcr_a1b2c3", "tgr_a1b2c3"] {
            let body = serde_json::to_vec(&json!({
                "version": 1,
                "job_id": id,
                "kind": "task_review"
            }))
            .unwrap();
            assert!(RabbitMqJobWakeupConsumer::valid_wakeup(&body, Some(id)));
            assert!(!RabbitMqJobWakeupConsumer::valid_wakeup(
                &body,
                Some("different_id")
            ));
        }

        let wakeup = JobWakeup::new("repo_job_1".into(), JobKind::RepositoryIndex).unwrap();
        let body = serde_json::to_vec(&wakeup).unwrap();
        assert!(!RabbitMqJobWakeupConsumer::valid_wakeup(
            &body,
            Some("repo_job_1")
        ));

        for id in ["unknown_1", "tcr_", "tgr"] {
            let body = serde_json::to_vec(&json!({
                "version": 1,
                "job_id": id,
                "kind": "task_review"
            }))
            .unwrap();
            assert!(!RabbitMqJobWakeupConsumer::valid_wakeup(&body, Some(id)));
        }
    }
}
