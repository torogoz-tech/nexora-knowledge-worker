use crate::{RabbitMqJobWakeupDelivery, RabbitMqJobWakeupTopology};
use futures_lite::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicConsumeOptions, BasicQosOptions},
    types::FieldTable,
};
use nexora_platform::{JobKind, JobWakeup, PlatformError};

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
                JobKind::KnowledgeSynthesis.queue_name().into(),
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
            if !Self::valid_wakeup(&delivery.data, message_id) {
                delivery
                    .reject(lapin::options::BasicRejectOptions { requeue: false })
                    .await
                    .map_err(|_| PlatformError::Unavailable)?;
                continue;
            }

            return Ok(Some(RabbitMqJobWakeupDelivery::new(delivery)));
        }
    }

    fn valid_wakeup(body: &[u8], message_id: Option<&str>) -> bool {
        serde_json::from_slice::<JobWakeup>(body).is_ok_and(|wakeup| {
            matches!(
                wakeup.kind(),
                JobKind::KnowledgeSynthesis
                    | JobKind::KnowledgeProcessing
                    | JobKind::DailyDeveloperAnalysis
            ) && wakeup.kind().queue_name() == JobKind::KnowledgeSynthesis.queue_name()
                && message_id == Some(wakeup.job_id())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RabbitMqJobWakeupConsumer;
    use nexora_platform::{JobKind, JobWakeup};

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

        let wakeup = JobWakeup::new("repo_job_1".into(), JobKind::RepositoryIndex).unwrap();
        let body = serde_json::to_vec(&wakeup).unwrap();
        assert!(!RabbitMqJobWakeupConsumer::valid_wakeup(
            &body,
            Some("repo_job_1")
        ));
    }
}
