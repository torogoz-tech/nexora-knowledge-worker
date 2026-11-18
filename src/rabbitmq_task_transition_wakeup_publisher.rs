use crate::RabbitMqJobWakeupTopology;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ConfirmSelectOptions},
};
use nexora_platform_contracts::{JobKind, JobWakeup, PlatformError};
use serde_json::json;
use std::time::{Duration, SystemTime};

pub(crate) struct RabbitMqTaskTransitionWakeupPublisher {
    _connection: Connection,
    channel: Channel,
}

impl RabbitMqTaskTransitionWakeupPublisher {
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
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|_| PlatformError::Unavailable)?;

        Ok(Self {
            _connection: connection,
            channel,
        })
    }

    pub(crate) async fn publish(&self, run_id: &str) -> Result<(), PlatformError> {
        let wakeup = JobWakeup::new(run_id.to_owned(), JobKind::TaskTransitionResume)
            .map_err(|_| PlatformError::Validation("invalid task transition run identifier"))?;
        let body = serde_json::to_vec(&wakeup).map_err(|_| PlatformError::Storage)?;
        let properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_message_id(run_id.to_owned().into());
        self.publish_confirmed(
            JobKind::TaskTransitionResume.routing_key(),
            &body,
            properties,
        )
        .await
    }

    pub(crate) async fn publish_task_review_wakeup(
        &self,
        job_id: &str,
        available_at: SystemTime,
    ) -> Result<(), PlatformError> {
        let body = serde_json::to_vec(&json!({
            "version": 1,
            "job_id": job_id,
            "kind": "task_review"
        }))
        .map_err(|_| PlatformError::Storage)?;
        let wait = available_at
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_message_id(job_id.to_owned().into());
        let routing_key = if wait.is_zero() {
            JobKind::KnowledgeSynthesis.routing_key()
        } else {
            properties = properties.with_expiration(wait.as_millis().max(1).to_string().into());
            "nexora.jobs.knowledge.retry"
        };
        self.publish_confirmed(routing_key, &body, properties).await
    }

    async fn publish_confirmed(
        &self,
        routing_key: &str,
        body: &[u8],
        properties: BasicProperties,
    ) -> Result<(), PlatformError> {
        let confirmation = self
            .channel
            .basic_publish(
                "nexora.jobs".into(),
                routing_key.into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..BasicPublishOptions::default()
                },
                body,
                properties,
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?
            .await
            .map_err(|_| PlatformError::Unavailable)?;

        match confirmation {
            Confirmation::Ack(None) => Ok(()),
            Confirmation::Ack(Some(_)) | Confirmation::Nack(_) | Confirmation::NotRequested => {
                Err(PlatformError::Unavailable)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nexora_platform_contracts::{JobKind, JobWakeup};

    #[test]
    fn release_wakeup_uses_transition_run_id_and_release_routing_key() {
        let wakeup = JobWakeup::new(
            "run_0123456789abcdef".to_owned(),
            JobKind::TaskTransitionResume,
        )
        .unwrap();
        let payload = serde_json::to_value(&wakeup).unwrap();

        assert_eq!(wakeup.job_id(), "run_0123456789abcdef");
        assert_eq!(wakeup.kind().routing_key(), "nexora.jobs.release");
        assert_eq!(payload["kind"], "task_transition_resume");
    }

    #[test]
    fn task_review_retry_wakeup_preserves_wire_kind_and_identifier() {
        let payload = serde_json::json!({
            "version": 1,
            "job_id": "tcr_a1b2c3",
            "kind": "task_review"
        });
        let parsed = serde_json::from_value::<crate::WorkerJobWakeup>(payload).unwrap();

        assert_eq!(parsed.job_id(), "tcr_a1b2c3");
        assert_eq!(parsed.kind(), "task_review");
    }
}
