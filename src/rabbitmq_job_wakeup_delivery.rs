use crate::WorkerJobWakeup;
use lapin::{message::Delivery, options::BasicAckOptions, options::BasicNackOptions};
use nexora_platform_contracts::PlatformError;

pub(crate) struct RabbitMqJobWakeupDelivery {
    delivery: Delivery,
    wakeup: WorkerJobWakeup,
}

impl RabbitMqJobWakeupDelivery {
    pub(crate) fn new(delivery: Delivery, wakeup: WorkerJobWakeup) -> Self {
        Self { delivery, wakeup }
    }

    pub(crate) fn task_review_job_id(&self) -> Option<&str> {
        (self.wakeup.kind() == "task_review").then_some(self.wakeup.job_id())
    }

    pub(crate) async fn acknowledge(self) -> Result<(), PlatformError> {
        self.delivery
            .ack(BasicAckOptions::default())
            .await
            .map(|_| ())
            .map_err(|_| PlatformError::Unavailable)
    }

    pub(crate) async fn requeue(self) -> Result<(), PlatformError> {
        self.delivery
            .nack(BasicNackOptions {
                requeue: true,
                ..Default::default()
            })
            .await
            .map(|_| ())
            .map_err(|_| PlatformError::Unavailable)
    }
}
