use crate::{
    KnowledgeWorker, RabbitMqJobWakeupConsumer, RabbitMqTaskTransitionWakeupPublisher,
    TaskReviewProcessingOutcome, WorkerShutdown,
};
use nexora_database_connections::DatabaseConnections;
use nexora_platform_contracts::PlatformError;
use nexora_task_workflow::{TaskKnowledgeWakeup, TaskTransitionWakeup, TaskWorkflowStore};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    future::pending,
    path::PathBuf,
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

type LeaseTimer = Reverse<(Instant, String)>;

pub struct KnowledgeWorkerHost;

impl KnowledgeWorkerHost {
    pub async fn run(
        connections: DatabaseConnections,
        schema: String,
        settings: PathBuf,
        once: bool,
    ) -> Result<(), PlatformError> {
        let uri = std::env::var("NEXORA_RABBITMQ_URL")
            .map_err(|_| PlatformError::Validation("NEXORA_RABBITMQ_URL is required"))?;
        if uri.trim().is_empty() {
            return Err(PlatformError::Validation(
                "NEXORA_RABBITMQ_URL cannot be empty",
            ));
        }
        if once {
            Self::recover(connections.clone(), schema.clone(), settings.clone()).await?;
            let publisher = RabbitMqTaskTransitionWakeupPublisher::connect(&uri).await?;
            Self::publish_pending_task_wakeups(
                connections.clone(),
                schema.clone(),
                &publisher,
                None,
            )
            .await?;
            return Self::publish_ready_transitions(connections, schema, &publisher).await;
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
                    eprintln!("nexora-knowledge-worker: RabbitMQ consumer unavailable");
                    if Self::wait_to_retry(&mut shutdown, retry_seconds).await {
                        return Ok(());
                    }
                    retry_seconds = (retry_seconds * 2).min(30);
                    continue;
                }
            };
            let publisher = match RabbitMqTaskTransitionWakeupPublisher::connect(&uri).await {
                Ok(publisher) => publisher,
                Err(_) => {
                    eprintln!("nexora-knowledge-worker: RabbitMQ publisher unavailable");
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

            let mut lease_timers = BinaryHeap::new();
            if Self::publish_pending_task_wakeups(
                connections.clone(),
                schema.clone(),
                &publisher,
                Some(&mut lease_timers),
            )
            .await
            .is_err()
                || Self::publish_ready_transitions(connections.clone(), schema.clone(), &publisher)
                    .await
                    .is_err()
            {
                eprintln!("nexora-knowledge-worker: RabbitMQ startup recovery failed");
                if Self::wait_to_retry(&mut shutdown, retry_seconds).await {
                    return Ok(());
                }
                retry_seconds = (retry_seconds * 2).min(30);
                continue;
            }
            retry_seconds = 1;

            loop {
                let timer_deadline = lease_timers.peek().map(|timer| timer.0.0);
                let timer_wait = async move {
                    match timer_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => pending::<()>().await,
                    }
                };
                tokio::select! {
                    _ = shutdown.wait() => return Ok(()),
                    _ = schedule.tick() => {
                        if Self::schedule_knowledge_only(connections.clone(), schema.clone(), settings.clone()).await.is_err() {
                            eprintln!("nexora-knowledge-worker: daily knowledge scheduling failed");
                            break;
                        }
                    }
                    _ = timer_wait => {
                        if Self::publish_due_lease_timers(&mut lease_timers, &publisher).await.is_err() {
                            eprintln!("nexora-knowledge-worker: lease-expiry wake publication failed");
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
                        let task_review_job_id = delivery.task_review_job_id().map(str::to_owned);
                        match Self::drain_wakeup(connections.clone(), schema.clone(), settings.clone(), task_review_job_id).await {
                            Ok(outcomes) => {
                                match Self::publish_outcomes(
                                    outcomes,
                                    connections.clone(),
                                    schema.clone(),
                                    &publisher,
                                    &mut lease_timers,
                                ).await {
                                    Ok(()) => delivery.acknowledge().await?,
                                    Err(_) => {
                                        delivery.requeue().await?;
                                        eprintln!("nexora-knowledge-worker: task wake publication failed; durable intent will be recovered");
                                        break;
                                    }
                                }
                            }
                            Err(_) => {
                                delivery.requeue().await?;
                                eprintln!("nexora-knowledge-worker: PostgreSQL processing failed; wake-up requeued");
                                break;
                            }
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

    async fn schedule_knowledge_only(
        connections: DatabaseConnections,
        schema: String,
        settings: PathBuf,
    ) -> Result<(), PlatformError> {
        tokio::task::spawn_blocking(move || {
            let mut worker = KnowledgeWorker::connect(&connections, &schema, &settings)?;
            worker.refresh_ratings()?;
            worker.schedule_daily()?;
            worker.drain_scheduled()
        })
        .await
        .map_err(|_| PlatformError::Unavailable)?
    }

    async fn drain_wakeup(
        connections: DatabaseConnections,
        schema: String,
        settings: PathBuf,
        task_review_job_id: Option<String>,
    ) -> Result<Vec<TaskReviewProcessingOutcome>, PlatformError> {
        tokio::task::spawn_blocking(move || {
            let mut worker = KnowledgeWorker::connect(&connections, &schema, &settings)?;
            worker.drain_wakeup(task_review_job_id.as_deref())
        })
        .await
        .map_err(|_| PlatformError::Unavailable)?
    }

    async fn publish_outcomes(
        outcomes: Vec<TaskReviewProcessingOutcome>,
        connections: DatabaseConnections,
        schema: String,
        publisher: &RabbitMqTaskTransitionWakeupPublisher,
        lease_timers: &mut BinaryHeap<LeaseTimer>,
    ) -> Result<(), PlatformError> {
        for outcome in outcomes {
            match outcome {
                TaskReviewProcessingOutcome::Completed(completion) => {
                    if completion.transition_ready {
                        Self::publish_ready_transitions(
                            connections.clone(),
                            schema.clone(),
                            publisher,
                        )
                        .await?;
                    }
                }
                TaskReviewProcessingOutcome::RetryScheduled {
                    job_id,
                    available_at,
                    retry_count,
                    terminal,
                } => {
                    if terminal {
                        eprintln!(
                            "nexora-knowledge-worker: task review retry limit reached ({retry_count})"
                        );
                    } else {
                        publisher
                            .publish_task_review_wakeup(&job_id, available_at)
                            .await?;
                    }
                }
                TaskReviewProcessingOutcome::ClaimDeferred(wakeup) => {
                    Self::schedule_deferred_wakeup(wakeup, publisher, lease_timers).await?;
                }
                TaskReviewProcessingOutcome::ClaimSuperseded
                | TaskReviewProcessingOutcome::Failed => {}
            }
        }
        Ok(())
    }

    async fn schedule_deferred_wakeup(
        wakeup: TaskKnowledgeWakeup,
        publisher: &RabbitMqTaskTransitionWakeupPublisher,
        lease_timers: &mut BinaryHeap<LeaseTimer>,
    ) -> Result<(), PlatformError> {
        if Self::is_live_claim(&wakeup, SystemTime::now()) {
            Self::schedule_lease_timer(lease_timers, wakeup.job_id, wakeup.available_at);
            return Ok(());
        }
        publisher
            .publish_task_review_wakeup(&wakeup.job_id, wakeup.available_at)
            .await
    }

    async fn publish_pending_task_wakeups(
        connections: DatabaseConnections,
        schema: String,
        publisher: &RabbitMqTaskTransitionWakeupPublisher,
        mut lease_timers: Option<&mut BinaryHeap<LeaseTimer>>,
    ) -> Result<(), PlatformError> {
        let wakeups = tokio::task::spawn_blocking(move || {
            let mut store = TaskWorkflowStore::connect(&connections, &schema)?;
            store.pending_task_knowledge_wakeups()
        })
        .await
        .map_err(|_| PlatformError::Unavailable)??;
        for wakeup in wakeups {
            if Self::is_live_claim(&wakeup, SystemTime::now()) {
                if let Some(timers) = lease_timers.as_deref_mut() {
                    Self::schedule_lease_timer(timers, wakeup.job_id, wakeup.available_at);
                } else {
                    publisher
                        .publish_task_review_wakeup(&wakeup.job_id, wakeup.available_at)
                        .await?;
                }
            } else {
                publisher
                    .publish_task_review_wakeup(&wakeup.job_id, wakeup.available_at)
                    .await?;
            }
        }
        Ok(())
    }

    fn schedule_lease_timer(
        timers: &mut BinaryHeap<LeaseTimer>,
        job_id: String,
        available_at: SystemTime,
    ) {
        let delay = available_at
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        let deadline = Instant::now() + delay.max(Duration::from_millis(1));
        timers.push(Reverse((deadline, job_id)));
    }

    fn is_live_claim(wakeup: &TaskKnowledgeWakeup, now: SystemTime) -> bool {
        wakeup.status == "running" && wakeup.available_at > now
    }

    async fn publish_due_lease_timers(
        timers: &mut BinaryHeap<LeaseTimer>,
        publisher: &RabbitMqTaskTransitionWakeupPublisher,
    ) -> Result<(), PlatformError> {
        while timers
            .peek()
            .is_some_and(|timer| timer.0.0 <= Instant::now())
        {
            let Some(Reverse((_, job_id))) = timers.pop() else {
                continue;
            };
            publisher
                .publish_task_review_wakeup(&job_id, SystemTime::now())
                .await?;
        }
        Ok(())
    }

    async fn publish_ready_transitions(
        connections: DatabaseConnections,
        schema: String,
        publisher: &RabbitMqTaskTransitionWakeupPublisher,
    ) -> Result<(), PlatformError> {
        loop {
            let wakeups =
                Self::pending_transition_wakeups(connections.clone(), schema.clone()).await?;
            if wakeups.is_empty() {
                return Ok(());
            }
            let mut stale_generation = false;
            for wakeup in wakeups {
                publisher.publish(&wakeup.run_id).await?;
                let acknowledged = Self::acknowledge_transition_wakeup(
                    connections.clone(),
                    schema.clone(),
                    wakeup,
                )
                .await?;
                stale_generation |= !acknowledged;
            }
            if !stale_generation {
                return Ok(());
            }
        }
    }

    async fn pending_transition_wakeups(
        connections: DatabaseConnections,
        schema: String,
    ) -> Result<Vec<TaskTransitionWakeup>, PlatformError> {
        tokio::task::spawn_blocking(move || {
            let mut store = TaskWorkflowStore::connect(&connections, &schema)?;
            store.pending_task_transition_wakeups()
        })
        .await
        .map_err(|_| PlatformError::Unavailable)?
    }

    async fn acknowledge_transition_wakeup(
        connections: DatabaseConnections,
        schema: String,
        wakeup: TaskTransitionWakeup,
    ) -> Result<bool, PlatformError> {
        tokio::task::spawn_blocking(move || {
            let mut store = TaskWorkflowStore::connect(&connections, &schema)?;
            store.acknowledge_task_transition_wakeup(&wakeup.run_id, wakeup.generation)
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
            let _ = worker.drain_task_knowledge_pending()?;
            worker.drain_scheduled()
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

#[cfg(test)]
mod tests {
    use super::KnowledgeWorkerHost;
    use nexora_task_workflow::TaskKnowledgeWakeup;
    use std::{
        cmp::Reverse,
        collections::BinaryHeap,
        time::{Duration, SystemTime},
    };
    use tokio::time::Instant;

    fn wakeup(status: &str, available_at: SystemTime) -> TaskKnowledgeWakeup {
        TaskKnowledgeWakeup {
            job_id: "tcr_a1b2c3".to_owned(),
            job_kind: "task_review".to_owned(),
            status: status.to_owned(),
            run_id: "run_a1b2c3".to_owned(),
            task_id: "tsk_a1b2c3".to_owned(),
            available_at,
        }
    }

    #[test]
    fn only_live_running_claims_use_one_shot_lease_timers() {
        let now = SystemTime::now();
        let future = now + Duration::from_secs(30);

        assert!(KnowledgeWorkerHost::is_live_claim(
            &wakeup("running", future),
            now
        ));
        assert!(!KnowledgeWorkerHost::is_live_claim(
            &wakeup("queued", future),
            now
        ));
        assert!(!KnowledgeWorkerHost::is_live_claim(
            &wakeup("running", now - Duration::from_secs(1)),
            now
        ));
    }

    #[test]
    fn startup_timer_recovery_keeps_job_id_and_deadline_in_one_shot_queue() {
        let mut timers = BinaryHeap::<Reverse<(Instant, String)>>::new();
        let available_at = SystemTime::now() + Duration::from_secs(30);
        KnowledgeWorkerHost::schedule_lease_timer(
            &mut timers,
            "tgr_a1b2c3".to_owned(),
            available_at,
        );

        let Some(Reverse((deadline, job_id))) = timers.pop() else {
            panic!("lease timer was not scheduled");
        };
        assert_eq!(job_id, "tgr_a1b2c3");
        assert!(deadline > Instant::now());
    }
}
