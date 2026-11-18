use nexora_task_workflow::{TaskKnowledgeCompletion, TaskKnowledgeWakeup};
use std::time::SystemTime;

pub(crate) enum TaskReviewProcessingOutcome {
    Completed(TaskKnowledgeCompletion),
    RetryScheduled {
        job_id: String,
        available_at: SystemTime,
        retry_count: i32,
        terminal: bool,
    },
    ClaimDeferred(TaskKnowledgeWakeup),
    ClaimSuperseded,
    Failed,
}
