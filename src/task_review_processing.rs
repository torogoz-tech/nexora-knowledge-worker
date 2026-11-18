use crate::{OpenAiClient, ReasoningError, TaskKnowledgeJobKind, TaskReviewProcessingOutcome};
use nexora_platform_contracts::PlatformError;
use nexora_task_workflow::{TaskKnowledgeJob, TaskKnowledgeRetry, TaskWorkflowStore};

pub(crate) struct TaskReviewProcessing;

impl TaskReviewProcessing {
    pub(crate) fn process(
        store: &mut TaskWorkflowStore,
        client: &OpenAiClient,
        job: &TaskKnowledgeJob,
        effort: &str,
    ) -> Result<TaskReviewProcessingOutcome, PlatformError> {
        let kind = TaskKnowledgeJobKind::from_job(&job.job_id, &job.job_kind)
            .filter(|kind| kind.matches_job(job))
            .ok_or(PlatformError::Validation(
                "invalid task review job identity",
            ))?;
        let report = match kind {
            TaskKnowledgeJobKind::TechnicalReview => {
                crate::task_code_review::run(client, &job.input, effort)
            }
            TaskKnowledgeJobKind::ContributorRating => {
                crate::task_contributor_rating::run(client, &job.input, effort)
            }
        };
        let report = match report {
            Ok(report) => report,
            Err(error) if is_permanent_reasoning_failure(&error) => {
                return Self::fail_permanently(store, kind, job);
            }
            Err(error) => {
                return Self::schedule_retry(store, kind, job, reasoning_error_code(&error));
            }
        };
        let completion = match kind {
            TaskKnowledgeJobKind::TechnicalReview => store.complete_task_pre_pr_review(
                &job.job_id,
                &job.claim_token,
                &report,
                &client.model,
            ),
            TaskKnowledgeJobKind::ContributorRating => store.complete_task_contributor_rating(
                &job.job_id,
                &job.claim_token,
                &report,
                &client.model,
            ),
        };
        match completion {
            Ok(completion) => Ok(TaskReviewProcessingOutcome::Completed(completion)),
            Err(PlatformError::Validation(_)) => Self::fail_permanently(store, kind, job),
            Err(PlatformError::Conflict("task review claim was superseded"))
            | Err(PlatformError::Conflict("contributor assessment claim was superseded"))
            | Err(PlatformError::Conflict("private rating claim was superseded")) => {
                Ok(TaskReviewProcessingOutcome::ClaimSuperseded)
            }
            Err(PlatformError::Conflict(_)) => Self::fail_permanently(store, kind, job),
            Err(_) => Self::schedule_retry(store, kind, job, "task_review_completion_failed"),
        }
    }

    fn schedule_retry(
        store: &mut TaskWorkflowStore,
        kind: TaskKnowledgeJobKind,
        job: &TaskKnowledgeJob,
        error_code: &'static str,
    ) -> Result<TaskReviewProcessingOutcome, PlatformError> {
        let retry = match kind {
            TaskKnowledgeJobKind::TechnicalReview => store.retry_task_pre_pr_review(
                &job.job_id,
                &job.claim_token,
                error_code,
                "Task review provider processing failed.",
            ),
            TaskKnowledgeJobKind::ContributorRating => store.retry_task_contributor_rating(
                &job.job_id,
                &job.claim_token,
                error_code,
                "Private contributor assessment provider processing failed.",
            ),
        }?;
        Ok(Self::retry_outcome(&job.job_id, retry))
    }

    fn fail_permanently(
        store: &mut TaskWorkflowStore,
        kind: TaskKnowledgeJobKind,
        job: &TaskKnowledgeJob,
    ) -> Result<TaskReviewProcessingOutcome, PlatformError> {
        match kind {
            TaskKnowledgeJobKind::TechnicalReview => store.fail_task_pre_pr_review(
                &job.job_id,
                &job.claim_token,
                "invalid_task_review_output",
                "The task review input or generated report failed validation.",
            )?,
            TaskKnowledgeJobKind::ContributorRating => store.fail_task_contributor_rating(
                &job.job_id,
                &job.claim_token,
                "invalid_contributor_rating_output",
                "Private contributor assessment input or output failed validation.",
            )?,
        };
        Ok(TaskReviewProcessingOutcome::Failed)
    }

    fn retry_outcome(job_id: &str, retry: TaskKnowledgeRetry) -> TaskReviewProcessingOutcome {
        TaskReviewProcessingOutcome::RetryScheduled {
            job_id: job_id.to_owned(),
            available_at: retry.available_at,
            retry_count: retry.retry_count,
            terminal: retry.terminal,
        }
    }
}

fn reasoning_error_code(error: &ReasoningError) -> &'static str {
    match error {
        ReasoningError::Credential => "task_review_credential_unavailable",
        ReasoningError::Transport => "task_review_provider_transport_failed",
        ReasoningError::Http(_) => "task_review_provider_http_failed",
        ReasoningError::Budget => "task_review_provider_budget_exhausted",
        ReasoningError::InvalidInput | ReasoningError::InvalidResponse => {
            "task_review_validation_failed"
        }
    }
}

fn is_permanent_reasoning_failure(error: &ReasoningError) -> bool {
    match error {
        ReasoningError::InvalidInput
        | ReasoningError::Credential
        | ReasoningError::InvalidResponse
        | ReasoningError::Budget => true,
        ReasoningError::Http(status) => !matches!(status, 408 | 425 | 429 | 500..=599),
        ReasoningError::Transport => false,
    }
}

#[cfg(test)]
mod tests {
    use super::is_permanent_reasoning_failure;
    use crate::ReasoningError;

    #[test]
    fn validation_and_configuration_errors_are_terminal() {
        for error in [
            ReasoningError::InvalidInput,
            ReasoningError::Credential,
            ReasoningError::InvalidResponse,
            ReasoningError::Budget,
            ReasoningError::Http(401),
            ReasoningError::Http(422),
        ] {
            assert!(is_permanent_reasoning_failure(&error));
        }
    }

    #[test]
    fn transient_provider_failures_use_durable_retries() {
        for error in [
            ReasoningError::Transport,
            ReasoningError::Http(408),
            ReasoningError::Http(425),
            ReasoningError::Http(429),
            ReasoningError::Http(503),
        ] {
            assert!(!is_permanent_reasoning_failure(&error));
        }
    }
}
