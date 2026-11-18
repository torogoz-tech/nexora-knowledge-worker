use nexora_task_workflow::TaskKnowledgeJob;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskKnowledgeJobKind {
    TechnicalReview,
    ContributorRating,
}

impl TaskKnowledgeJobKind {
    pub(crate) fn from_job(id: &str, kind: &str) -> Option<Self> {
        if Self::valid_id_prefix(id, "tcr") && kind == "task_review" {
            return Some(Self::TechnicalReview);
        }
        if Self::valid_id_prefix(id, "tgr") && kind == "contributor_rating" {
            return Some(Self::ContributorRating);
        }
        None
    }

    pub(crate) fn from_wakeup(job_id: &str) -> Option<Self> {
        if Self::valid_id_prefix(job_id, "tcr") {
            return Some(Self::TechnicalReview);
        }
        if Self::valid_id_prefix(job_id, "tgr") {
            return Some(Self::ContributorRating);
        }
        None
    }

    fn valid_id_prefix(id: &str, prefix: &str) -> bool {
        id.strip_prefix(prefix)
            .and_then(|suffix| suffix.strip_prefix('_'))
            .is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
    }

    pub(crate) fn matches_job(self, job: &TaskKnowledgeJob) -> bool {
        Self::from_job(&job.job_id, &job.job_kind) == Some(self)
    }
}
