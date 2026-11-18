use crate::{
    OpenAiClient, TaskKnowledgeJobKind,
    http_fixture::{receive, reply},
};
use serde_json::{Value, json};
use std::net::TcpListener;

#[test]
fn task_review_wakeup_deserializes_and_routes_from_id_prefix() {
    let bytes = serde_json::to_vec(&json!({
        "version": 1,
        "job_id": "tcr_a1b2c3",
        "kind": "task_review"
    }))
    .unwrap();
    let decoded = crate::WorkerJobWakeup::parse(&bytes).unwrap();
    assert_eq!(decoded.kind(), "task_review");
    assert_eq!(
        TaskKnowledgeJobKind::from_wakeup(decoded.job_id()),
        Some(TaskKnowledgeJobKind::TechnicalReview)
    );

    let rating = crate::WorkerJobWakeup::parse(
        br#"{"version":1,"job_id":"tgr_a1b2c3","kind":"task_review"}"#,
    )
    .unwrap();
    assert_eq!(
        TaskKnowledgeJobKind::from_wakeup(rating.job_id()),
        Some(TaskKnowledgeJobKind::ContributorRating)
    );
}

#[test]
fn task_job_identity_requires_matching_prefix_and_kind() {
    assert_eq!(
        TaskKnowledgeJobKind::from_job("tcr_a1b2c3", "task_review"),
        Some(TaskKnowledgeJobKind::TechnicalReview)
    );
    assert_eq!(
        TaskKnowledgeJobKind::from_job("tgr_a1b2c3", "contributor_rating"),
        Some(TaskKnowledgeJobKind::ContributorRating)
    );
    assert_eq!(
        TaskKnowledgeJobKind::from_job("tcr_a1b2c3", "contributor_rating"),
        None
    );
    assert_eq!(TaskKnowledgeJobKind::from_wakeup("unknown_1"), None);
}

#[test]
fn technical_review_inference_uses_fixture_and_rejects_invalid_report() {
    let report = json!({
        "summary": "No demonstrated issue.",
        "findings": [],
        "limitations": ["Static review only."],
        "criteria_results": []
    });
    let (client, request) = fixture_client(response(report.clone()));
    let actual =
        crate::task_code_review::run(&client, &json!({"task":"fixture"}), "medium").unwrap();
    let (_headers, body) = request.join().unwrap();
    assert_eq!(actual, report);
    assert_eq!(body["store"], false);
    assert_eq!(body["tools"], json!([]));
    assert_eq!(body["text"]["format"]["name"], "task_code_review");

    let invalid = json!({
        "summary": "Bad citation.",
        "findings": [{"severity":"P1","title":"Issue","body":"Body","path":"src/lib.rs","line":0,"evidence":"quote"}],
        "limitations": [],
        "criteria_results": []
    });
    let (client, request) = fixture_client(response(invalid));
    assert!(crate::task_code_review::run(&client, &json!({}), "medium").is_err());
    request.join().unwrap();
}

#[test]
fn contributor_rating_inference_uses_fixture_and_rejects_scored_unknown() {
    let report = json!({
        "status": "insufficient_evidence",
        "reason": "There is not enough evidence.",
        "rubric": [],
        "limitations": ["No changed-line evidence was supplied."]
    });
    let (client, request) = fixture_client(response(report.clone()));
    let actual =
        crate::task_contributor_rating::run(&client, &json!({"contributor":"actor"}), "low")
            .unwrap();
    let (_headers, body) = request.join().unwrap();
    assert_eq!(actual, report);
    assert_eq!(body["text"]["format"]["name"], "private_contributor_rating");
    assert_eq!(body["parallel_tool_calls"], false);

    let invalid = json!({
        "status": "insufficient_evidence",
        "reason": "Unknown evidence.",
        "rubric": [{
            "dimension":"security","score":4,"reason":"Unsupported score.",
            "evidence":[{"commit_sha":"abc","path":"src/lib.rs","side":"new","line":1,"quote":"line"}]
        }],
        "limitations": []
    });
    let (client, request) = fixture_client(response(invalid));
    assert!(crate::task_contributor_rating::run(&client, &json!({}), "high").is_err());
    request.join().unwrap();
}

fn fixture_client(response: Value) -> (OpenAiClient, std::thread::JoinHandle<(String, Value)>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/responses", listener.local_addr().unwrap());
    let thread = std::thread::spawn(move || {
        let (socket, headers, body) = receive(&listener);
        reply(socket, 200, response);
        (headers, body)
    });
    let client = OpenAiClient::test_client(endpoint);
    (client, thread)
}

fn response(report: Value) -> Value {
    json!({
        "status":"completed",
        "output":[{"type":"message","content":[{"type":"output_text","text":report.to_string()}]}]
    })
}
