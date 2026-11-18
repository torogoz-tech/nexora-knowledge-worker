use crate::{DailyDeveloperAnalysis, OpenAiClient, ReasoningError, http_fixture};
use serde_json::json;
#[test]
fn daily_summary_uses_one_bounded_request_without_tools_or_storage() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let task = std::thread::spawn(move || {
        let (socket, _, request) = http_fixture::receive(&listener);
        assert_eq!(request["max_output_tokens"], 600);
        assert_eq!(request["store"], false);
        assert_eq!(request["tools"], json!([]));
        assert_eq!(request["text"]["format"]["strict"], true);
        assert!(
            request["instructions"]
                .as_str()
                .unwrap()
                .contains("untrusted")
        );
        let report = json!({"summary":"Documented the retry policy."});
        http_fixture::reply(
            socket,
            200,
            json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":report.to_string()}]}]}),
        );
    });
    let client = OpenAiClient::test_client(format!("http://{address}/responses"));
    let input =
        json!({"evidence":{"items":[{"kind":"knowledge","title":"Retry policy"}]},"language":"en"});
    assert_eq!(
        DailyDeveloperAnalysis::run(&client, &input, "low").unwrap()["summary"],
        "Documented the retry policy."
    );
    task.join().unwrap();
}
#[test]
fn empty_and_over_budget_evidence_never_connects() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let client = OpenAiClient::test_client(format!(
        "http://{}/responses",
        listener.local_addr().unwrap()
    ));
    assert!(matches!(
        DailyDeveloperAnalysis::run(&client, &json!({"evidence":{"items":[]}}), "low"),
        Err(ReasoningError::InvalidInput)
    ));
    assert!(matches!(
        DailyDeveloperAnalysis::run(
            &client,
            &json!({"evidence":{"items":[{"detail":"x".repeat(24001)}]}}),
            "low"
        ),
        Err(ReasoningError::Budget)
    ));
    assert!(listener.accept().is_err());
}
#[test]
fn response_validation_rejects_empty_oversized_and_unexpected_fields() {
    assert!(
        DailyDeveloperAnalysis::validate(&json!({"summary":"Documented retry policy."})).is_ok()
    );
    for invalid in [
        json!({"summary":" "}),
        json!({"summary":"x".repeat(2001)}),
        json!({"summary":"Valid","rating":5}),
        json!({"summary":null}),
    ] {
        assert!(DailyDeveloperAnalysis::validate(&invalid).is_err());
    }
}
