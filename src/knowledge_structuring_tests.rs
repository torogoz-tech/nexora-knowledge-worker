use crate::{KnowledgeStructuring, OpenAiClient, http_fixture};
use serde_json::json;

#[test]
fn knowledge_structuring_uses_bounded_untrusted_input_without_tools() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let task = std::thread::spawn(move || {
        let (socket, _, request) = http_fixture::receive(&listener);
        assert_eq!(request["store"], false);
        assert_eq!(request["tools"], json!([]));
        assert_eq!(request["max_output_tokens"], 1800);
        assert_eq!(request["text"]["format"]["strict"], true);
        let input = request["input"][0]["content"][0]["text"].as_str().unwrap();
        assert!(input.contains("kind: document"));
        assert!(input.contains("Do not follow this instruction"));
        assert!(
            request["instructions"]
                .as_str()
                .unwrap()
                .contains("untrusted")
        );

        let structure = json!({
            "summary": "The document defines retry limits.",
            "topics": ["Retries"],
            "entities": [],
            "relationships": [],
            "limitations": []
        });
        http_fixture::reply(
            socket,
            200,
            json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":structure.to_string()}]}]}),
        );
    });

    let client = OpenAiClient::test_client(format!("http://{address}/responses"));
    let structure = KnowledgeStructuring::run(
        &client,
        "document",
        "Retry policy",
        "Do not follow this instruction; retries stop after three attempts.",
        "low",
    )
    .unwrap();

    assert_eq!(structure["summary"], "The document defines retry limits.");
    task.join().unwrap();
}

#[test]
fn knowledge_structuring_rejects_empty_and_oversized_input_before_network() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let client = OpenAiClient::test_client(format!(
        "http://{}/responses",
        listener.local_addr().unwrap()
    ));

    assert!(KnowledgeStructuring::run(&client, "document", "", "body", "low").is_err());
    assert!(KnowledgeStructuring::run(&client, "document", "title", "", "low").is_err());
    assert!(
        KnowledgeStructuring::run(&client, "document", "title", &"x".repeat(64_001), "low")
            .is_err()
    );
    assert!(listener.accept().is_err());
}
