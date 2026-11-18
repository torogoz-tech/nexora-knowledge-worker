use crate::{DocumentSynthesis, OpenAiClient, ReasoningError, http_fixture};
use serde_json::json;
#[test]
fn document_synthesis_sends_original_once_with_bounded_output_and_no_tools() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let task = std::thread::spawn(move || {
        let (socket, _, request) = http_fixture::receive(&listener);
        assert_eq!(request["store"], false);
        assert_eq!(request["tools"], json!([]));
        assert_eq!(request["max_output_tokens"], 1800);
        assert_eq!(
            request["input"][0]["content"][0]["file_data"],
            "data:application/pdf;base64,JVBERi0="
        );
        assert!(
            request["instructions"]
                .as_str()
                .unwrap()
                .contains("untrusted")
        );
        http_fixture::reply(
            socket,
            200,
            json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"## Alcance\nEl servicio registra pedidos."}]}]}),
        );
    });
    let client = OpenAiClient::test_client(format!("http://{address}/responses"));
    let result=DocumentSynthesis::run(&client,&json!({"filename":"manual.pdf","mime_type":"application/pdf","data_base64":"JVBERi0=","language":"es"}),"low").unwrap();
    assert!(result.contains("registra pedidos"));
    task.join().unwrap();
}
#[test]
fn document_budget_is_checked_before_network() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let client = OpenAiClient::test_client(format!(
        "http://{}/responses",
        listener.local_addr().unwrap()
    ));
    assert!(matches!(
        DocumentSynthesis::run(
            &client,
            &json!({"filename":"large.pdf","mime_type":"application/pdf","data_base64":"x".repeat(6_990_509)}),
            "low"
        ),
        Err(ReasoningError::Budget)
    ));
    assert!(listener.accept().is_err());
}
