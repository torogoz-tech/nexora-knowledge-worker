use crate::ReasoningError;
use serde_json::Value;

pub(crate) fn completed_output_text(response: &Value) -> Result<String, ReasoningError> {
    if response["status"] != "completed" {
        return Err(ReasoningError::InvalidResponse);
    }

    let mut texts = Vec::new();
    for item in response["output"]
        .as_array()
        .ok_or(ReasoningError::InvalidResponse)?
    {
        match item["type"].as_str() {
            Some("reasoning") => {}
            Some("message") => {
                for content in item["content"]
                    .as_array()
                    .ok_or(ReasoningError::InvalidResponse)?
                {
                    if content["type"] != "output_text" {
                        return Err(ReasoningError::InvalidResponse);
                    }
                    texts.push(
                        content["text"]
                            .as_str()
                            .ok_or(ReasoningError::InvalidResponse)?,
                    );
                }
            }
            _ => return Err(ReasoningError::InvalidResponse),
        }
    }
    if texts.len() != 1 {
        return Err(ReasoningError::InvalidResponse);
    }

    Ok(texts[0].to_owned())
}
