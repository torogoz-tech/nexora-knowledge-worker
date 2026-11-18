use crate::ReasoningError;
use nexora_platform_contracts::OpenAiEndpoint;
use serde_json::Value;
use std::time::Duration;
use zeroize::Zeroizing;
/// Direct OpenAI transport: no redirects, alternate providers or implicit retries.
pub struct OpenAiClient {
    agent: ureq::Agent,
    key: Zeroizing<String>,
    endpoint: String,
    pub(crate) model: String,
}
impl OpenAiClient {
    pub fn new(key: Zeroizing<String>, model: String) -> Result<Self, ReasoningError> {
        if key.is_empty()
            || key.len() > 4096
            || !key.bytes().all(|c| c.is_ascii_graphic())
            || model.is_empty()
            || model.len() > 128
            || !model
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-._:/".contains(&c))
        {
            return Err(ReasoningError::InvalidInput);
        }
        Ok(Self {
            agent: ureq::Agent::config_builder()
                .proxy(None)
                .max_redirects(0)
                .http_status_as_error(false)
                .timeout_global(Some(Duration::from_secs(90)))
                .build()
                .into(),
            key,
            endpoint: "https://api.openai.com/v1/responses".into(),
            model,
        })
    }
    pub fn with_endpoint(mut self, endpoint: &str) -> Result<Self, ReasoningError> {
        self.endpoint =
            OpenAiEndpoint::normalize(endpoint).map_err(|_| ReasoningError::InvalidInput)?;
        Ok(self)
    }
    pub(crate) fn send(&self, body: &Value, timeout: Duration) -> Result<Value, ReasoningError> {
        self.send_to(&self.endpoint, body, timeout)
    }

    pub fn embed(&self, model: &str, input: &str) -> Result<Vec<f32>, ReasoningError> {
        Self::validate_embedding_request(model, std::slice::from_ref(&input.to_owned()))?;
        let embeddings = self.embed_many(model, &[input.to_owned()])?;
        embeddings
            .into_iter()
            .next()
            .ok_or(ReasoningError::InvalidResponse)
    }

    pub fn embed_many(
        &self,
        model: &str,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>, ReasoningError> {
        Self::validate_embedding_request(model, inputs)?;
        let endpoint = self
            .endpoint
            .strip_suffix("/responses")
            .map(|base| format!("{base}/embeddings"))
            .ok_or(ReasoningError::InvalidInput)?;
        let response = self.send_to(
            &endpoint,
            &serde_json::json!({"model":model,"input":inputs,"encoding_format":"float"}),
            Duration::from_secs(90),
        )?;
        let data = response["data"]
            .as_array()
            .ok_or(ReasoningError::InvalidResponse)?;
        if data.len() != inputs.len() {
            return Err(ReasoningError::InvalidResponse);
        }
        let mut embeddings = vec![None; inputs.len()];
        for item in data {
            let index = item["index"]
                .as_u64()
                .and_then(|index| usize::try_from(index).ok())
                .ok_or(ReasoningError::InvalidResponse)?;
            if index >= inputs.len() || embeddings[index].is_some() {
                return Err(ReasoningError::InvalidResponse);
            }
            let embedding = item["embedding"]
                .as_array()
                .ok_or(ReasoningError::InvalidResponse)?
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .filter(|number| number.is_finite())
                        .map(|number| number as f32)
                        .ok_or(ReasoningError::InvalidResponse)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if embedding.is_empty() {
                return Err(ReasoningError::InvalidResponse);
            }
            embeddings[index] = Some(embedding);
        }
        embeddings
            .into_iter()
            .map(|embedding| embedding.ok_or(ReasoningError::InvalidResponse))
            .collect()
    }

    fn validate_embedding_request(model: &str, inputs: &[String]) -> Result<(), ReasoningError> {
        if model.is_empty()
            || model.len() > 128
            || !model
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-._:/".contains(&c))
            || inputs.is_empty()
            || inputs.len() > 64
            || inputs
                .iter()
                .any(|input| input.is_empty() || input.len() > 32_000)
            || inputs.iter().map(String::len).sum::<usize>() > 2_000_000
        {
            return Err(ReasoningError::InvalidInput);
        }
        Ok(())
    }

    fn send_to(
        &self,
        endpoint: &str,
        body: &Value,
        timeout: Duration,
    ) -> Result<Value, ReasoningError> {
        let encoded = serde_json::to_vec(body).map_err(|_| ReasoningError::InvalidInput)?;
        if encoded.len() > 8 * 1024 * 1024 {
            return Err(ReasoningError::Budget);
        }
        let bearer = Zeroizing::new(format!("Bearer {}", self.key.as_str()));
        let mut auth =
            ureq::http::HeaderValue::from_str(&bearer).map_err(|_| ReasoningError::Credential)?;
        auth.set_sensitive(true);
        let mut reply = self
            .agent
            .post(endpoint)
            .config()
            .timeout_global(Some(timeout.min(Duration::from_secs(90))))
            .build()
            .header("Authorization", auth)
            .header("Content-Type", "application/json")
            .send(encoded.as_slice())
            .map_err(|_| ReasoningError::Transport)?;
        if reply.status().as_u16() != 200 {
            return Err(ReasoningError::Http(reply.status().as_u16()));
        }
        let bytes = reply
            .body_mut()
            .with_config()
            .limit(1048576)
            .read_to_vec()
            .map_err(|_| ReasoningError::Transport)?;
        serde_json::from_slice(&bytes).map_err(|_| ReasoningError::InvalidResponse)
    }
    #[cfg(test)]
    pub(crate) fn test_client(endpoint: String) -> Self {
        let mut client = Self::new(
            Zeroizing::new("fixture-secret".into()),
            "fixture-model".into(),
        )
        .unwrap();
        client.endpoint = endpoint;
        client
    }
}
