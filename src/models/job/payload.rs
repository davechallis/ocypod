use serde_json;

/// Job definition delivered to clients when they take a job from a queue.
#[derive(Debug, Serialize)]
pub struct Payload {
    id: u64,
    input: Option<serde_json::Value>,
}

impl Payload {
    pub fn new(id: u64, input: Option<serde_json::Value>) -> Self {
        Self { id, input }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn input(&self) -> &Option<serde_json::Value> {
        &self.input
    }
}
