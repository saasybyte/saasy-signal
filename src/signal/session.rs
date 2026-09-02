use std::collections::HashMap;

use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct Participant {
    pub sender: mpsc::Sender<Vec<u8>>,
}

impl Participant {
    pub fn new(sender: mpsc::Sender<Vec<u8>>) -> Self {
        Self { sender }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionState {
    pub participants: HashMap<String, Participant>,
}

impl SessionState {
    pub fn new() -> Self {
        Self {
            participants: HashMap::new(),
        }
    }
}
