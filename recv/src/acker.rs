use jap::Ack;


pub struct Acker {
    current_ack: Option< Ack >,
}

impl Acker {
    pub fn new() -> Self {
        Self { current_ack: None }
    }
}
