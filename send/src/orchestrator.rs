use anyhow::{Result, anyhow};
use jap::{
    Packet, PacketValue, SeqNum, adjust_rtt, check_for_packets, send_packets, try_share_packets,
};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
use tokio::net::UdpSocket;

/// Struct that is given a designated set of packets to transmit
/// given timeout for waitin for responses from sender
#[derive(Debug)]
pub struct Orchestrator {
    packets: Vec<Packet>,
    received_acks: BTreeSet<SeqNum>,
    timeout: Duration,
    socket: UdpSocket,
    window: SeqNum,
    send_window: BTreeSet<SeqNum>,
    last_sent: Instant,
}

impl Orchestrator {
    const INITIAL_WINDOW: SeqNum = 4;

    pub async fn new(socket: UdpSocket, timeout: Duration) -> Self {
        Self {
            packets: vec![],
            received_acks: BTreeSet::new(),
            timeout,
            socket,
            window: Self::INITIAL_WINDOW, // good initial window size
            send_window: BTreeSet::new(),
            last_sent: Instant::now(),
        }
    }

    pub(crate) fn success(&self) -> bool {
        self.packets
            .iter()
            .all(|p| self.received_acks.contains(&p.seq))
    }

    pub fn change_packets(&mut self, list: Vec<Packet>) {
        self.packets = list;
        self.received_acks.clear();
        self.send_window.clear();
        self.window = Self::INITIAL_WINDOW;
        self.timeout = Duration::from_secs_f32(2.2);
    }

    /// Reads packets received on the socket and adjusts the received acks
    pub async fn check(&mut self) {
        let (received_packets, remaining) = check_for_packets(&mut self.socket, 10).await;
        if received_packets.is_empty() {
            return;
        }

        if !remaining.is_empty() {
            eprintln!("Undeserialized bytes {:?}", remaining);
        }

        eprintln!(
            "Got {} packets from the receiver: {:?}",
            received_packets.len(),
            received_packets
        );
        let mut acks_got = BTreeSet::new();
        for packet in received_packets {
            if let PacketValue::Ack { cum } = packet.value {
                for seq in self.packets.iter().map(|p| p.seq).filter(|seq| *seq <= cum) {
                    if self.received_acks.insert(seq) {
                        self.send_window.remove(&seq);
                        acks_got.insert(seq);
                    }
                }
            }
        }

        // self.window = (acks_got.len() as SeqNum).max(1);
        if acks_got.len() > 0 {
            eprintln!(
                "Acks received: {:?}",
                acks_got,
            );
        }
        // self.send_window.clear();
    }

    /// Checks if the orchestrator either hasn't send IDs or has timed out
    pub fn timed_out(&mut self) -> bool {
        let time_since = Instant::now() - self.last_sent;
        if self.send_window.is_empty() {
            true
        } else if time_since > self.timeout {
            for recseq in self.received_acks.iter() {
                if !self.send_window.contains(recseq) {
                    self.window = 1.max(self.window - 1);
                }
            }
            self.timeout *= 2;
            true
        } else {
            false
        }
    }

    /// Will transmit packets
    pub async fn transmit(&mut self) -> Result<()> {
        self.send_window = self
            .packets
            .iter()
            .filter_map(|p| (!self.received_acks.contains(&p.seq)).then_some(p.seq))
            .take(self.window as usize)
            .collect();
        if self.send_window.is_empty() {
            return Err(anyhow!("Doesn't have any IDs to send. {:?}", self));
        }
        self.last_sent = Instant::now();
        eprintln!("Trying to send IDs {:?}", self.send_window);
        send_packets(
            &mut self.socket,
            self.packets
                .iter()
                .filter(|p| self.send_window.contains(&p.seq)),
        )
        .await?;

        Ok(())
    }
}
