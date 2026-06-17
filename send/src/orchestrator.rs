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
    ids_sent: BTreeSet<SeqNum>,
    last_received: Instant,
}

impl Orchestrator {
    pub async fn new(socket: UdpSocket, timeout: Duration) -> Self {
        Self {
            packets: vec![],
            received_acks: BTreeSet::new(),
            timeout,
            socket,
            window: 4, // good initial window size
            ids_sent: BTreeSet::new(),
            last_received: Instant::now(),
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
        self.ids_sent.clear();
    }

    /// Reads packets received on the socket and adjusts the received acks
    pub async fn check(&mut self) {
        let (received_packets, remaining) = check_for_packets(&mut self.socket, 10).await;
        if received_packets.is_empty() {
            return;
        }
        self.last_received = Instant::now();

        if !remaining.is_empty() {
            eprintln!("Undeserialized bytes {:?}", remaining);
        }

        eprintln!("Got {} packets from the receiver", received_packets.len());
        let mut acks_got = BTreeSet::new();
        for packet in received_packets {
            if let PacketValue::Ack(set) = packet.value {
                for seq in set.into_iter().filter(|id| self.ids_sent.contains(id)) {
                    if self.received_acks.insert(seq) {
                        acks_got.insert(seq);
                    }
                }
            }
        }

        self.window = acks_got.len() as SeqNum;
        eprintln!(
            "Acks received: {:?}. # of packets dropped: {}",
            acks_got,
            self.ids_sent.len() - acks_got.len()
        );
        self.ids_sent.clear();
    }

    /// Checks if the orchestrator either hasn't send IDs or has timed out
    pub fn timed_out(&self) -> bool {
        self.ids_sent.is_empty() || Instant::now() - self.last_received > self.timeout * 2
    }

    /// Will transmit packets
    pub async fn transmit(&mut self) -> Result<()> {
        self.ids_sent = self
            .packets
            .iter()
            .filter_map(|p| (!self.received_acks.contains(&p.seq)).then_some(p.seq))
            .take(self.window as usize)
            .collect();
        if self.ids_sent.is_empty() {
            return Err(anyhow!("Doesn't have any IDs to send. {:?}", self));
        }
        eprintln!("Trying to send IDs {:?}", self.ids_sent);
        send_packets(
            &mut self.socket,
            self.packets
                .iter()
                .filter(|p| self.ids_sent.contains(&p.seq)),
        )
        .await?;

        Ok(())
    }
}
