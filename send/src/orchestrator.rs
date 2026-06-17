use anyhow;
use anyhow::Result;
use jap::{Packet, PacketValue, SeqNum, adjust_rtt, try_share_packets};
use std::{collections::BTreeSet, time::Duration};
use tokio::net::UdpSocket;

/// Struct that is given a designated set of packets to transmit
/// given timeout for waitin for responses from sender
pub struct Orchestrator {
    packets: Vec<Packet>,
    received_acks: BTreeSet<SeqNum>,
    timeout: Duration,
    socket: UdpSocket,
    window: SeqNum,
}

impl Orchestrator {
    pub async fn new(socket: UdpSocket, timeout: Duration) -> Self {
        Self {
            packets: vec![],
            received_acks: BTreeSet::new(),
            timeout,
            socket,
            window: 4,
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
    }

    /// Will transmit packets
    pub async fn transmit(&mut self) -> Result<()> {
        let ids_sent: BTreeSet<SeqNum> = self
            .packets
            .iter()
            .filter_map(|p| (!self.received_acks.contains(&p.seq)).then_some(p.seq))
            .take(self.window as usize)
            .collect();
        if ids_sent.is_empty() {
            return Ok(());
        }
        let (received_packets, time_taken) = try_share_packets(
            self.timeout,
            // send only the unacked packets
            self.packets.iter().filter(|p| ids_sent.contains(&p.seq)),
            &mut self.socket,
            self.window,
        )
        .await?;
        eprintln!("S: Sent IDs {:?}", ids_sent);
        self.timeout = adjust_rtt(self.timeout, time_taken);

        let mut acks_got: SeqNum = 0;
        for packet in received_packets {
            if let Packet {
                seq: _,
                value: PacketValue::Ack(a),
            } = packet
            {
                for seq in a.into_iter().filter(|id| ids_sent.contains(&id)) {
                    if self.received_acks.insert(seq) {
                        eprintln!("Got an Ack {seq}");
                        acks_got += 1;
                    }
                }
            }
        }

        self.window = acks_got;

        Ok(())
    }
}
