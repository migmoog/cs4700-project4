use anyhow;

use anyhow::Result;

use tokio::{net::UdpSocket, time::sleep};

use std::{collections::BTreeSet, ops::Bound, panic, time::Duration};

use jap::{Ack, Packet, PacketValue, SeqNum, adjust_rtt, try_share_packets};

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

    fn is_ack_valid(&self, a: &Ack) -> bool {
        let min = self.packets.iter().map(|p| p.seq).min().unwrap();
        if a.cum < min {
            return false;
        }

        let max = self.packets.iter().map(|p| p.seq).max().unwrap();
        if a.sel.iter().any(|&r| r < min || r > max) {
            return false;
        }

        true
    }

    fn adjust_acks(&mut self, a: &Ack) {
        let mut didack = false;
        for seq in self.packets.iter().map(|p| p.seq).min().unwrap()..=a.cum {
            if self.received_acks.insert(seq) {
                didack = true;
                eprintln!("Got Ack {}", seq);
            }
        }

        for &sack in a.sel.iter() {
            if self.received_acks.insert(sack) {
                didack = true;
                eprintln!("Got ack {}", sack);
            }
        }

        let received = self.received_acks.len();
        let packets = self.packets.len();
        let seqacks = self.packets.iter().map(|p| p.seq).collect::<Vec<_>>();
        if didack {
            if received > packets {
                panic!(
                    "received acks set exceeds the packets\nreceived: {:?}\npacket_ids {:?}",
                    self.received_acks, seqacks
                );
            }
            eprintln!(
                "Acks adjusted. Received {:?}/{:?}",
                self.received_acks, seqacks
            );
        } else {
            eprintln!(
                "didn't accept this ack packet: {:?}\n{:?}/{:?}",
                a, self.received_acks, seqacks
            );
        }
    }

    /// Will transmit packets
    pub(crate) async fn transmit(&mut self) -> Result<()> {
        let transmitted = 
            self.packets
                .clone()
                .into_iter()
                .filter_map(|p| {
                    if !self.received_acks.contains(&p.seq) {
                        eprintln!("S: Sending unacked {}", p.seq);
                        if let PacketValue::Data { data, .. } = p.value {
                            Some(Packet {
                                seq: p.seq,
                                value: PacketValue::Data {
                                    data: data,
                                    window: self.window,
                                },
                            })
                        } else {
                            Some(p)
                        }
                    } else {
                        None
                    }
                })
                .take(self.window as usize).collect::<Vec<_>>();
        let (received_packets, time_taken) = try_share_packets(
            self.timeout,
            // send only the unacked packets
            transmitted.iter(),
            &mut self.socket,
            self.window,
        )
        .await?;
        self.timeout = adjust_rtt(self.timeout, time_taken);

        for packet in received_packets {
            if let Packet {
                seq: _,
                value: PacketValue::Ack(a),
            } = packet
                && self.is_ack_valid(&a)
            {
                self.window = a.adv_win;
                self.adjust_acks(&a);
            }
        }

        Ok(())
    }
}
