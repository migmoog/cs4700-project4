use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::Packet;

use super::SeqNum;

use derive_more::Deref;
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Deref)]
pub struct FileData(pub Vec<u8>);

impl Debug for FileData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FileData")
            .field(&String::from_utf8_lossy(&self.0))
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Ack {
    /// The cumulative ack.
    /// AKA the highest packet ID of ones received contiguously
    pub cum: SeqNum,

    /// The SACK, set of ranges that represent
    /// the package IDs received by the receiver
    pub sel: BTreeSet<SeqNum>,

    /// Advertised window of packets capable of being received
    pub adv_win: SeqNum,
}

impl Ack {
    pub fn add_seq(&mut self, new_seq: SeqNum) -> bool {
        if new_seq <= self.cum {
            return false;
        }

        if self.cum + 1 == new_seq {
            self.cum = new_seq;
            self.sel.retain(|&seq| seq > self.cum);
            true
        } else {
            self.sel.insert(new_seq)
        }
    }

    pub fn from_packets<'a>(packets: &'a [Packet], adv_win: SeqNum) -> Self {
        let min = packets.iter().map(|p| p.seq).min().unwrap();
        let mut out = Self {
            cum: min,
            sel: Default::default(),
            adv_win,
        };

        for p in packets {
            if out.add_seq(p.seq) {
                eprintln!("Added {} to ack", p.seq);
            }
        }

        out
    }
}

/// represents data being sent over a socket,
/// as well as ACKs
#[derive(Serialize, Deserialize, Debug)]
pub enum PacketValue {
    // Sender gives this to the receiver,
    // then uses it get the RTT estimate based on how long it takes to receive an ack.
    // It also tells the receiver how many packets to expect
    Start(SeqNum),

    // an actual packet with data
    Data(FileData),

    // An Acknowledgement from a receiver that a packet was received
    Ack(Ack),

    // A message from the sender that all packets have been
    // succesfully sent and acked
    Fin,
}
