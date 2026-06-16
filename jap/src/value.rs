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

// #[derive(Default, Debug, Deref, DerefMut, Serialize, Deserialize)]
// pub struct Sack(BTreeSet<SeqNum>);

// impl Serialize for Sack {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         let rangevec: Vec<(Bound<SeqNum>, Bound<SeqNum>)> =
//             self.0.iter().map(|ar| (ar.start, ar.end)).collect();
//         rangevec.serialize(serializer)
//     }
// }
//
// impl<'de> Deserialize<'de> for Sack {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         let rangevec = Vec::<(Bound<SeqNum>, Bound<SeqNum>)>::deserialize(deserializer)?;
//         let mut set = RangeSet::new();
//         for (start, end) in rangevec {
//             set.insert(AnyRange::new(start, end));
//         }
//         Ok(Self(set))
//     }
// }

// impl Sack {
//     fn add_seq(&mut self, seq: SeqNum) -> bool {
//         self.0.insert(seq)
//     }
// }

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
    fn add_seq(&mut self, new_seq: SeqNum) -> bool {
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
        let mut out = Self {
            cum: 0,
            sel: Default::default(),
            adv_win,
        };

        for p in packets {
            out.add_seq(p.seq);
        }

        out
    }
}

/// represents data being sent over a socket,
/// as well as ACKs
#[derive(Serialize, Deserialize, Debug)]
pub enum PacketValue {
    // Sender gives this to the receiver,
    // then uses it get the RTT estimate based on how long it takes to receive an ack
    Start,

    // an actual packet with data
    Data(FileData),

    // An Acknowledgement from a receiver that a packet was received
    Ack(Ack),

    // A message from the sender that all packets have been
    // succesfully sent and acked
    Fin,
}
