use std::{fmt::Debug, net::SocketAddr};

use anyhow::{Result, anyhow};
use postcard::{experimental::serialized_size, take_from_bytes, to_slice};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

mod window;
pub use window::{adjust_rtt, send_packets, start, try_share_packets, check_for_packets};
mod value;
pub use value::{Ack, FileData, PacketValue};

pub type SeqNum = u32;

// data cannot be over the size of blabla
pub const MTU: usize = 1500;

/// container with a sequence number for the data
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Packet {
    pub seq: SeqNum,
    pub value: value::PacketValue,
}

pub(crate) type PacketList = Vec<Packet>;

impl Packet {
    /// Initializes a set of fragmented data packets
    /// with a random initial sequence number.
    pub fn data(seq: &mut SeqNum, data: Vec<u8>, window: SeqNum) -> Result<PacketList> {
        let (out, new_seq_start) = Self {
            seq: *seq,
            value: value::PacketValue::Data {
                data: value::FileData(data),
                window: 1
            },
        }
        .fragment(window)?;

        *seq = new_seq_start;

        Ok(out)
    }

    pub fn fin(seq: &mut SeqNum) -> Self {
        *seq += 1;

        Self {
            seq: *seq,
            value: PacketValue::Fin,
        }
    }

    /// The size of the MTU that can be dedicated to data
    pub const MAX_SEGMENT_SIZE: usize = MTU - size_of::<Self>();

    /// Checks the size of the serialized packets in bytes and
    /// fragments it accordingly. Returns the list of fragments
    /// alongside the last SequenceNumber
    fn fragment(self, window: SeqNum) -> Result<(Vec<Self>, SeqNum)> {
        let size = serialized_size(&self)?;
        if size <= MTU {
            let seq = self.seq;
            return Ok((vec![self], seq));
        }

        let value::PacketValue::Data { data: original, .. } = self.value else {
            return Err(anyhow!("non-data packet exceeds MTU {:?}", self.value));
        };

        let mut seq = self.seq;
        let mut next_seq = seq.wrapping_add(1);
        let fragments = original
            .as_slice()
            .chunks(Self::MAX_SEGMENT_SIZE)
            .map(|segment| {
                let p = Self {
                    seq: next_seq,
                    value: value::PacketValue::Data {
                        data: value::FileData(segment.into()),
                        // NOTE: DO NOT SEND IT LIKE THIS, MUTATE THIS VAR
                        window,
                    },
                };
                seq = next_seq;
                next_seq = next_seq.wrapping_add(1);
                p
            })
            .collect();
        Ok((fragments, seq))
    }

    /// Sends data on a one to one socket
    pub async fn write_to(&self, writer: &mut UdpSocket) -> Result<usize> {
        let mut buf = [0u8; MTU];
        let serialized = to_slice(self, &mut buf)?;
        let bytes_read = writer.send(serialized).await?;
        Ok(bytes_read)
    }

    /// Sends data on a one-to-many socket
    pub async fn write_to_addr(&self, writer: &mut UdpSocket, addr: &SocketAddr) -> Result<usize> {
        let mut buf = [0u8; MTU];
        let serialized = to_slice(self, &mut buf)?;
        let bytes_read = writer.send_to(serialized, addr).await?;
        Ok(bytes_read)
    }

    /// Converts a window of net bytes to a list of packets
    pub fn from_bytes(mut buffer: &[u8]) -> (Vec<Self>, &[u8]) {
        let mut out = Vec::<Packet>::new();
        while let Ok((packet, remaining)) = take_from_bytes(buffer) {
            out.push(packet);
            buffer = remaining;
        }
        out.sort_by_key(|p| p.seq);
        (out, buffer)
    }
}
