use std::{fmt::Debug, net::SocketAddr, ops::{Deref, Range}};

use anyhow::{Result, anyhow};
use postcard::{experimental::serialized_size, take_from_bytes, to_slice};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

pub type SequenceNumber = u32;

// data cannot be over the size of blabla
pub const MTU: usize = 1500;

/// container with a sequence number for the data
#[derive(Serialize, Deserialize, Debug)]
pub struct Packet {
    pub seq: SequenceNumber,
    pub value: PacketValue,
}

#[derive(Serialize, Deserialize)]
pub struct FileData(pub Vec<u8>);

impl Debug for FileData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FileData")
            .field(&String::from_utf8_lossy(&self.0))
            .finish()
    }
}

impl Deref for FileData {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// represents data being sent over a socket,
/// as well as ACKs
#[derive(Serialize, Deserialize, Debug)]
pub enum PacketValue {
    // an actual packet with data
    Data(FileData),

    // An Acknowledgement from a receiver that a packet was received
    Ack {
        /// The cumulative ack.
        /// AKA the highest packet ID of ones received contiguously
        cum: SequenceNumber,

        /// The SACK, vector of ranges that represent
        /// the package IDs received by the receiver
        sel: Vec<Range<SequenceNumber>>,
    },

    // Sender gives this to the receiver,
    // then uses it get the RTT estimate based on how long it takes to receive an ack
    Start,

    // A message from the sender that all packets have been
    // succesfully sent and acked
    Fin,
}

impl Packet {
    /// Initializes a set of fragmented data packets
    /// with a random initial sequence number.
    pub fn data(data: Vec<u8>) -> Result<Vec<Self>> {
        Self {
            seq: 0,
            value: PacketValue::Data(FileData(data)),
        }
        .fragment()
    }

    /// The size of the MTU that can be dedicated to data
    pub const MAX_SEGMENT_SIZE: usize = MTU - size_of::<Self>();

    /// Checks the size of the serialized packets in bytes and
    /// fragments it accordingly.
    pub fn fragment(self) -> Result<Vec<Self>> {
        let size = serialized_size(&self)?;
        if size <= MTU {
            return Ok(vec![self]);
        }

        let PacketValue::Data(original) = self.value else {
            return Err(anyhow!("non-data packet exceeds MTU {:?}", self.value));
        };

        let mut seq: u32 = 0;
        Ok(original
            .as_slice()
            .chunks(Self::MAX_SEGMENT_SIZE)
            .map(|segment| {
                let p = Self {
                    seq,
                    value: PacketValue::Data(FileData(segment.into())),
                };
                seq += 1;
                p
            })
            .collect())
    }

    pub async fn write_to(&self, writer: &mut UdpSocket) -> Result<usize> {
        let mut buf = [0u8; MTU];
        let serialized = to_slice(self, &mut buf)?;
        let bytes_read = writer.send(serialized).await?;
        Ok(bytes_read)
    }

    pub async fn write_to_addr(&self, writer: &mut UdpSocket, addr: &SocketAddr) -> Result<usize> {
        let mut buf = [0u8; MTU];
        let serialized = to_slice(self, &mut buf)?;
        let bytes_read = writer.send_to(serialized, addr).await?;
        Ok(bytes_read)
    }

    pub fn from_bytes(mut buffer: &[u8]) -> (Vec<Self>, &[u8]) {
        let mut out = Vec::new();
        while let Ok((packet, remaining)) = take_from_bytes(buffer) {
            out.push(packet);
            buffer = remaining;
        }
        (out, buffer)
    }
}
