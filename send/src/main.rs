use std::collections::BTreeSet;
use std::ops::Range;
use std::time::Duration;

use anyhow::{Result, anyhow};
use jap::{Packet, PacketValue, SequenceNumber};
use postcard::from_bytes;
use tokio::io::{self, AsyncReadExt};
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout};

/// Struct that is given a designated set of packets to transmit
/// given timeout for waitin for responses from sender
struct Orchestrator {
    transmitted_packets: Vec<Packet>,
    received_acks: BTreeSet<SequenceNumber>,
    timeout: Duration,
}

impl Orchestrator {
    fn new(transmitted_packets: Vec<Packet>, timeout: Duration) -> Self {
        Self {
            transmitted_packets,
            received_acks: BTreeSet::new(),
            timeout,
        }
    }

    fn success(&self) -> bool {
        self.transmitted_packets
            .iter()
            .all(|p| self.received_acks.contains(&p.seq))
    }

    fn adjust(&mut self, cum: SequenceNumber, sel: &Vec<Range<SequenceNumber>>) {
        for seq in 0..=cum {
            self.received_acks.insert(seq);
        }

        for sack in sel {
            for seq in sack.start..sack.end {
                self.received_acks.insert(seq);
            }
        }
    }

    /// Will transmit packets
    async fn transmit(&mut self, socket: &mut UdpSocket) -> Result<bool> {
        for packet in self.transmitted_packets.iter() {
            let _bytes_written = packet.write_to(socket).await?;
        }

        let mut receiver_buf = [0u8; 1024];
        let bytes_read = match timeout(self.timeout, socket.recv(&mut receiver_buf)).await {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => return Err(anyhow!("Socket failure {e}")),
            Err(_elapsed) => return Ok(false),
        };
        let (received_packets, remaining) = Packet::from_bytes(&receiver_buf[..bytes_read]);

        if !remaining.is_empty() {
            eprintln!("Sender has unserializeable data {:?}", remaining);
        }
        eprintln!("Sender recv'd packets: {:?}", received_packets);

        for packet in received_packets {
            if let PacketValue::Ack { cum, sel } = packet.value {
                self.adjust(cum, &sel);
            } else {
                eprintln!("S: got non-ack packet: {:?}", packet);
            }
        }

        Ok(true)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args();
    let _program = args.next().unwrap();
    let host = args.next().unwrap();
    let port = args.next().unwrap();

    let mut sender = UdpSocket::bind("127.0.0.1:0").await?;
    sender.connect(format!("{host}:{port}")).await?;

    let rtt = loop {
        let mut rtt_ack_buf = [0u8; 1024];
        let before_start = Instant::now();
        let start = Packet {
            seq: 0,
            value: PacketValue::Start,
        };
        let _ = start.write_to(&mut sender).await?;
        let bytes_read = sender.recv(&mut rtt_ack_buf).await?;

        // check if our packet was got or corrupted
        if let Ok(Packet {
            seq: _,
            value: PacketValue::Ack { cum: _, sel: _ },
        }) = from_bytes(&rtt_ack_buf[..bytes_read])
        {
            break Instant::now() - before_start;
        }
    };
    eprintln!("S: started with an RTT of {:?}", rtt);

    let mut stdin = io::stdin();
    let mut stdin_buffer = [0u8; 2048];

    loop {
        let bytes_read = stdin.read(&mut stdin_buffer).await?;
        // means we encountered an EOF
        let reached_eof = bytes_read == 0;
        let transmitted_packets = if reached_eof {
            let fin = Packet {
                seq: 0,
                value: PacketValue::Fin,
            };
            vec![fin]
        } else {
            Packet::data(stdin_buffer[..bytes_read].to_vec())?
        };

        let mut orch = Orchestrator::new(transmitted_packets, rtt);
        while !orch.success() {
            let timed_out  = orch.transmit(&mut sender).await?;

            if timed_out {
                eprintln!("S: sender timed out on waiting for acks for packet ids");
            }
        }

        if reached_eof {
            break;
        }
    }

    eprintln!("S: concluded");
    Ok(())
}
