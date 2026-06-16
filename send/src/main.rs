use anyhow::Result;
use jap::{Packet, SeqNum, start};
use tokio::io::{self, AsyncReadExt};
use tokio::net::UdpSocket;

use crate::orchestrator::Orchestrator;

mod orchestrator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args();
    let _program = args.next().unwrap();
    let host = args.next().unwrap();
    let port = args.next().unwrap();

    let mut stdin = io::stdin();
    let mut stdin_buffer = Vec::new();
    let mut sender = UdpSocket::bind("127.0.0.1:0").await?;
    sender.connect(format!("{host}:{port}")).await?;
    let bytes_read = stdin.read_to_end(&mut stdin_buffer).await?;
    // all packets will start at 0 and will wrap
    let mut seq = 0;
    // means we encountered an EOF
    let packets = Packet::data(&mut seq, stdin_buffer[..bytes_read].to_vec(), 1)?;

    let rtt = start(&mut sender, packets.len() as SeqNum).await?;

    let mut orch = Orchestrator::new(sender, rtt).await;


    orch.change_packets(packets);

    // transmitting file data
    while !orch.success() {
        orch.transmit().await?;
    }

    orch.change_packets(vec![Packet::fin(&mut seq)]);
    // transmitting finished packet
    while !orch.success() {
        orch.transmit().await?;
    }

    eprintln!("S: concluded");
    Ok(())
}
