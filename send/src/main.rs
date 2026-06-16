use anyhow::Result;
use jap::{Packet, start};
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

    let mut sender = UdpSocket::bind("127.0.0.1:0").await?;
    sender.connect(format!("{host}:{port}")).await?;

    // all packets will start at 0 and will wrap
    let mut seq = 0;
    let rtt = start(&mut sender).await?;

    let mut orch = Orchestrator::new(sender, rtt).await;

    let mut stdin = io::stdin();
    let mut stdin_buffer = Vec::new();

    let bytes_read = stdin.read_to_end(&mut stdin_buffer).await?;
    // means we encountered an EOF
    let packets = Packet::data(&mut seq, stdin_buffer[..bytes_read].to_vec())?;
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
