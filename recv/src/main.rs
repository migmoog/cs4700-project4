use anyhow::Result;
use jap::{Ack, FileData, Packet, PacketValue, SeqNum, check_for_packets, send_packets};
use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use tokio::{net::UdpSocket, task::JoinHandle};

fn print_file_data(fd: &BTreeMap<u32, FileData>) {
    let total_data: Vec<u8> = fd.values().map(|f| f.0.clone()).flatten().collect();
    print!("{}", String::from_utf8_lossy(&total_data));
}

struct Acker {
    /// ack gets dynamically constructed
    ack: Arc<Mutex<Option<Ack>>>,
    _handle: JoinHandle<()>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut receiver = UdpSocket::bind(format!("127.0.0.1:0")).await?;
    let port = receiver.local_addr()?.port();
    eprintln!("Bound to port {port}");

    let mut file_data = BTreeMap::<SeqNum, FileData>::new();
    loop {
        let (received_packets, remaining) = check_for_packets(&mut receiver, adv_win).await;
        if !received_packets.is_empty() {
            // bad condition so it needs to be logged
            if !remaining.is_empty() {
                eprintln!(
                    "R: got unserializable data: {:?}",
                    String::from_utf8_lossy(&remaining)
                );
            }

            for packet in received_packets {

            }
        }
    }
}
