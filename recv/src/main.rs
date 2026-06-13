use jap::{FileData, MTU, Packet, PacketValue};
use postcard::from_bytes;
use std::{collections::BTreeMap, ops::Range};
use tokio::net::UdpSocket;

const RWND: usize = MTU * 3;

/// Returns (cumulative, selective_blocks).
/// cumulative = next expected id; everything below it is contiguous from 0.
/// each Range start..end is half-open: ids start..end were received.
fn build_sack(file_data: &BTreeMap<u32, FileData>) -> (u32, Vec<Range<u32>>) {
    let mut runs: Vec<Range<u32>> = Vec::new();
    for &id in file_data.keys() {
        match runs.last_mut() {
            Some(run) if run.end == id => run.end = id + 1,
            _ => runs.push(id..id + 1),
        }
    }

    let cum = match runs.first() {
        Some(r) if r.start == 0 => r.end,
        _ => 0,
    };

    let sel = runs.into_iter().filter(|r| r.start > cum).collect();
    (cum, sel)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut receiver = UdpSocket::bind(format!("127.0.0.1:0")).await?;
    let port = receiver.local_addr()?.port();
    eprintln!("Bound to port {port}");

    let mut buf = [0u8; RWND];
    // wait for the initial start message from the sender
    let (len, addr) = receiver.recv_from(&mut buf).await?;
    let p: Packet = from_bytes(&buf[..len])?;
    assert!(matches!(p.value, PacketValue::Start), "First message should be a start message");
    let _startack_bytes_written = Packet {
        seq: 0,
        value: PacketValue::Ack { cum: p.seq, sel: vec![] }
    }.write_to_addr(&mut receiver, &addr).await?;

    let mut file_data = BTreeMap::new();
    'r: loop {
        let (len, addr) = receiver.recv_from(&mut buf).await?;
        let (received_packets, remaining) = Packet::from_bytes(&buf[..len]);
        if !remaining.is_empty() {
            eprintln!("Unfinished serialized packet: {:?}", remaining);
        }
        eprintln!("Deserialized packets: {:?}", received_packets);

        for packet in received_packets {
            match packet.value {
                PacketValue::Data(data) => {
                    file_data.entry(packet.seq).or_insert(data);
                }
                PacketValue::Fin => break 'r,
                _ => unreachable!(),
            }
        }

        let (cum, sel) = build_sack(&file_data);
        let sack = Packet {
            seq: 0,
            value: PacketValue::Ack { cum, sel },
        };
        eprintln!("Sending ack: {:?}", sack.value);
        sack.write_to_addr(&mut receiver, &addr).await?;
    }

    let total_data: Vec<u8> = file_data.into_values().map(|f| f.0).flatten().collect();
    println!("{}", String::from_utf8_lossy(&total_data));
    loop {}
}
