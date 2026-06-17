use std::time::Duration;

use anyhow::Result;
use tokio::net::UdpSocket;

use crate::{MTU, Packet, PacketList, SeqNum};

/// Adjusts the round trip time estimate with the provided formula
pub fn adjust_rtt(old_rtt: Duration, new_sample: Duration) -> Duration {
    const RTT_ALPHA: f32 = 0.85;
    Duration::from_secs_f32(
        RTT_ALPHA * old_rtt.as_secs_f32() + (1.0 - RTT_ALPHA) * new_sample.as_secs_f32(),
    )
}

pub async fn check_for_packets(socket: &mut UdpSocket, rwnd: SeqNum) -> (PacketList, Vec<u8>) {
    let mut buf = Vec::with_capacity(MTU * rwnd as usize);
    buf.reserve(MTU);

    // read all the bytes possible
    while let Ok(_) = if socket.peer_addr().is_ok() {
        socket.try_recv_buf(&mut buf)
    } else {
        let res = socket.try_recv_buf_from(&mut buf);
        match res {
            Ok((len, addr)) => {
                socket.connect(addr).await.expect("Failed to connect");
                Ok(len)
            }
            Err(e) => Err(e),
        }
    } {}

    let (list, remaining) = Packet::from_bytes(&buf);
    (list, remaining.to_vec())
}

/// Writes packets to the socket.
/// NOTE: Assumes the packets do not exceed MTU
pub async fn send_packets<'a, T>(socket: &mut UdpSocket, packets: T) -> Result<()>
where
    T: Iterator<Item = &'a Packet>,
{
    for p in packets {
        p.write_to(socket).await?;
    }

    Ok(())
}
