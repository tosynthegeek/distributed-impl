use std::error::Error;

use frost::keys::dkg::round2;
use frost_ristretto255::{self as frost};
use futures::SinkExt;
use futures::channel::oneshot;
use libp2p::{Multiaddr, PeerId, request_response::ResponseChannel};

use crate::common::types::{Client, Command, MessageResponse};

impl Client {
    pub async fn start_listening(&mut self, addr: Multiaddr) -> Result<(), Box<dyn Error + Send>> {
        let (sender, _) = oneshot::channel();
        self.sender
            .send(Command::StartListening { addr, sender })
            .await
            .expect("Command receiver not to be dropped.");
        // receiver.await.expect("Sender not to be dropped.")

        Ok(())
    }

    pub async fn dial(
        &mut self,
        peer_id: PeerId,
        peer_addr: Multiaddr,
    ) -> Result<(), Box<dyn Error + Send>> {
        let (sender, _) = oneshot::channel();
        self.sender
            .send(Command::Dial {
                peer_id,
                peer_addr,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        // receiver.await.expect("Sender not to be dropped.")
        Ok(())
    }

    pub async fn send_package(
        &mut self,
        peer: PeerId,
        package: round2::Package,
    ) -> Result<(), Box<dyn Error + Send>> {
        let (sender, _) = oneshot::channel();
        self.sender
            .send(Command::RequestMessage {
                peer,
                sender,
                message: package,
            })
            .await
            .expect("Command receiver not to be dropped.");
        // receiver.await.expect("Sender not be dropped.")

        Ok(())
    }

    pub async fn send_acknowlegement(
        &mut self,
        peer: PeerId,
        channel: ResponseChannel<MessageResponse>,
    ) {
        self.sender
            .send(Command::RespondMessage { peer, channel })
            .await
            .expect("Command receiver not to be dropped.");
    }
}
