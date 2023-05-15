#[cfg(test)]
mod discovery_test;
mod mixed_behaviour;
use std::collections::HashSet;
use std::task::Poll;

use futures::{Stream, StreamExt};
use libp2p::core::identity::PublicKey;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{Kademlia, KademliaEvent, QueryResult};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{identify, Multiaddr, PeerId};
use libp2p_identity::PeerId as KadPeerId;
use mixed_behaviour::{MixedBehaviour, MixedEvent};

pub struct Discovery {
    swarm: Swarm<MixedBehaviour>,
    found_peers: HashSet<PeerId>,
}

impl Unpin for Discovery {}

impl Stream for Discovery {
    type Item = PeerId;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            let item = self.swarm.poll_next_unpin(cx);
            match item {
                Poll::Ready(Some(swarm_event)) => {
                    match swarm_event {
                        SwarmEvent::Behaviour(MixedEvent::Kademlia(
                            KademliaEvent::OutboundQueryProgressed {
                                id: _,
                                result: QueryResult::GetClosestPeers(Ok(r)),
                                ..
                            },
                        )) => {
                            self.perform_closest_peer_query();
                            for peer_id in r.peers {
                                if let Some(new_peer_id) = self.handle_found_peer(peer_id) {
                                    // TODO get peer ids from all peers of this request
                                    return Poll::Ready(Some(new_peer_id));
                                }
                            }
                            continue;
                        }
                        SwarmEvent::Behaviour(MixedEvent::Identify(
                            identify::Event::Received { peer_id, info },
                        )) => {
                            for address in info.listen_addrs {
                                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, address);
                            }
                        }
                        // TODO try to get peers from other events
                        _ => {
                            // print!(
                            //     "{:?} got event {:?}\n",
                            //     self.swarm.local_peer_id(),
                            //     swarm_event
                            // );
                            continue;
                        }
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Discovery {
    pub fn new<I>(
        transport: Boxed<(PeerId, StreamMuxerBox)>,
        public_key: PublicKey,
        address: Multiaddr,
        known_peers: I,
    ) -> Self
    where
        I: IntoIterator<Item = (PeerId, Multiaddr)>,
    {
        let peer_id = PeerId::from_public_key(&public_key);
        let mut swarm = Swarm::without_executor(
            transport,
            MixedBehaviour {
                kademlia: Kademlia::new(peer_id, MemoryStore::new(peer_id)),
                identify: identify::Behaviour::new(identify::Config::new(
                    "discovery/0.0.1".to_string(),
                    public_key,
                )),
            },
            peer_id,
        );
        // TODO handle error
        swarm.listen_on(address).unwrap();
        for (known_peer_id, known_peer_address) in known_peers {
            swarm.behaviour_mut().kademlia.add_address(&known_peer_id, known_peer_address.clone());
        }
        // // TODO handle error
        // let qid = swarm.behaviour_mut().bootstrap().unwrap();
        // loop {
        //     let event = swarm.next().await;
        //     println!("{:?} got event {:?}", peer_id, event);
        //     if let Some(SwarmEvent::Behaviour(KademliaEvent::OutboundQueryProgressed {
        //         id,
        //         result: QueryResult::Bootstrap(Ok(_)),
        //         ..
        //     })) = event
        //     {
        //         if id == qid {
        //             println!("{:?} bootstrapped", peer_id);
        //             break;
        //         }
        //     }
        // }
        let mut discovery = Self { swarm, found_peers: HashSet::new() };
        // TODO send multiple queries
        discovery.perform_closest_peer_query();
        discovery
    }

    fn perform_closest_peer_query(&mut self) {
        self.swarm.behaviour_mut().kademlia.get_closest_peers(KadPeerId::random());
    }

    fn handle_found_peer(&mut self, found_peer: PeerId) -> Option<PeerId> {
        if !self.found_peers.contains(&found_peer) {
            self.found_peers.insert(found_peer);
            return Some(found_peer);
        }
        None
    }
}
