#[cfg(test)]
mod discovery_test;
mod mixed_behaviour;
use std::collections::HashSet;
use std::task::Poll;
use std::time::Instant;

use futures::{Stream, StreamExt};
use libp2p::core::identity::PublicKey;
use libp2p::core::multiaddr::Protocol;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{Kademlia, KademliaEvent, QueryResult};
use libp2p::swarm::{Swarm, SwarmBuilder, SwarmEvent};
use libp2p::{identify, Multiaddr, PeerId};
use libp2p_identity::PeerId as KadPeerId;
use mixed_behaviour::{MixedBehaviour, MixedEvent};
use primitive_types::U256;
use tracing::{debug, info};

#[derive(Clone)]
pub struct DiscoveryConfig {
    pub n_active_queries: usize,
    pub found_peers_limit: Option<usize>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self { n_active_queries: 1, found_peers_limit: None }
    }
}

pub struct Discovery {
    discovery_config: DiscoveryConfig,
    swarm: Swarm<MixedBehaviour>,
    found_peers: HashSet<PeerId>,
    address: Multiaddr,
    global_peers_names: Vec<(String, PeerId, Multiaddr)>,
    time_last_query_sent: Instant,
}

impl Unpin for Discovery {}

impl Stream for Discovery {
    type Item = (PeerId, Multiaddr);
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.time_last_query_sent.elapsed().as_secs() > 5 {
            if self
                .global_peers_names
                .iter()
                .filter(|(name, peer_id, _)| peer_id == self.swarm.local_peer_id() && name == "5")
                .next()
                .is_some()
            {
                self.log_message(format!("!!!! {:?} performed query", self.swarm.local_peer_id()));
                self.perform_closest_peer_query();
                self.time_last_query_sent = Instant::now();
            }
        }
        if let Some(found_peers_limit) = self.discovery_config.found_peers_limit {
            if self.found_peers.len() >= found_peers_limit {
                return Poll::Ready(None);
            }
        }
        loop {
            let item = self.swarm.poll_next_unpin(cx);
            match item {
                Poll::Ready(Some(swarm_event)) => match swarm_event {
                    SwarmEvent::Behaviour(MixedEvent::Kademlia(kademlia_event)) => {
                        match kademlia_event {
                            KademliaEvent::OutboundQueryProgressed {
                                id: _,
                                result: QueryResult::GetClosestPeers(Ok(r)),
                                ..
                            } => {
                                self.log_message(format!(
                                    "{:?} got query result {:?}",
                                    self.peer_id(),
                                    r.peers
                                ));
                                for peer in r.peers {
                                    if !self.found_peers.contains(&peer) {
                                        self.log_message(format!(
                                            "ERROR: {:?} found peer {:?} without routing to it",
                                            self.peer_id(),
                                            peer,
                                        ));
                                    }
                                }
                                // self.perform_closest_peer_query();
                            }
                            KademliaEvent::RoutingUpdated { peer, addresses, .. } => {
                                self.log_message(format!(
                                    "{:?} found peer {:?} through RoutingUpdated",
                                    self.peer_id(),
                                    peer,
                                ));
                                if let Some((peer_id, address)) =
                                    self.handle_found_peer(peer, addresses.first().clone())
                                {
                                    return Poll::Ready(Some((peer_id, address)));
                                }
                            }
                            KademliaEvent::RoutablePeer { peer, address } => {
                                self.log_message(format!(
                                    "{:?} found peer {:?} through RoutablePeer",
                                    self.peer_id(),
                                    peer,
                                ));
                                if let Some((peer_id, address)) =
                                    self.handle_found_peer(peer, address)
                                {
                                    return Poll::Ready(Some((peer_id, address)));
                                }
                            }
                            KademliaEvent::PendingRoutablePeer { peer, address } => {
                                self.log_message(format!(
                                    "{:?} found peer {:?} through PendingRoutablePeer",
                                    self.peer_id(),
                                    peer,
                                ));
                                if let Some((peer_id, address)) =
                                    self.handle_found_peer(peer, address)
                                {
                                    return Poll::Ready(Some((peer_id, address)));
                                }
                            }
                            _ => {
                                self.log_message(format!(
                                    "{:?} got event {:?}",
                                    self.swarm.local_peer_id(),
                                    kademlia_event,
                                ));
                            }
                        }
                    }
                    SwarmEvent::Behaviour(MixedEvent::Identify(identify::Event::Received {
                        peer_id,
                        info,
                    })) => {
                        for address in info.listen_addrs {
                            self.log_message(format!(
                                "{:?} found through identify {:?} with {:?}",
                                self.peer_id(),
                                peer_id,
                                address
                            ));
                            self.swarm.behaviour_mut().kademlia.add_address(&peer_id, address);
                        }
                    }
                    SwarmEvent::IncomingConnection { send_back_addr, .. } => {
                        self.log_message(format!(
                            "{:?} has incoming connection from {:?}",
                            self.peer_id(),
                            send_back_addr
                        ));
                    }
                    _ => {
                        debug!("{:?} got event {:?}", self.swarm.local_peer_id(), swarm_event);
                    }
                },
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Discovery {
    pub fn new<I>(
        discovery_config: DiscoveryConfig,
        transport: Boxed<(PeerId, StreamMuxerBox)>,
        public_key: PublicKey,
        address: Multiaddr,
        known_peers: I,
        global_peers_names: Vec<(String, PeerId, Multiaddr)>,
    ) -> Self
    where
        I: IntoIterator<Item = (PeerId, Multiaddr)>,
    {
        let peer_id = PeerId::from_public_key(&public_key);
        // TODO allow customization of swarm building (executor and builder functions)
        let mut swarm = SwarmBuilder::without_executor(
            transport,
            MixedBehaviour {
                kademlia: Kademlia::new(peer_id, MemoryStore::new(peer_id)),
                identify: identify::Behaviour::new(identify::Config::new(
                    "discovery/0.0.1".to_string(),
                    public_key,
                )),
            },
            peer_id,
        )
        .build();
        // TODO handle error
        swarm.listen_on(address.clone()).unwrap();
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
        let mut discovery = Self {
            discovery_config,
            swarm,
            found_peers: HashSet::new(),
            address,
            global_peers_names,
            time_last_query_sent: Instant::now(),
        };
        // for _ in 0..discovery.discovery_config.n_active_queries {
        //     discovery.perform_closest_peer_query();
        // }
        discovery
    }

    pub fn peer_id(&self) -> &PeerId {
        self.swarm.local_peer_id()
    }

    pub fn address(&self) -> &Multiaddr {
        &self.address
    }

    fn perform_closest_peer_query(&mut self) {
        self.log_message(format!("{:?} starts query", self.swarm.local_peer_id(),));
        self.swarm.behaviour_mut().kademlia.get_closest_peers(KadPeerId::random());
    }

    fn handle_found_peer(
        &mut self,
        found_peer: PeerId,
        address: Multiaddr,
    ) -> Option<(PeerId, Multiaddr)> {
        let mut address = address;
        if !self.found_peers.contains(&found_peer) {
            self.found_peers.insert(found_peer);
            if let Some(Protocol::P2p(_)) = address.iter().last() {
                address.pop();
            }
            return Some((found_peer, address));
        }
        None
    }

    fn log_message(&self, msg: String) {
        // if self
        //     .global_peers_names
        //     .iter()
        //     .filter(|(name, peer_id, _)| peer_id == self.swarm.local_peer_id() && name != "5")
        //     .next()
        //     .is_some()
        // {
        //     return;
        // }
        let mut msg = msg;
        for (name, peer_id, address) in &self.global_peers_names {
            msg = msg.replace(&format!("{:?}", peer_id), &format!("id{name}"));
            msg = msg.replace(&format!("{:?}", address), &format!("address{name}"));
            msg =
                msg.replace(&format!("{:?}", address).trim_matches('"'), &format!("address{name}"));
            let mut parts: Vec<String> = msg.split("Distance(").map(|s| s.to_string()).collect();
            for mut part in parts.iter_mut().skip(1) {
                let i = part.find(')').unwrap();
                let (s1, s2) = part.split_at(i);
                let n = U256::from_dec_str(&s1).unwrap();
                let ilog2: i32 = (256 - n.leading_zeros() - 1).try_into().unwrap();
                let new_part = format!("{}{}", ilog2, s2);
                *part = new_part;
            }
            msg = parts.join("");
        }
        info!(msg);
    }
}
