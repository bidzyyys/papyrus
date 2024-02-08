use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use deadqueue::unlimited::Queue;
use futures::channel::mpsc::{unbounded, Sender, UnboundedSender};
use futures::future::poll_fn;
use futures::stream::{FuturesUnordered, Stream};
use futures::{pin_mut, Future, FutureExt, StreamExt};
use libp2p::PeerId;
use starknet_api::block::{BlockHeader, BlockNumber};
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use super::swarm_trait::{Event, SwarmTrait};
use super::GenericNetworkManager;
use crate::block_headers::behaviour::{PeerNotConnected, SessionIdNotFoundError};
use crate::block_headers::Event as BehaviourEvent;
use crate::db_executor::{poll_query_execution_set, DBExecutor, DBExecutorError, Data, QueryId};
use crate::streamed_data::{InboundSessionId, OutboundSessionId};
use crate::{BlockHashOrNumber, BlockQuery, Direction};

#[derive(Default)]
struct MockSwarm {
    pub pending_events: Queue<Event>,
    inbound_session_id_to_data_sender: HashMap<InboundSessionId, UnboundedSender<Data>>,
}

impl Stream for MockSwarm {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let fut = self.pending_events.pop().map(|x| Some(x));
        pin_mut!(fut);
        fut.poll_unpin(cx)
    }
}

impl MockSwarm {
    pub fn get_data_sent_to_inbound_session(
        &mut self,
        inbound_session_id: InboundSessionId,
    ) -> impl Future<Output = Vec<Data>> {
        let (data_sender, data_receiver) = unbounded();
        if self.inbound_session_id_to_data_sender.insert(inbound_session_id, data_sender).is_some()
        {
            panic!("Called get_data_sent_to_inbound_session on {inbound_session_id:?} twice");
        }
        data_receiver.collect()
    }
}

impl SwarmTrait for MockSwarm {
    fn send_data(
        &mut self,
        data: Data,
        inbound_session_id: InboundSessionId,
    ) -> Result<(), SessionIdNotFoundError> {
        let data_sender = self
            .inbound_session_id_to_data_sender
            .get(&inbound_session_id)
            .expect("Called send_data without calling get_data_sent_to_inbound_session first");
        let is_fin = matches!(data, Data::Fin);
        println!("Got data {:?}", data);
        data_sender.unbounded_send(data).unwrap();
        if is_fin {
            data_sender.close_channel();
        }
        Ok(())
    }

    fn send_query(
        &mut self,
        _query: BlockQuery,
        _peer_id: PeerId,
    ) -> Result<OutboundSessionId, PeerNotConnected> {
        unimplemented!()
    }
}

#[derive(Default)]
struct MockDBExecutor {
    next_query_id: usize,
    pub query_to_headers: HashMap<BlockQuery, Vec<Result<BlockHeader, DBExecutorError>>>,
    query_execution_set: FuturesUnordered<JoinHandle<Result<QueryId, DBExecutorError>>>,
}

impl Stream for MockDBExecutor {
    type Item = Result<QueryId, DBExecutorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        poll_query_execution_set(&mut Pin::into_inner(self).query_execution_set, cx)
    }
}

impl DBExecutor for MockDBExecutor {
    // TODO(shahak): Consider fixing code duplication with BlockHeaderDBExecutor.
    fn register_query(&mut self, query: BlockQuery, mut sender: Sender<Data>) -> QueryId {
        let query_id = QueryId(self.next_query_id);
        self.next_query_id += 1;
        let headers = self.query_to_headers.remove(&query).unwrap();
        self.query_execution_set.push(tokio::task::spawn(async move {
            {
                for header_result in headers {
                    let header = header_result?;
                    // Using poll_fn because Sender::poll_ready is not a future
                    if let Ok(()) = poll_fn(|cx| sender.poll_ready(cx)).await {
                        sender
                            .start_send(Data::BlockHeaderAndSignature { header, signature: None })
                            .unwrap();
                    }
                }
                if let Ok(()) = poll_fn(|cx| sender.poll_ready(cx)).await {
                    sender.start_send(Data::Fin).unwrap();
                }
                Ok(query_id)
            }
        }));
        query_id
    }
}

const HEADER_BUFFER_SIZE: usize = 100;

#[tokio::test]
async fn process_incoming_query() {
    let query = BlockQuery {
        start_block: BlockHashOrNumber::Number(BlockNumber(0)),
        direction: Direction::Forward,
        limit: 5,
        step: 1,
    };
    let headers = (0..5)
        .map(|i| BlockHeader { block_number: BlockNumber(i), ..Default::default() })
        .collect::<Vec<_>>();

    let mut mock_db_executor = MockDBExecutor::default();
    mock_db_executor
        .query_to_headers
        .insert(query, headers.iter().map(|header| Ok(header.clone())).collect());

    let mut mock_swarm = MockSwarm::default();
    let inbound_session_id = InboundSessionId { value: 0 };
    mock_swarm
        .pending_events
        .push(Event::Behaviour(BehaviourEvent::NewInboundQuery { query, inbound_session_id }));
    let get_data_fut = mock_swarm.get_data_sent_to_inbound_session(inbound_session_id);

    let network_manager =
        GenericNetworkManager::inner_new(mock_swarm, mock_db_executor, HEADER_BUFFER_SIZE);

    select! {
        inbound_session_data = get_data_fut => {
            let mut expected_data = headers
                .into_iter()
                .map(|header| Data::BlockHeaderAndSignature { header, signature: None })
                .collect::<Vec<_>>();
            expected_data.push(Data::Fin);
            assert_eq!(inbound_session_data, expected_data);
        }
        _ = network_manager.run() => {
            panic!("GenericNetworkManager::run finished before the session finished");
        }
        _ = sleep(Duration::from_secs(5)) => {
            panic!("Test timed out");
        }
    }
}

#[tokio::test]
async fn handle_db_executor_error() {
    let query = BlockQuery {
        start_block: BlockHashOrNumber::Number(BlockNumber(0)),
        direction: Direction::Forward,
        limit: 5,
        step: 1,
    };
    let headers = (0..3)
        .map(|i| BlockHeader { block_number: BlockNumber(i), ..Default::default() })
        .collect::<Vec<_>>();

    let mut headers_results = headers.iter().map(|header| Ok(header.clone())).collect::<Vec<_>>();
    // TODO(shahak): Think of a way to not rely on the query id being 0.
    headers_results.push(Err(DBExecutorError::BlockNotFound {
        block_hash_or_number: BlockHashOrNumber::Number(BlockNumber(3)),
        query_id: QueryId(0),
    }));

    let mut mock_db_executor = MockDBExecutor::default();
    mock_db_executor.query_to_headers.insert(query, headers_results);

    let mut mock_swarm = MockSwarm::default();
    let inbound_session_id = InboundSessionId { value: 0 };
    mock_swarm
        .pending_events
        .push(Event::Behaviour(BehaviourEvent::NewInboundQuery { query, inbound_session_id }));
    let get_data_fut = mock_swarm.get_data_sent_to_inbound_session(inbound_session_id);

    let network_manager =
        GenericNetworkManager::inner_new(mock_swarm, mock_db_executor, HEADER_BUFFER_SIZE);

    select! {
        inbound_session_data = get_data_fut => {
            let mut expected_data = headers
                .into_iter()
                .map(|header| Data::BlockHeaderAndSignature { header, signature: None })
                .collect::<Vec<_>>();
            expected_data.push(Data::Fin);
            assert_eq!(inbound_session_data, expected_data);
        }
        _ = network_manager.run() => {
            panic!("GenericNetworkManager::run finished before the session finished");
        }
        _ = sleep(Duration::from_secs(5)) => {
            panic!("Test timed out");
        }
    }
}
