// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anemo::{PeerId, Request};
use async_trait::async_trait;
use crypto::{traits::KeyPair, NetworkKeyPair, NetworkPublicKey};
use mysten_common::sync::notify_once::NotifyOnce;
use parking_lot::RwLock;
use tokio::{select, time::sleep};
use types::{
    error::LocalClientError, FetchBatchesRequest, FetchBatchesResponse, 
    WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerSynchronizeMessage, 
    WorkerToPrimaryClient as WorkerToPrimaryGrpcClient, PrimaryToWorkerClient as PrimaryToWorkerGrpcClient, WorkerOurBatchMessage,
};

use crate::{traits::{FetchBatchesRequestToWorker, SynchronizeRequestToWorker}, anemo_ext::WaitingPeer, ReportBatchToPrimary};

/// NetworkClient provides the interface to send requests to other nodes, and call other components
/// directly if they live in the same process. It is used by both primary and worker(s).
///
/// Currently this only supports local direct calls, and it will be extended to support remote
/// network calls.
///
/// TODO: investigate splitting this into Primary and Worker specific clients.
#[derive(Clone)]
pub struct WorkerNetworkClient {
    inner: Arc<RwLock<WorkerNetworkClientInner>>,
    shutdown_notify: Arc<NotifyOnce>,
}

#[allow(dead_code)]
struct WorkerNetworkClientInner {
    // The private-public network key pair of this authority.
    primary_peer_id: PeerId,

    // used by primary to send messages to other authorities' workers.
    primary_to_worker_handler: BTreeMap<PeerId, PrimaryToWorkerGrpcClient<WaitingPeer>>,
    shutdown: bool,
}

impl WorkerNetworkClient {
    const GET_CLIENT_RETRIES: usize = 50;
    const GET_CLIENT_INTERVAL: Duration = Duration::from_millis(100);

    pub fn new(primary_peer_id: PeerId) -> Self {

        Self {
            inner: Arc::new(RwLock::new(WorkerNetworkClientInner {
                primary_peer_id,
                primary_to_worker_handler: BTreeMap::new(),
                shutdown: false,
            })),
            shutdown_notify: Arc::new(NotifyOnce::new()),
        }
    }

    pub fn new_from_keypair(primary_network_keypair: &NetworkKeyPair) -> Self {
        Self::new(PeerId(primary_network_keypair.public().0.into()))
    }

    pub fn set_primary_to_worker_local_handler(
        &self,
        worker_peer_id: PeerId,
        handler: PrimaryToWorkerGrpcClient<WaitingPeer>,
    ) {
        let mut inner = self.inner.write();
        inner.primary_to_worker_handler.insert(worker_peer_id, handler);
    }

    pub fn shutdown(&self) {
        let mut inner = self.inner.write();
        if inner.shutdown {
            return;
        }
        inner.primary_to_worker_handler = BTreeMap::new();
        inner.shutdown = true;
        let _ = self.shutdown_notify.notify();
    }

    async fn get_primary_to_worker_handler(
        &self,
        peer_id: PeerId,
    ) -> Result<PrimaryToWorkerGrpcClient<WaitingPeer>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown);
                }
                if let Some(handler) = inner.primary_to_worker_handler.get(&peer_id) {
                    return Ok(handler.clone());
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::WorkerNotStarted(peer_id))
    }
}

// TODO: extract common logic for cancelling on shutdown.

#[async_trait]
impl SynchronizeRequestToWorker for WorkerNetworkClient {
    async fn synchronize(
        &self,
        worker_name: NetworkPublicKey,
        request: WorkerSynchronizeMessage,
    ) -> Result<(), LocalClientError> {
        let mut c = self
            .get_primary_to_worker_handler(PeerId(worker_name.0.into()))
            .await?;
        select! {
            resp = c.synchronize(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}

#[async_trait]
impl FetchBatchesRequestToWorker for WorkerNetworkClient {
    async fn fetch_batches(
        &self,
        worker_name: NetworkPublicKey,
        request: FetchBatchesRequest,
    ) -> Result<FetchBatchesResponse, LocalClientError> {
        let mut c = self
            .get_primary_to_worker_handler(PeerId(worker_name.0.into()))
            .await?;
        select! {
            resp = c.fetch_batches(Request::new(request)) => {
                Ok(resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?.into_inner())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}

/// A client to send messages from worker to the primary. Currently, used in BatchMaker and WorkerReceiverHandler.
#[derive(Clone)]
pub struct PrimaryNetworkClient {
    inner: Arc<RwLock<PrimaryNetworkClientInner>>,
    shutdown_notify: Arc<NotifyOnce>,
}

impl PrimaryNetworkClient {
    const GET_CLIENT_RETRIES: usize = 50;
    const GET_CLIENT_INTERVAL: Duration = Duration::from_millis(100);

    pub fn new(primary_peer_id: PeerId) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PrimaryNetworkClientInner {
                primary_peer_id,
                worker_to_primary_handler: None,
                shutdown: false,
            })),
            shutdown_notify: Arc::new(NotifyOnce::new()),
        }
    }

    pub fn new_from_keypair(primary_network_keypair: &NetworkKeyPair) -> Self {
        Self::new(PeerId(primary_network_keypair.public().0.into()))
    }

    pub fn set_local_handler(&self, handler: WorkerToPrimaryGrpcClient<WaitingPeer>) {
        let mut inner = self.inner.write();
        inner.worker_to_primary_handler = Some(handler);
    }

    async fn get_handler(&self) -> Result<WorkerToPrimaryGrpcClient<WaitingPeer>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown);
                }
                if let Some(handler) = &inner.worker_to_primary_handler {
                    return Ok(handler.clone());
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::PrimaryNotStarted(
            self.inner.read().primary_peer_id,
        ))
    }

    pub fn shutdown(&self) {
        let mut inner = self.inner.write();
        if inner.shutdown {
            return;
        }
        inner.worker_to_primary_handler = None;
        inner.shutdown = true;
        let _ = self.shutdown_notify.notify();
    }
}

#[async_trait]
impl ReportBatchToPrimary for PrimaryNetworkClient {
    // TODO: Remove once we have upgraded to protocol version 12.
    async fn report_our_batch(
        &self,
        request: WorkerOurBatchMessage,
    ) -> Result<(), LocalClientError> {
        let mut c = self.get_handler().await?;
        select! {
            resp = c.report_our_batch(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }

    async fn report_own_batch(
        &self,
        request: WorkerOwnBatchMessage,
    ) -> Result<(), LocalClientError> {
        let mut handler = self.get_handler().await?;
        select! {
            resp = handler.report_own_batch(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }

    async fn report_others_batch(
        &self,
        request: WorkerOthersBatchMessage,
    ) -> Result<(), LocalClientError> {
        let mut handler = self.get_handler().await?;
        select! {
            resp = handler.report_others_batch(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}


struct PrimaryNetworkClientInner {
    // The private-public network key pair of this authority.
    primary_peer_id: PeerId,

    // used by worker to send messages to its primary
    worker_to_primary_handler: Option<WorkerToPrimaryGrpcClient<WaitingPeer>>,  
    shutdown: bool,
}