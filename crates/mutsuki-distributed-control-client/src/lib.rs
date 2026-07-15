//! Minimal authenticated client for product-side capability gating.

#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::must_use_candidate)]

use hmac::{Hmac, Mac};
use mutsuki_distributed_contracts::{
    ControllerCommand, ControllerReply, ControllerReplyBody, ControllerRequest, DistributedError,
    DistributedErrorKind, NodeId, SidecarCapabilityProof, decode_control, encode_control,
};
use mutsuki_link::{
    Connection, EndpointId, TransportBudget, TransportErrorKind,
    local::{LocalAddress, LocalConnection},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

type HmacSha256 = Hmac<Sha256>;
const AUTH_CONTEXT: &[u8] = b"mutsuki.distributed.local-session.v1";
static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthHello {
    node_id: NodeId,
    nonce: [u8; 32],
    proof: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthWelcome {
    node_id: NodeId,
    nonce: [u8; 32],
    proof: [u8; 32],
}

pub struct DistributedControlClient {
    local_node: NodeId,
    address: String,
    secret: Arc<[u8]>,
    timeout: Duration,
    connection: Mutex<Option<LocalConnection>>,
    next_request_id: AtomicU64,
}

impl DistributedControlClient {
    pub fn new(
        local_node: NodeId,
        address: impl Into<String>,
        secret: Arc<[u8]>,
        timeout: Duration,
    ) -> Result<Self, DistributedError> {
        if local_node.0.trim().is_empty() || secret.len() < 32 || timeout.is_zero() {
            return Err(DistributedError::new(
                DistributedErrorKind::InvalidConfig,
                "control client identity, secret, and timeout must be valid",
            ));
        }
        let address = address.into();
        if address.trim().is_empty() {
            return Err(DistributedError::new(
                DistributedErrorKind::InvalidConfig,
                "control endpoint must not be empty",
            ));
        }
        Ok(Self {
            local_node,
            address,
            secret,
            timeout,
            connection: Mutex::new(None),
            next_request_id: AtomicU64::new(1),
        })
    }

    pub async fn capabilities(&self) -> Result<SidecarCapabilityProof, DistributedError> {
        match self.request(ControllerCommand::Capabilities).await? {
            ControllerReplyBody::Capabilities(proof) => Ok(proof),
            _ => Err(protocol_error(
                "controller capability reply has the wrong type",
            )),
        }
    }

    pub async fn health(&self) -> Result<String, DistributedError> {
        match self.request(ControllerCommand::Health).await? {
            ControllerReplyBody::Health(health) => Ok(health),
            _ => Err(protocol_error("controller health reply has the wrong type")),
        }
    }

    async fn request(
        &self,
        command: ControllerCommand,
    ) -> Result<ControllerReplyBody, DistributedError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let payload = encode_control(&ControllerRequest {
            request_id,
            command,
        })?;
        let mut state = self.connection.lock().await;
        if state.is_none() {
            *state = Some(self.connect().await?);
        }
        let result = async {
            let connection = state.as_mut().expect("control connection initialized");
            send_message(connection, &payload, self.timeout).await?;
            let reply: ControllerReply =
                decode_control(&receive_message(connection, self.timeout).await?)?;
            if reply.request_id != request_id {
                return Err(protocol_error("controller reply request id does not match"));
            }
            reply.result.map_err(|failure| {
                DistributedError::new(failure.kind, "controller rejected control request")
            })
        }
        .await;
        if result.is_err() {
            if let Some(connection) = state.as_mut() {
                connection.abort();
            }
            *state = None;
        }
        result
    }

    async fn connect(&self) -> Result<LocalConnection, DistributedError> {
        let context = mutsuki_link::ConnectContext {
            deadline: Some(Instant::now() + self.timeout),
            ..mutsuki_link::ConnectContext::default()
        };
        let mut connection = mutsuki_link::local::connect(
            &LocalAddress(self.address.clone()),
            endpoint_id(&self.local_node),
            endpoint_id(&NodeId("controller-management".into())),
            transport_budget(),
            &context,
        )
        .await
        .map_err(|error| map_transport(&error))?;
        authenticate_client(
            &mut connection,
            &self.local_node,
            &NodeId("controller-management".into()),
            &self.secret,
            self.timeout,
        )
        .await?;
        Ok(connection)
    }
}

async fn authenticate_client(
    connection: &mut LocalConnection,
    local: &NodeId,
    remote: &NodeId,
    secret: &[u8],
    timeout: Duration,
) -> Result<(), DistributedError> {
    let client_nonce = nonce(local);
    let hello = AuthHello {
        node_id: local.clone(),
        nonce: client_nonce,
        proof: auth_proof(secret, b"hello", local, remote, &client_nonce, &[0; 32])?,
    };
    send_message(
        connection,
        &serde_json::to_vec(&hello).map_err(|_| auth_error())?,
        timeout,
    )
    .await?;
    let welcome: AuthWelcome = serde_json::from_slice(&receive_message(connection, timeout).await?)
        .map_err(|_| auth_error())?;
    if &welcome.node_id != remote
        || welcome.proof
            != auth_proof(
                secret,
                b"welcome",
                remote,
                local,
                &welcome.nonce,
                &client_nonce,
            )?
    {
        return Err(auth_error());
    }
    Ok(())
}

async fn send_message(
    connection: &mut LocalConnection,
    bytes: &[u8],
    timeout: Duration,
) -> Result<(), DistributedError> {
    let deadline = Instant::now() + timeout;
    loop {
        match connection.try_send_control(bytes) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind == TransportErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(transport_timeout());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(error) => return Err(map_transport(&error)),
        }
    }
}

async fn receive_message(
    connection: &mut LocalConnection,
    timeout: Duration,
) -> Result<Vec<u8>, DistributedError> {
    let deadline = Instant::now() + timeout;
    loop {
        match connection.try_receive() {
            Ok(Some(bytes)) => return Ok(bytes),
            Ok(None) => {}
            Err(error) if error.kind == TransportErrorKind::WouldBlock => {}
            Err(error) => return Err(map_transport(&error)),
        }
        if Instant::now() >= deadline {
            return Err(transport_timeout());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn endpoint_id(node: &NodeId) -> EndpointId {
    let digest = Sha256::digest(node.0.as_bytes());
    EndpointId::from_bytes(digest[..16].try_into().expect("SHA prefix"))
}

fn transport_budget() -> TransportBudget {
    TransportBudget {
        max_frame_bytes: mutsuki_distributed_contracts::MAX_CONTROL_FRAME_BYTES,
        idle_timeout: None,
        ..TransportBudget::default()
    }
}

fn nonce(node: &NodeId) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(AUTH_CONTEXT);
    hash.update(node.0.as_bytes());
    hash.update(now_millis().to_le_bytes());
    hash.update(std::process::id().to_le_bytes());
    hash.update(NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    hash.finalize().into()
}

fn auth_proof(
    secret: &[u8],
    role: &[u8],
    local: &NodeId,
    remote: &NodeId,
    nonce: &[u8; 32],
    peer_nonce: &[u8; 32],
) -> Result<[u8; 32], DistributedError> {
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| auth_error())?;
    mac.update(AUTH_CONTEXT);
    mac.update(role);
    mac.update(local.0.as_bytes());
    mac.update(remote.0.as_bytes());
    mac.update(nonce);
    mac.update(peer_nonce);
    Ok(mac.finalize().into_bytes().into())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn map_transport(error: &mutsuki_link::TransportError) -> DistributedError {
    let kind = match error.kind {
        TransportErrorKind::TimedOut => DistributedErrorKind::WorkerUnavailable,
        _ => DistributedErrorKind::TransportClosed,
    };
    DistributedError::new(kind, "authenticated sidecar control transport failed")
}

const fn transport_timeout() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::WorkerUnavailable,
        "authenticated sidecar control request timed out",
    )
}

const fn auth_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Protocol,
        "authenticated sidecar control session could not be established",
    )
}

const fn protocol_error(message: &'static str) -> DistributedError {
    DistributedError::new(DistributedErrorKind::Protocol, message)
}
