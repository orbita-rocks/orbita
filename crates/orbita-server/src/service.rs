//! The client-facing gRPC surface.
//!
//! This layer is thin on purpose. It turns a tonic request into the node's
//! request, and an `orbita_core::Error` into a status through the one mapping
//! in [`crate::status`]. Everything else, meaning routing, conditions, and the
//! read path, belongs to the node so that a request that arrives over the peer
//! transport takes exactly the same path as one that arrives over gRPC.

use crate::node::Node;
use crate::status::to_status;

use orbita_proto::v1::kv_server::Kv;
use orbita_proto::v1::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, ListRequest, ListResponse, SetRequest,
    SetResponse,
};
use orbita_runtime::Runtime;

use std::sync::Arc;
use tonic::{Request, Response, Status};

pub(crate) struct KvService<R: Runtime> {
    node: Arc<Node<R>>,
}

impl<R: Runtime> KvService<R> {
    pub(crate) fn new(node: Arc<Node<R>>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl<R: Runtime> Kv for KvService<R> {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        self.node
            .get(request.into_inner(), false)
            .await
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        self.node
            .set(request.into_inner(), false)
            .await
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.node
            .delete(request.into_inner(), false)
            .await
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }

    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        self.node
            .list(request.into_inner(), false)
            .await
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }
}
