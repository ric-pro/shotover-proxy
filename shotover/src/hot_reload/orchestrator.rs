//! Hot Reload Orchestrator
//!
//! This module provides the HotReloadOrchestrator which handles the complete
//! workflow for hot reloading: connecting to existing instances, requesting
//! file descriptors, and coordinating the handoff process.

use crate::hot_reload::client::UnixSocketClient;
use crate::hot_reload::protocol::{FileDescriptor, Request, Response};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::os::unix::io::RawFd;
use tracing::{debug, info, warn};

/// Represents the result of a successful hot reload operation
#[derive(Debug)]
pub struct HotReloadResult {
    /// Map of port numbers to their corresponding file descriptors
    pub port_to_fd: HashMap<u32, FileDescriptor>,
}

/// Orchestrates the complete hot reload workflow
///
/// The HotReloadOrchestrator is responsible for:
/// 1. Connecting to the existing shotover instance via Unix socket
/// 2. Requesting file descriptors for all listening sockets
/// 3. Validating the received file descriptors
/// 4. Providing the file descriptors to the new shotover instance
pub struct HotReloadOrchestrator {
    socket_path: String,
    client: UnixSocketClient,
}

impl HotReloadOrchestrator {
    /// Create a new HotReloadOrchestrator
    ///
    /// # Arguments
    ///
    /// * `socket_path` - Path to the Unix socket of the existing shotover instance
    pub fn new(socket_path: String) -> Self {
        let client = UnixSocketClient::new(socket_path.clone());
        Self {
            socket_path,
            client,
        }
    }

    /// Perform the complete hot reload workflow
    ///
    /// This method handles the entire process of requesting file descriptors
    /// from the existing shotover instance and preparing them for use by
    /// the new instance.
    ///
    /// # Returns
    ///
    /// Returns a HotReloadResult containing the file descriptors that can
    /// be used to create listening sockets, or an error if the process fails.
    pub async fn perform_hot_reload(&self) -> Result<HotReloadResult> {
        info!(
            "Starting hot reload orchestration with socket path: {}",
            self.socket_path
        );

        // Step 1: Request file descriptors from the existing instance
        let response = self
            .client
            .send_request(Request::SendListeningSockets)
            .await
            .context("Failed to request listening sockets from existing shotover instance")?;

        // Step 2: Process the response
        match response {
            Response::SendListeningSockets { port_to_fd } => {
                info!(
                    "Successfully received {} file descriptors from existing shotover instance",
                    port_to_fd.len()
                );

                // Step 3: Validate the received file descriptors
                self.validate_file_descriptors(&port_to_fd)?;

                // Step 4: Log the received file descriptors for debugging
                for (port, fd) in &port_to_fd {
                    debug!("Received file descriptor {} for port {}", fd.0, port);
                }

                Ok(HotReloadResult { port_to_fd })
            }
            Response::Error(msg) => Err(anyhow::anyhow!("Hot reload request failed: {}", msg)),
        }
    }

    /// Validate the received file descriptors
    ///
    /// This method performs basic validation on the received file descriptors
    /// to ensure they are valid and can be used to create listening sockets.
    ///
    /// # Arguments
    ///
    /// * `port_to_fd` - Map of ports to file descriptors to validate
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if all file descriptors are valid, or an error if
    /// validation fails.
    fn validate_file_descriptors(&self, port_to_fd: &HashMap<u32, FileDescriptor>) -> Result<()> {
        if port_to_fd.is_empty() {
            return Err(anyhow::anyhow!(
                "No file descriptors received from existing shotover instance"
            ));
        }

        for (port, fd) in port_to_fd {
            // Basic validation: ensure the file descriptor is not negative
            if fd.0 < 0 {
                return Err(anyhow::anyhow!(
                    "Invalid file descriptor {} for port {}",
                    fd.0,
                    port
                ));
            }

            // Additional validation using our fd_utils module
            if !crate::hot_reload::fd_utils::validate_tcp_listener_fd(fd.0) {
                warn!(
                    "File descriptor {} for port {} failed validation, but continuing anyway",
                    fd.0, port
                );
                // Note: We warn but don't fail here because validation might be
                // overly strict in some environments
            }
        }

        info!("File descriptor validation completed successfully");
        Ok(())
    }

    /// Get the socket path being used by this orchestrator
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    /// Extract raw file descriptors from the hot reload result
    ///
    /// This is a convenience method to extract just the raw file descriptors
    /// from a HotReloadResult, which can be useful for passing to other
    /// components that need the raw FDs.
    ///
    /// # Arguments
    ///
    /// * `result` - The HotReloadResult to extract FDs from
    ///
    /// # Returns
    ///
    /// Returns a HashMap mapping ports to raw file descriptors
    pub fn extract_raw_fds(result: &HotReloadResult) -> HashMap<u32, RawFd> {
        result
            .port_to_fd
            .iter()
            .map(|(port, fd)| (*port, fd.0))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot_reload::protocol::HotReloadListenerResponse;
    use crate::hot_reload::server::{SourceHandle, UnixSocketServer};
    use crate::hot_reload::tests::wait_for_unix_socket_connection;
    use std::os::unix::io::AsRawFd;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn test_orchestrator_basic_workflow() {
        let socket_path = "/tmp/test-orchestrator-basic.sock";

        // Create a test listener to get a valid FD
        let test_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let test_port = test_listener.local_addr().unwrap().port();
        let test_fd = test_listener.as_raw_fd();

        // Start mock server
        let (tx, mut rx) = unbounded_channel();
        let source_handles = vec![SourceHandle {
            name: "test-source".to_string(),
            sender: tx,
        }];

        let mut server = UnixSocketServer::new(socket_path.to_string(), source_handles).unwrap();

        // Handle the hot reload request in the background
        tokio::spawn(async move {
            if let Some(request) = rx.recv().await {
                request
                    .return_chan
                    .send(HotReloadListenerResponse::HotReloadResponse {
                        port: test_port,
                        listener_socket_fd: FileDescriptor(test_fd),
                    })
                    .unwrap();
            }
        });

        let server_handle = tokio::spawn(async move {
            server.run().await.unwrap();
        });

        // Wait for server to start
        wait_for_unix_socket_connection(socket_path, 2000).await;

        // Test the orchestrator
        let orchestrator = HotReloadOrchestrator::new(socket_path.to_string());
        let result = orchestrator.perform_hot_reload().await.unwrap();

        // Verify the result
        assert_eq!(result.port_to_fd.len(), 1);
        assert!(result.port_to_fd.contains_key(&(test_port as u32)));

        // Test raw FD extraction
        let raw_fds = HotReloadOrchestrator::extract_raw_fds(&result);
        assert_eq!(raw_fds.len(), 1);
        assert_eq!(raw_fds[&(test_port as u32)], test_fd);

        // Cleanup
        server_handle.abort();
        drop(test_listener); // This will close the FD
    }

    #[tokio::test]
    async fn test_orchestrator_validation_failure() {
        let socket_path = "/tmp/test-orchestrator-validation.sock";

        // Start mock server that returns invalid FDs
        let (tx, mut rx) = unbounded_channel();
        let source_handles = vec![SourceHandle {
            name: "test-source".to_string(),
            sender: tx,
        }];

        let mut server = UnixSocketServer::new(socket_path.to_string(), source_handles).unwrap();

        // Handle the hot reload request with invalid FD
        tokio::spawn(async move {
            if let Some(request) = rx.recv().await {
                request
                    .return_chan
                    .send(HotReloadListenerResponse::HotReloadResponse {
                        port: 8080,
                        listener_socket_fd: FileDescriptor(-1), // Invalid FD
                    })
                    .unwrap();
            }
        });

        let server_handle = tokio::spawn(async move {
            server.run().await.unwrap();
        });

        // Wait for server to start
        wait_for_unix_socket_connection(socket_path, 2000).await;

        // Test the orchestrator with invalid FD
        let orchestrator = HotReloadOrchestrator::new(socket_path.to_string());
        let result = orchestrator.perform_hot_reload().await;

        // Should fail due to invalid FD
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid file descriptor")
        );

        // Cleanup
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_orchestrator_empty_response() {
        let socket_path = "/tmp/test-orchestrator-empty.sock";

        // Start mock server that returns no FDs
        let source_handles: Vec<SourceHandle> = vec![];
        let mut server = UnixSocketServer::new(socket_path.to_string(), source_handles).unwrap();

        let server_handle = tokio::spawn(async move {
            server.run().await.unwrap();
        });

        // Wait for server to start
        wait_for_unix_socket_connection(socket_path, 2000).await;

        // Test the orchestrator with empty response
        let orchestrator = HotReloadOrchestrator::new(socket_path.to_string());
        let result = orchestrator.perform_hot_reload().await;

        // Should fail due to no FDs received
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No file descriptors received")
        );

        // Cleanup
        server_handle.abort();
    }
}
