// File descriptor utilities for hot reload functionality
//!
//! This module provides safe wrappers around unsafe file descriptor operations
//! needed for hot reload socket handoff between shotover instances.

use anyhow::{Context, Result};
use std::os::unix::io::{FromRawFd, RawFd};
use tokio::net::TcpListener;
use tracing::{debug, warn};

/// Safely recreate a TcpListener from a raw file descriptor
///
/// This function takes a raw file descriptor that represents a listening TCP socket
/// and converts it back into a tokio::net::TcpListener that can be used by the new
/// shotover instance.
///
/// # Safety
///
/// This function uses unsafe code to convert a raw file descriptor into a TcpListener.
/// The caller must ensure that:
/// 1. The file descriptor is valid and represents a TCP listening socket
/// 2. The file descriptor is not used elsewhere after this call
/// 3. The original socket was properly configured (non-blocking, etc.)
///
/// # Arguments
///
/// * `raw_fd` - The raw file descriptor from the old shotover instance
/// * `expected_addr` - The expected address for validation (optional)
///
/// # Returns
///
/// Returns a Result containing the recreated TcpListener or an error if the
/// conversion fails.
pub fn recreate_tcp_listener_from_fd(
    raw_fd: RawFd,
    expected_addr: Option<&str>,
) -> Result<TcpListener> {
    debug!("Recreating TcpListener from file descriptor: {}", raw_fd);

    // Step 1: Convert raw FD to std::net::TcpListener
    let std_listener = unsafe {
        // SAFETY: We assume the caller has provided a valid file descriptor
        // that represents a TCP listening socket. This is unsafe because
        // we cannot verify the FD is valid or that it's not used elsewhere.
        std::net::TcpListener::from_raw_fd(raw_fd)
    };

    // Step 2: Validate the socket if we have an expected address
    if let Some(expected) = expected_addr {
        match std_listener.local_addr() {
            Ok(actual_addr) => {
                debug!("Socket local address: {}", actual_addr);
                // Basic validation - check if the address matches expectations
                if !actual_addr
                    .to_string()
                    .contains(expected.split(':').next_back().unwrap_or(""))
                {
                    warn!(
                        "Socket address mismatch. Expected: {}, Actual: {}",
                        expected, actual_addr
                    );
                }
            }
            Err(e) => {
                warn!("Could not get local address for validation: {}", e);
            }
        }
    }

    // Step 3: Ensure the socket is in non-blocking mode
    std_listener
        .set_nonblocking(true)
        .context("Failed to set socket to non-blocking mode")?;

    // Step 4: Convert to tokio TcpListener
    let tokio_listener = TcpListener::from_std(std_listener)
        .context("Failed to convert std::net::TcpListener to tokio::net::TcpListener")?;

    debug!("Successfully recreated TcpListener from FD {}", raw_fd);
    Ok(tokio_listener)
}

/// Validate that a raw file descriptor represents a valid TCP listening socket
///
/// This function performs basic validation on a file descriptor to ensure it
/// represents a TCP socket in listening state before attempting to recreate
/// a TcpListener from it.
///
/// # Arguments
///
/// * `raw_fd` - The raw file descriptor to validate
///
/// # Returns
///
/// Returns true if the file descriptor appears to be a valid TCP listening socket
pub fn validate_tcp_listener_fd(raw_fd: RawFd) -> bool {
    // Create a temporary std::net::TcpListener to test the FD
    let test_listener = unsafe {
        // SAFETY: This is a validation function, we're creating a temporary
        // listener just to check if the FD is valid. We'll drop it immediately.
        std::net::TcpListener::from_raw_fd(raw_fd)
    };

    // Try to get the local address - if this fails, the FD is not valid
    match test_listener.local_addr() {
        Ok(addr) => {
            debug!("FD {} validated as TCP listener on {}", raw_fd, addr);
            // Don't drop the listener here - it would close the socket
            // Instead, we need to "forget" it so the FD remains open
            std::mem::forget(test_listener);
            true
        }
        Err(e) => {
            debug!("FD {} validation failed: {}", raw_fd, e);
            // Don't drop the listener here either - we don't want to close
            // a potentially valid FD just because validation failed
            std::mem::forget(test_listener);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_recreate_tcp_listener_from_fd() {
        // Create a test listener to get a valid FD
        let original_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = original_listener.local_addr().unwrap();
        let _raw_fd = original_listener.as_raw_fd();

        // Convert to std listener and extract FD
        let std_listener = original_listener.into_std().unwrap();
        let extracted_fd = std_listener.as_raw_fd();

        // Forget the std listener so it doesn't close the FD
        std::mem::forget(std_listener);

        // Now recreate the listener from the FD
        let recreated_listener =
            recreate_tcp_listener_from_fd(extracted_fd, Some(&local_addr.to_string())).unwrap();

        // Verify the recreated listener has the same address
        assert_eq!(recreated_listener.local_addr().unwrap(), local_addr);
    }

    #[tokio::test]
    async fn test_validate_tcp_listener_fd() {
        // Create a test listener to get a valid FD
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raw_fd = listener.as_raw_fd();

        // Validate the FD
        assert!(validate_tcp_listener_fd(raw_fd));

        // Test with an invalid FD (this might not work on all systems)
        assert!(!validate_tcp_listener_fd(-1));
    }
}
