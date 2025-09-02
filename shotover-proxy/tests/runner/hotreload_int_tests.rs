use crate::shotover_process;
use redis::{Client, Commands};
use test_helpers::docker_compose::docker_compose;

#[tokio::test]
async fn test_hotreload_basic_valkey_connection() {
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_process = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con = client.get_connection().unwrap();

    let _: () = con.set("test_key", "test_value").unwrap();
    let result: String = con.get("test_key").unwrap();
    assert_eq!(result, "test_value");
    let pong: String = redis::cmd("PING").query(&mut con).unwrap();
    assert_eq!(pong, "PONG");

    shotover_process.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_dual_shotover_instances_with_valkey() {
    let socket_path = "/tmp/test-hotreload.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_a = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_a = client_a.get_connection().unwrap();
    let _: () = con_a.set("key_from_a", "value_from_a").unwrap();

    //Second shotover
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology-alt.yaml")
        .with_hotreload_from_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_b = Client::open("valkey://127.0.0.1:6381").unwrap();
    let mut con_b = client_b.get_connection().unwrap();
    let _: () = con_b.set("key_from_b", "value_from_b").unwrap();

    let value_a_from_b: String = con_b.get("key_from_a").unwrap();
    assert_eq!(value_a_from_b, "value_from_a");
    let value_b_from_a: String = con_a.get("key_from_b").unwrap();
    assert_eq!(value_b_from_a, "value_from_b");

    let final_check: String = con_a.get("key_from_a").unwrap();
    assert_eq!(final_check, "value_from_a");

    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_actual_hotreload_socket_takeover() {
    let socket_path = "/tmp/test-hotreload.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    // Start original shotover with hot reload enabled
    let shotover_original = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Connect to original instance and set some data
    let client = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con = client.get_connection().unwrap();
    let _: () = con.set("original_key", "original_value").unwrap();

    // Verify original instance works
    let result: String = con.get("original_key").unwrap();
    assert_eq!(result, "original_value");

    // Start new shotover instance that should take over the socket
    let shotover_new = shotover_process("tests/test-configs/hotreload/topology.yaml") // SAME topology
        .with_hotreload_from_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Give time for socket handoff
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // NEW CONNECTIONS should now go to the new instance
    let client_new = Client::open("valkey://127.0.0.1:6380").unwrap(); // SAME PORT
    let mut con_new = client_new.get_connection().unwrap();

    // Set data through new connection (should go to new instance)
    let _: () = con_new.set("new_key", "new_value").unwrap();

    // Verify new connection works
    let result: String = con_new.get("new_key").unwrap();
    assert_eq!(result, "new_value");

    // EXISTING connection should still work (handled by original instance)
    let existing_result: String = con.get("original_key").unwrap();
    assert_eq!(existing_result, "original_value");

    // Both instances should be able to read data from backend
    let cross_read: String = con_new.get("original_key").unwrap();
    assert_eq!(cross_read, "original_value");

    shotover_original
        .shutdown_and_then_consume_events(&[])
        .await;
    shotover_new.shutdown_and_then_consume_events(&[]).await;
}
