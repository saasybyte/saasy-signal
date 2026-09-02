# saasy-signal

## Commands
- `make run` — run dev server
- `make build` / `make release` — debug / release build
- `make check` — fast compilation check (no codegen)
- `make test` — run tests
- `make clippy` / `make clippy-strict` — lint / lint with `-D warnings`
- `make fmt` — format code

## Conventions
- **Error types**: per-module enums with `thiserror::Error` (e.g., `SfuClientError`, `AuthError`). No `anyhow`.
- **Logging**: `tracing` crate (`info!`, `warn!`, `error!`, `debug!`). Not `log` or `println!`.
- **Entry point**: `#[actix_web::main]`, not `#[tokio::main]`.
- **WebSocket upgrades**: require `#[allow(clippy::future_not_send)]` — actix-ws types are `!Send` but this is safe because Actix runs a per-thread executor.
- **Shared state**: `Arc<RwLock<_>>` for read-heavy state, `Arc<Mutex<_>>` for exclusive access (e.g., gRPC clients).
- **SessionManager Clone**: manual `Clone` impl that `Arc::clone`s all fields — shared state, not deep copy.
- **Proto types**: from `saasy-proto-rust` (git dep): `saasy_proto_rust::{signal, sfu, shared}`.
- **gRPC client pattern**: wraps tonic client in `Mutex`, each method builds a request envelope with `type` string + data variant, then match-extracts the response data variant.
- **Double-termination guard**: `SessionManager.terminating_sessions` (HashSet) prevents concurrent terminate calls on the same session.
- **Coturn config**: all-or-nothing — all 4 `COTURN_*` env vars must be set together, or none at all.

## Service Boundaries
- **Calls saasy-sfu** (gRPC): media resource management. Signal does not handle media directly.
- **Calls saasy-core** (gRPC): usage/budget tracking during active sessions.
- **Serves web client** (WebSocket): session setup and WebRTC negotiation.
- **Serves saasy-orchestrator** (WebSocket): session lifecycle events. Orchestrator also joins sessions as a participant.
- **Proto types from saasy-proto-rust** (git dep): do not define proto types locally.
- **Does not own**: auth token issuance (saasy-core), media forwarding (saasy-sfu), AI inference (saasy-orchestrator), proto schema (saasy-proto-rust).
