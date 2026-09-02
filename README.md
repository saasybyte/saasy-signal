# saasy-signal

Control-plane signaling server for [SaasyByte](https://github.com/saasybyte/saasybyte), an open-source real-time AI voice platform.

Signal is the central coordination hub: every client (the web app, and the AI Orchestrator acting as a participant) connects to it over WebSocket, speaking Proto3 binary envelopes. It owns session lifecycle, validates JWTs issued by the auth service, allocates media resources on the SFU over gRPC, generates time-limited TURN credentials, publishes the session lifecycle events that bring the AI into a session, and enforces per-session usage budgets against the auth service.

## How It Fits

- **Serves the web client** (WebSocket): session setup and WebRTC negotiation.
- **Serves saasy-orchestrator** (WebSocket): session lifecycle events; the Orchestrator also joins sessions as a participant.
- **Calls saasy-sfu** (gRPC): media resource management. Signal never touches media itself.
- **Calls saasy-core** (gRPC): usage/budget tracking during active sessions.
- **Proto types** come from [saasy-proto-rust](https://github.com/saasybyte/saasy-proto-rust) (git dependency).

See the [platform overview](https://github.com/saasybyte/saasybyte) for the full architecture.

## Build & Run

Requirements: stable Rust toolchain, `protoc` (protobuf compiler).

```bash
make run            # run dev server
make build          # debug build
make release        # release build
make test           # run tests
make clippy-strict  # lint, fail on warnings
```

Configuration is layered: `config/default.toml` provides defaults, overridden by environment variables (loaded from `.env` via dotenvy). See `.env.example` for the variables a real deployment needs (SFU and Core endpoints, JWT public key, optional `COTURN_*` settings, which must be set all four together or not at all).

A `Dockerfile` is included; `docker build .` needs no credentials.

## License

Apache-2.0, see [LICENSE](LICENSE).
