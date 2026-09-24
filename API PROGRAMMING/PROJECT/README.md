# Georuggine — Distributed Fleet Telemetry & Management Platform

[![Rust](https://img.shields.io/badge/rust-edition%202024-orange.svg)](https://www.rust-lang.org)
[![Tokio](https://img.shields.io/badge/async-tokio%201.53-blue.svg)](https://tokio.rs)
[![Database](https://img.shields.io/badge/persistence-SQLite%20ACID-003B57.svg)](https://sqlite.org)
[![Security](https://img.shields.io/badge/auth-bcrypt-brightgreen.svg)](https://github.com/Keats/rust-bcrypt)
[![Documentation](https://img.shields.io/badge/docs-manuals%20available-informational.svg)](docs/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Georuggine** is a high-performance, distributed geospatial telemetry and real-time fleet management system developed in **100% Safe Rust** on top of the **Tokio** asynchronous runtime. Engineered for enterprise telematics, logistics tracking, and connected commercial vehicle fleets, Georuggine coordinates real-time vehicular GPS streams, evaluates kinematic finite state machines, executes geodesic spherical trigonometry, maintains embedded relational ACID persistence, enables bidirectional driver-operator messaging, and provides live CPU performance profiling.

---

## 📑 Table of Contents
- [1. Project Mission & Final Goal](#1-project-mission--final-goal)
- [2. System Architecture](#2-system-architecture)
- [3. Functional Requirements & Specifications](#3-functional-requirements--specifications)
  - [3.1 Authentication & Session Security](#31-authentication--session-security)
  - [3.2 Telemetry Ingestion & GPS Cadence](#32-telemetry-ingestion--gps-cadence)
  - [3.3 Kinematic Finite State Machine (FSM)](#33-kinematic-finite-state-machine-fsm)
  - [3.4 Geodesic Analytics & Spherical Trigonometry](#34-geodesic-analytics--spherical-trigonometry)
  - [3.5 Time-Window Aggregations & Boundary Interpolation](#35-time-window-aggregations--boundary-interpolation)
  - [3.6 Bidirectional Messaging Subsystem](#36-bidirectional-messaging-subsystem)
  - [3.7 Embedded SQLite ACID Persistence](#37-embedded-sqlite-acid-persistence)
  - [3.8 Vehicle Movement Simulation Strategies](#38-vehicle-movement-simulation-strategies)
  - [3.9 CPU Performance Profiling & Monitoring](#39-cpu-performance-profiling--monitoring)
- [4. Non-Functional & Engineering Attributes](#4-non-functional--engineering-attributes)
- [5. Wire Protocol Specification](#5-wire-protocol-specification)
- [6. Repository Layout](#6-repository-layout)
- [7. Getting Started](#7-getting-started)
  - [Prerequisites & Build](#prerequisites--build)
  - [Running the Server Daemon](#running-the-server-daemon)
  - [Interactive Server Shell Commands](#interactive-server-shell-commands)
  - [Running Vehicle OBU Clients](#running-vehicle-obu-clients)
- [8. Verification & Test Suite](#8-verification--test-suite)
- [9. Technical Documentation Deliverables](#9-technical-documentation-deliverables)
- [10. Author & Metadata](#10-author--metadata)

---

## 1. Project Mission & Final Goal

Modern logistics and ground transportation infrastructure require distributed telematics platforms capable of handling high-frequency telemetry streams from geographically dispersed vehicular On-Board Units (OBUs). The final goal of the **Georuggine** platform is to provide a complete, reliable, and memory-safe infrastructure that solves four fundamental engineering challenges:

1. **Deterministic Kinematic Tracking**: Automatically categorizing vehicle states (`Moving`, `Stopped`, `Disconnected`) from raw latitude and longitude feeds, distinguishing true operational halts from sensor noise and traffic signals using a strict 180-second stasis threshold.
2. **Precision Spherical Geodesics**: Evaluating true great-circle surface distances across the Earth's curvature via the Haversine trigonometric formulation, delivering auditable distance, speed, and pause metrics across calendar windows (Current Day, Current Week, Current Month).
3. **Resilient Dual-End Concurrency**: Managing concurrent bi-directional communication channels over TCP where vehicles stream sensor data and exchange operator dispatches simultaneously, backed by bounded asynchronous mailboxes that prevent slow consumers from exhausting server memory.
4. **Zero-Loss Embedded Persistence**: Storing vehicle state transitions and historic telemetry in an embedded SQLite relational database, enabling instant state reconstitution and analytical continuity across server reboots.

---

## 2. System Architecture

Georuggine is organized into a shared domain foundation and two independent binary executable targets:

```text
                           +-----------------------------------------------+
                           |          FLEET ADMINISTRATION CONSOLE         |
                           |       (Interactive Stdin Shell Interface)     |
                           +-----------------------+-----------------------+
                                                   |
                                                   v
                           +-----------------------------------------------+
                           |             ASYNC TOKIO TCP SERVER            |
                           |            (src/bin/server/runtime.rs)        |
                           +-------+---------------+---------------+-------+
                                   |               |               |
             +---------------------+               |               +---------------------+
             v                                     v                                     v
+---------------------------+        +---------------------------+        +---------------------------+
|  EMBEDDED SQLite ACID DB  |        |    KINEMATIC FSM ENGINE   |        |   GEODESIC MATH ENGINE    |
|   (src/bin/server/state)  |        |    (src/bin/server/state) |        |        (src/lib.rs)       |
|  - Users & Credentials    |        |  - Moving vs Stopped      |        |  - Haversine Distance     |
|  - Position Breadcrumbs   |        |  - 180s Stasis Logic      |        |  - UTC Calendar Slices    |
|  - Bcrypt Password Hashes |        |  - 2m Jitter Elimination  |        |  - Proportional Boundary  |
+---------------------------+        +---------------------------+        +---------------------------+
                                                   ^
                                                   |  Framed JSON Lines over TCP
                                                   v
                           +-----------------------------------------------+
                           |          ON-BOARD UNIT (OBU) CLIENTS          |
                           |            (src/bin/client/runtime.rs)        |
                           +-----------------------+-----------------------+
                                                   |
                             +---------------------+---------------------+
                             v                                           v
               +---------------------------+               +---------------------------+
               |   GPS SIMULATION ENGINE   |               |    RAW-MODE TERMINAL TUI  |
               |  - CSV Route Playback     |               |  - Non-blocking Keystroke |
               |  - Point A -> B with Stop |               |  - Line-editing & History |
               |  - Synthetic Random Walk  |               |  - Driver Messaging Input |
               +---------------------------+               +---------------------------+
```

---

## 3. Functional Requirements & Specifications

### 3.1 Authentication & Session Security
- **Registration**: Fleet operators or vehicles submit a desired `username` (4 to 20 alphanumeric characters) and `password` (minimum 8 characters).
- **Password Hashing**: Passwords are never stored in plaintext. They are salted and hashed using `bcrypt` with cost factor 10.
- **Session Mutual Exclusion**: A registered driver can maintain at most one active connection. Secondary login attempts from a different socket while an existing session is open are strictly rejected.
- **Auto-Login**: Successful account registration automatically establishes an authenticated session on the same socket connection.

### 3.2 Telemetry Ingestion & GPS Cadence
- **Cadence**: Active vehicle clients sample and transmit coordinates every **30 seconds**.
- **Data Model**: Each update packages:
  - `latitude`: Signed floating-point coordinate ($\in [-90.0, +90.0]$).
  - `longitude`: Signed floating-point coordinate ($\in [-180.0, +180.0]$).
  - `timestamp`: 64-bit unsigned Unix epoch timestamp in seconds.
- **Temporal Monotonicity**: The server enforces monotonic timestamps; updates carrying timestamps older than the vehicle's last recorded position are rejected.

### 3.3 Kinematic Finite State Machine (FSM)
The system maintains a deterministic FSM for each registered vehicle:

```text
               +---------------+
               | DISCONNECTED  |
               +-------+-------+
                       | (Authentication)
                       v
                 +-----------+
                 |  STOPPED  | <---------------+
                 +-----+-----+                 |
                       |                       | Stasis Threshold
                       | Displacement > 2.0 m  | (No movement for >= 180s)
                       v                       |
                 +-----------+                 |
                 |  MOVING   +-----------------+
                 +-----+-----+
                       |
                       | Connection Drop / EOF
                       v
               +---------------+
               | DISCONNECTED  |
               +---------------+
```

- **GPS Jitter Rejection**: Real-world GPS receivers exhibit floating-point oscillation when stationary. Coordinate shifts $\le 2.0\,\text{meters}$ are classified as stationary jitter and do not trigger transitions to `Moving`.
- **180-Second Stasis Threshold**: When a vehicle in `Moving` state sends identical (or sub-2m) coordinates continuously for $\ge 180\,\text{seconds}$ ($6 \times 30\,\text{s}$ consecutive reports), the FSM transitions to `Stopped`.

### 3.4 Geodesic Analytics & Spherical Trigonometry
Distance along the Earth's spherical mantle is computed using the **Great-Circle Haversine Formulation**:

$$\Delta\sigma = 2 \arcsin \left( \sqrt{ \sin^2\left(\frac{\Delta\phi}{2}\right) + \cos(\phi_1)\cos(\phi_2)\sin^2\left(\frac{\Delta\lambda}{2}\right) } \right)$$

$$d = R \cdot \Delta\sigma$$

Where:
- $R = 6371.0\,\text{km}$ (mean Earth radius).
- $\phi_1, \phi_2$ are latitudes in radians; $\Delta\phi = \phi_2 - \phi_1$.
- $\lambda_1, \lambda_2$ are longitudes in radians; $\Delta\lambda = \lambda_2 - \lambda_1$.
- Safe numerical clamping is applied to prevent floating-point `NaN` artifacts.

### 3.5 Time-Window Aggregations & Boundary Interpolation
The server computes operational metrics filtered by standard UTC calendar boundaries:
- **`Day`**: From 00:00:00 UTC of the current calendar day.
- **`Week`**: From Monday 00:00:00 UTC of the current calendar week.
- **`Month`**: From the 1st day 00:00:00 UTC of the current calendar month.

#### Proportional Boundary Interpolation
When a movement interval $[t_A, t_B]$ straddles the lower window boundary $T_W$ ($t_A < T_W < t_B$), Georuggine computes the proportional time fraction:

$$\alpha = \frac{t_B - T_W}{t_B - t_A}$$

The distance credited to the window is scaled proportionally: $d_{\text{credited}} = \alpha \cdot d_{AB}$, ensuring that historical travel preceding the boundary is not erroneously billed to the current period.

Computed Metrics:
- **Total Distance Traveled** ($d_{\text{tot}}$ in km)
- **Total Moving Duration** ($T_{\text{mov}}$ in seconds / formatted $hh:mm:ss$)
- **Total Stopped Duration** ($T_{\text{stop}}$ in seconds / formatted $hh:mm:ss$)
- **Average Moving Speed**: $v_{\text{avg}} = \frac{d_{\text{tot}}}{T_{\text{mov}}} \times 3600\,\text{km/h}$

### 3.6 Bidirectional Messaging Subsystem
- **Admin Broadcast**: Global alert dispatched by the operator console, delivered asynchronously to all live connected drivers.
- **Admin Unicast / Direct Message**: Targeted dispatch routed strictly to a specific driver's active terminal session.
- **Driver-to-Admin Messaging**: Drivers can enter and transmit messages from their in-cab console to the central dispatch desk.
- **Non-blocking Dispatch**: Mailbox capacity is bounded; a slow or unresponsive client connection does not block the server event loop or delay messages destined for other vehicles.

### 3.7 Embedded SQLite ACID Persistence
All mission-critical state is persisted in an embedded SQLite database (`georuggine.db`):
- **`users` Table**: Stores `username`, `password_hash`, and creation timestamps.
- **`positions` Table**: Stores sequential telemetry records indexed by `(username, timestamp)`.
- **Reboot Resilience**: On startup, the server automatically initializes tables, reads existing user records, loads historic position counts, and resumes uninterrupted analytics without requiring data migration.

### 3.8 Vehicle Movement Simulation Strategies
The client subsystem (`src/bin/client/movement.rs`) provides 4 modular trajectory simulation strategies:
1. **CSV Route Playback**: Reads real-world waypoint sequences from structured CSV files (`latitude,longitude,timestamp_offset`) like `torino_asti.csv`.
2. **Point A $\rightarrow$ Point B with Pauses**: Generates a linear vector trajectory between geographic coordinates with programmable intermediate dwell times.
3. **Synthetic Pseudo-Random Walk**: Generates smooth, temporally coherent random trajectories for stress-testing server scalability.
4. **Interactive Manual Input**: Allows manual coordinate overrides directly from the client terminal.

### 3.9 CPU Performance Profiling & Monitoring
- **Periodic Background Sampling**: Every 120 seconds (2 minutes), a background Tokio task samples the server's instantaneous CPU consumption and physical memory usage.
- **Disk Logging**: Samples are appended to `cpu_usage.log` with timestamp, elapsed process time, CPU %, and memory in MB.
- **ASCII Histogram**: Operators can issue the `chart` command in the interactive shell to display an ASCII histogram of CPU consumption trends over time.

---

## 4. Non-Functional & Engineering Attributes

- **100% Safe Rust**: Complete prohibition of `unsafe` code blocks (`#![forbid(unsafe_code)]` enforced).
- **Cross-Platform Compatibility**: Fully verified on **macOS**, **Linux** (x86_64 / aarch64), and **Windows**.
- **Minimized Executable Footprint**: Production release profile configures Link-Time Optimization (`lto = true`), size-optimization (`opt-level = "z"`), single codegen unit, debug symbol stripping, and `panic = "abort"` to minimize binary size.
- **Asynchronous Architecture**: Built on Tokio 1.53, using cooperative futures and `select!` branching to avoid OS thread exhaustion.

---

## 5. Wire Protocol Specification

Communication over TCP uses UTF-8 newline-delimited (`\n`) JSON frames.

### Client $\rightarrow$ Server Messages (`ClientMessage`)
```json
// Registration
{"Register": {"username": "driver01", "password": "SecurePassword123!"}}

// Authentication
{"Login": {"username": "driver01", "password": "SecurePassword123!"}}

// Telemetry Transmission (every 30s)
{"UpdatePosition": {"latitude": 45.0703, "longitude": 7.6869, "timestamp": 1726000000}}

// In-cab Driver Message
{"SendText": {"content": "Loading dock reached. Beginning unload."}}
```

### Server $\rightarrow$ Client Messages (`ServerMessage`)
```json
// Authentication Success
{"AuthResult": {"Ok": null}}

// Authentication Failure
{"AuthResult": {"Err": "Invalid username or password"}}

// Administrative Broadcast
{"Broadcast": {"message": "Severe weather warning: slowdown advised."}}

// Direct Dispatch
{"Direct": {"message": "Proceed to warehouse bay 4."}}

// Generic Error
{"ErrorMessage": "Timestamp must be greater than previous position"}
```

---

## 6. Repository Layout

```text
georuggine/
├── Cargo.toml                  # Workspace manifest and dependencies
├── Cargo.lock                  # Deterministic dependency tree
├── README.md                   # Authoritative system documentation
├── torino_asti.csv             # Reference real-world route dataset (Turin to Asti)
├── src/
│   ├── lib.rs                  # Core domain model, wire protocol & Haversine math
│   ├── terminal.rs             # Terminal raw-mode abstraction & interactive editor
│   ├── time.rs                 # Deterministic time provider & mock time injection
│   └── bin/
│       ├── server/             # Central telemetry server & fleet operator shell
│       │   ├── main.rs         # Server entrypoint and CLI argument parser
│       │   ├── cli.rs          # Interactive operator shell command handler
│       │   ├── error.rs        # Typed ServerError definitions
│       │   ├── handler.rs      # Socket line decoder and connection lifecycle
│       │   ├── logger.rs       # Background CPU & memory profiler
│       │   ├── runtime.rs      # Async TCP listener and task supervisor
│       │   └── state.rs        # SQLite state persistence & analytics engine
│       └── client/             # Vehicle OBU client & telemetry generator
│           ├── main.rs         # Client entrypoint and CLI argument parser
│           ├── cli.rs          # Raw-mode driver console & line editor
│           ├── movement.rs     # Route parsing & trajectory simulation
│           ├── network.rs      # Framed socket connection & reconnection logic
│           ├── runtime.rs      # Client async event loop supervisor
│           └── state.rs        # Vehicle local state tracker
├── tests/                      # Hierarchical test suites
│   ├── common/                 # Test database fixtures and mock time providers
│   ├── integration_client.rs   # Client network & telemetry tests
│   ├── integration_domain.rs   # Domain model & Haversine formula verification
│   ├── integration_server.rs   # Server state, auth, and analytics integration tests
│   └── e2e_system_test.rs      # Multi-vehicle end-to-end stress & lifecycle tests
└── docs/                       # Official technical documentation deliverables
    ├── manuale_del_progettista.pdf # Architectural & detailed design manual (82 pages)
    └── manuale_utente.pdf          # Operational manual for operators and drivers (29 pages)
```

---

## 7. Getting Started

### Prerequisites & Build
Ensure you have the Rust toolchain (version 1.85+) and SQLite3 development libraries installed.

```bash
# Clone the repository
git clone https://github.com/Beccaceci/georuggine.git
cd georuggine

# Compile debug targets
cargo build

# Compile optimized release binaries
cargo build --release
```

### Running the Server Daemon
Start the server daemon listening on an address and port (default: `127.0.0.1:8080`):

```bash
cargo run --bin server -- --addr 0.0.0.0:8080
```

Upon launch, the server displays configuration metadata and opens the interactive administrative shell:

```text
================================================================================
GEORUGGINE FLEET SERVER - STARTUP SUMMARY
================================================================================
  Server Version         : 0.1.0 (Rust Edition 2024)
  Network Listening Addr : 0.0.0.0:8080
  Database Path          : georuggine.db (SQLite ACID)
  CPU Monitor Interval   : 120s
================================================================================
[SHELL] > 
```

### Interactive Server Shell Commands
| Command | Parameters | Description | Example |
| :--- | :--- | :--- | :--- |
| `list` | None | Lists all registered drivers, states, and position counts | `list` |
| `stats` | `<driver> [day\|week\|month]` | Computes distance, speed, and moving/stopped times | `stats truck01 day` |
| `broadcast` | `<message>` | Sends a broadcast message to all connected drivers | `broadcast Caution: icy roads` |
| `direct` | `<driver> <message>` | Sends a private message to a specific driver | `direct truck01 Return to base` |
| `cpu` | None | Displays immediate CPU and memory utilization | `cpu` |
| `chart` | None | Toggles the 2-minute periodic ASCII CPU utilization chart | `chart` |
| `help` | None | Displays help message and syntax guide | `help` |
| `quit` / `exit` | None | Gracefully disconnects clients and shuts down server | `quit` |

### Running Vehicle OBU Clients
Launch vehicle clients by providing the server address and an optional route CSV file:

```bash
# Launch vehicle client 1
cargo run --bin client -- --addr 127.0.0.1:8080 --route torino_asti.csv

# Launch vehicle client 2
cargo run --bin client -- --addr 127.0.0.1:8080 --route torino_asti.csv
```

Interactive Client Prompt:
```text
[OBU] Choose authentication action: [1] Register, [2] Login
> 1
Username: truck01
Password: ********
[OBU] Authentication successful. Logged in as: truck01
[OBU] Transmitting GPS telemetry every 30 seconds...
> 
```
While driving, the driver can enter text at the prompt `> ` to message the dispatch desk, receive alerts from central administration, or use keyboard navigation with command history.

---

## 8. Verification & Test Suite

Georuggine incorporates a multi-tier test suite validating algorithmic invariants, concurrency guarantees, SQLite durability, and multi-vehicle network stress:

```bash
# 1. Run domain and core library tests (134 unit tests)
cargo test --lib

# 2. Run binary target unit tests
cargo test --bins

# 3. Run full end-to-end integration and concurrency stress suite
cargo test --test e2e_system_test
```

### Key Verified Scenarios
- **Multi-Vehicle Fleet Lifecycle**: Multiple vehicles registering, updating positions, transitioning across FSM states, and logging off cleanly.
- **Server Restart & Analytics Recovery**: Terminating and restarting the server daemon, confirming 100% telemetry retention and accurate statistical queries from the persistent SQLite store.
- **Fleet Churn & Race Conditions**: High-concurrency stress testing with simultaneous vehicle registrations, parallel GPS transmissions, and abrupt connection drops without deadlocks or mailbox saturation.
- **Adversarial & Malformed Traffic**: Injecting invalid JSON lines, unauthenticated updates, and corrupted payloads; verifying that adversarial traffic is safely isolated without impacting honest connected vehicles.

---

## 9. Technical Documentation Deliverables

Two comprehensive PDF technical manuals are preserved in the [`docs/`](docs/) directory:

- 📐 **[Manuale del Progettista (PDF)](docs/manuale_del_progettista.pdf)** *(82 pages)*: Complete technical reference covering system architecture, C4 context diagrams, SQLite relational schema, wire protocol specifications, concurrency topologies, and comparative performance analyses.
- 📖 **[Manuale Utente (PDF)](docs/manuale_utente.pdf)** *(29 pages)*: Operational manual detailing procedures for fleet operators and drivers, troubleshooting error codes (E01–E15), terminal recovery procedures, and deployment setups.

---

## 10. Author & Metadata

- **Author**: **Nicola Beccaceci**
- **Email**: [nicola.beccaceci@gmail.com](mailto:nicola.beccaceci@gmail.com)
- **GitHub**: [@Beccaceci](https://github.com/Beccaceci)
- **Repository**: [https://github.com/Beccaceci/georuggine](https://github.com/Beccaceci/georuggine)

---

## 📄 License

This project is open-source software licensed under the [MIT License](LICENSE).
