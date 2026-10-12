# Flash Quick Start

## Running the System

### 1. Start Server (Terminal 1)
```bash
cd d:/Rust/Flash
cargo run -p flash-server
```

### 2. Start Consumer (Terminal 2)
```bash
cd d:/Rust/Flash
cargo run -p flash-client --example consumer
```

### 3. Start Interactive Producer (Terminal 3)
```bash
cd d:/Rust/Flash
cargo run -p flash-client --example producer
```

Both examples use the `notifications` topic by default. Pass a topic name as the
first argument to select another topic:

```bash
# Terminal 2: listen to orders
cargo run -p flash-client --example consumer -- orders

# Terminal 3: publish to orders
cargo run -p flash-client --example producer -- orders
```

Run a consumer and producer with the same topic to exchange messages. Different
topic names are isolated from one another. Multiple consumers can subscribe to the
same topic and each will receive published messages.

## Using the Interactive Producer

Once connected, you'll see:
```
Connected to Flash server
Type messages to publish (Ctrl+C to exit):
> 
```

**Type any message and press Enter:**
```
> Hello from Rust!
✓ Published: Hello from Rust!
> This is my second message
✓ Published: This is my second message
```

**All consumers will receive these messages in real-time!**

## Tips

- Run **multiple consumers** to see fan-out broadcasting
- Send **empty lines** to skip (won't publish)
- Press **Ctrl+C** to exit
- Messages appear instantly in all subscribed consumers

## Logging

The server and client examples emit structured logs through `tracing`. By default,
they are written to stdout. Set `RUST_LOG` to choose the verbosity, for example:

```bash
RUST_LOG=debug cargo run -p flash-server
RUST_LOG=flash_client=debug cargo run -p flash-client --example consumer
```

To export logs directly to New Relic, set `NEW_RELIC_LICENSE_KEY` to your New Relic
license key. The default US OTLP endpoint is `https://otlp.nr-data.net/v1/logs`.
For another New Relic region, set `NEW_RELIC_OTLP_ENDPOINT` to that region's OTLP
logs endpoint (for example, `https://otlp.eu01.nr-data.net/v1/logs`). The key is
sent as the OTLP `api-key` header and is never included in log fields. Without a
license key, the applications continue logging locally without remote export.

For local development, copy `.env.example` to `.env` and add your license key there.
The `.env` file is loaded automatically and ignored by Git:

```bash
cp .env.example .env
# Edit .env and replace the license-key placeholder.
cargo run -p flash-server
```

Example:

```bash
NEW_RELIC_LICENSE_KEY=your-license-key \
RUST_LOG=info \
cargo run -p flash-server
```

The exporter includes service names (`flash-server`, `flash-client-producer`, and
`flash-client-consumer`) and tracing event fields such as connection address, topic,
and payload length. In New Relic, inspect the log event attributes for `topic` to
filter by topic. Avoid logging message payload contents or credentials.
