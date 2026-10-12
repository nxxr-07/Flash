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

The server and client examples emit structured logs through `tracing`. Set `RUST_LOG`
to choose the verbosity, for example:

```bash
RUST_LOG=debug cargo run -p flash-server
RUST_LOG=flash_client=debug cargo run -p flash-client --example consumer
```
