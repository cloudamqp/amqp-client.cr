# Connection Identifier in Logs

## Summary
This change adds a unique identifier to all log lines from a connection, making it easier to distinguish between multiple AMQP clients in a single application.

## Implementation Details

### Connection Identifier
- For TCP and SSL/TLS connections: Uses the local port number (from `local_address.port`)
- For UNIX sockets and WebSocket connections: Uses the object ID

The local port is unique for each TCP connection on the same host, making it an excellent identifier that:
- Is easy to correlate with network debugging tools
- Is visible in server logs and management interfaces
- Doesn't require any additional state or configuration

### Code Changes

1. **Connection.cr**
   - Added `@connection_id : String` instance variable
   - Added `connection_identifier(io)` private method to extract the identifier
   - Set log context with `Log.context.set connection_id: @connection_id` in:
     - `read_loop` (main fiber that processes incoming frames)
     - `unsafe_write` (when sending frames)
     - `close` (when closing the connection)
     - `process_close` (when server closes the connection)
     - `process_channel_frame` (when processing channel frames)

2. **Test**
   - Added a test in `spec/amqp-client_spec.cr` to verify the connection identifier is set
   - Created `examples/connection_identifier_test.cr` to demonstrate the feature

### Log Context
Crystal's `Log.context` is fiber-local, which means each fiber maintains its own context. By setting the context in the main methods that perform logging, we ensure that:
- All log lines from the read_loop fiber include the connection_id
- All log lines from user code (write, close, etc.) include the connection_id
- The context is automatically included in the structured log output

### Example Log Output
Before:
```
DEBUG - recv Frame::Basic::Deliver(...)
DEBUG - sent Frame::Basic::Ack(...)
```

After:
```
DEBUG - recv Frame::Basic::Deliver(...) connection_id=54321
DEBUG - sent Frame::Basic::Ack(...) connection_id=54321
```

When using structured logging formatters, the connection_id will appear as a separate field in the log output.

## Testing
Run the example to see the connection identifier in action:
```
LOG_LEVEL=DEBUG crystal run examples/connection_identifier_test.cr
```

The test suite also includes a basic verification that the connection_id is properly set.
