# Debug Backend

The Debug Backend is a specialized backend designed for testing and debugging. It stores all published messages in an in-memory list, which can then be inspected programmatically.

## Configuration

To use the Debug Backend, set your `BROKER_URL` in `settings.py` as follows:

```python
STREAMING = {
    "BROKER_URL": "debug://"
}
```

## Usage

Messages published to the Debug Backend can be retrieved from the `debug_backend.messages` list.

```python
from streaming.manager import manager
from streaming.backends.debug import Backend as DebugBackend

# Assuming the Debug Backend is configured and active
manager.publish("First debug message")
manager.publish("Second debug message")

debug_backend = manager.get_backend() # This assumes you have a way to get the active backend instance

if isinstance(debug_backend, DebugBackend):
    print(debug_backend.messages)
    # Output: ["First debug message", "Second debug message"]
```

**Note:** The Debug Backend is not intended for production use as it does not persist messages and is reset with each application restart.
