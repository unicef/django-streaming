# Console Backend

The Console Backend is a simple backend that prints all published messages to the console. It is primarily useful for development and debugging purposes.

## Configuration

To use the Console Backend, set your `BROKER_URL` in `settings.py` as follows:

```python
STREAMING = {
    "BROKER_URL": "console://"
}
```

## Usage

Messages published using the `manager.publish()` method will be printed to the standard output.

```python
from streaming.manager import manager

manager.publish("Hello from Console Backend!")
# Output: Hello from Console Backend!
```
