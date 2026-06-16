# vast-daft

Daft custom connector (`DataSource` / `DataSink`) for [VastDB](https://vastdata.com).

## Installation

```bash
pip install vast-daft
```

Or with uv:

```bash
uv add vast-daft
```

## Quick Start

### Reading from VastDB

```python
import pyarrow as pa
from vast_daft import VastDBConfig, VastDBDataSource

config = VastDBConfig(
    endpoint="http://vastdb:9090",
    access_key="YOUR_ACCESS_KEY",
    secret_key="YOUR_SECRET_KEY",
    bucket="my-bucket",
    schema="my-schema",
)

schema = pa.schema([
    ("id", pa.string()),
    ("name", pa.string()),
    ("value", pa.float64()),
])

source = VastDBDataSource(config, "my_table", schema)
df = source.read()
df.show()
```

### Writing to VastDB

```python
import daft
from vast_daft import VastDBConfig, VastDBDataSink

config = VastDBConfig(...)
schema = pa.schema([("id", pa.string()), ("value", pa.float64())])

sink = VastDBDataSink(config, "my_table", schema)
daft.from_pydict({"id": ["a", "b"], "value": [1.0, 2.0]}).write_sink(sink).show()
```
