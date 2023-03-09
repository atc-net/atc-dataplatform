
# Streaming

# Example 1: streaming
```python

from atc.etl import Orchestrator
from atc.etl.extractors.stream_extractor import StreamExtractor
from atc.etl.loaders.stream_loader import StreamLoader
from atc.tables import TableHandle

Orchestrator().extract_from(StreamExtractor(handle=myhandle)).load_into(
    StreamLoader(handle=TableHandle())
)





```


# Example 2: streaming with autoload

```python

from atc.etl import Orchestrator
from atc.etl.extractors.stream_extractor import StreamExtractor
from atc.etl.loaders.stream_loader import StreamLoader
from atc.tables import TableHandle
from atc.autoloader.AutoloaderHandle

Orchestrator().extract_from(StreamExtractor(handle=AutoloaderHandle())).load_into(
    StreamLoader(handle=TableHandle())
)

