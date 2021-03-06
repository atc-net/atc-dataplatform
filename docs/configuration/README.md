
# Table Configurator

## Motivation
In databricks there is one standard way to refer to a table, the 
"database.table" reference. In our dataplatform projects we have often 
found a need for a more abstracted notion of a table reference,
one that allows for injection of test versions of tables and that allows 
unified reference across different environments (dev, staging, prod).
The `TableConfigurator` provides such a further abstraction level.

## Setting up configuration

The `TableConfigurator` is a singleton class that you need to instantiate
and configure once in your project. Make sure this code is always executed
before any reference to tables in made. Example:
```python
from atc.config_master import TableConfigurator
tc = TableConfigurator()
tc.set_extra(ENV='prod')
tc.add_resource_path(my.resource.module)
```
The `TableConfigurator` can be configured with json or yaml files. The
files must contain the following structure:
- top level objects are resources
- each resource must have one of three shapes:
  - a single key named 'alias' to refer to another resource
  - two keys called 'release' and 'debug', each containing resource details
  - resource details consisting of a 'name' attribute
    - optionally also with a 'path' attribute,
    - optionally also with a 'format' and 'partitioning' attribute.
You can see examples for all three cases in the unit-tests.

You optionally either provide 'release' and 'debug' versions of each table
or include the structure `{ID}` in the name and path. This is a special
replacement string that will be replaced with and empty string for 
production tables, or with a "__{GUID}" string when debug is selected.
The guid construction allows for non-colliding parallel testing.

Beyond the resource definitions, the `TableConfigurator` needs to be 
configured to return production or test versions of tables this is done
at the start of your code. In your jobs you need to set `TableConfigurator().set_prod()`
whereas your unit-tests should call `TableConfigurator().set_debug()`.

## Using the TableConfigurator

Once configured, the table configurator is often not used directly.
Other classes in the atc framework use it when configured with a resource
ID. You can find examples in the eventhub unittests:
```python
from atc.eh import EventHubCapture
EventHubCapture.from_tc("AtcEh")
```
or in the delta handle unit-tests:
```python
from atc.delta import DeltaHandle
DeltaHandle.from_tc("MyTbl")
```
But sometimes you still need to call the table configurator methods
e.g. when constructing your own sql:
```python
from atc.config_master import TableConfigurator
f"MERGE INTO {TableConfigurator().table_name('MyTbl')} AS target ..."
```

## Further Features

### MNT key
'MNT' is another special replacement, similar to "ID". In production it
is replaced with the string 'mnt' while in debug it is replace with 'tmp'.
The intended usage is in paths where production tables are mounted on
external storage, typically mounted under "/mnt" whereas test tables 
should be written to "/tmp" you can use is as in this example:
```yaml
MyTable:
  name: mydb{ID}.data_table
  path: /{MNT}/somestorage{ENV}/mydb{ID}/data_table
```

### Cross References
In some cases it is useful to refer to other defined resources and their properties.
This is fully supported as shown here:
```yaml
MyDb:
  name: mydb{ID}
  path: /{MNT}/storage{ENV}/mydb{ID}

MyTable:
  name: {MyDb}.mytable
  path: {MyDb_path}/mytable
```
As shown here the "name" property can be accessed with the bare resouce key
whereas other properties can be accessed by appending the property with an 
underscore. (Optionally, appending "_name" is also supported to access the
name, i.e. `{MyDb}` and `{MyDb_name}` are equivalent.)

### Extra details
As already shown in the example above, there is a method to add
further extra details such as an 'ENV' key. Note: The ENV key is not 
a built-in special property. Use the `set_extra` method:
```python
from atc.config_master import TableConfigurator
tc = TableConfigurator()
tc.set_extra(ENV='prod')
```
