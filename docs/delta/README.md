

# DeltaHandle and DbHandle

The `TableConfigurator` contains logic to distinguish between production and 
debug tables. To make full use of this functionality when reading and writing 
delta tables, the two classes `DeltaHandle` and `DbHandle` have 
been provided. Use the classes like this

```python
from atc.config_master import TableConfigurator
from atc.delta import DeltaHandle, DbHandle

tc = TableConfigurator()
tc.register("MyDb",
            dict(
                name= "TestDb{ID}",
                path= "/tmp/testdb{ID}"
            )
) 
tc.register("MyTblId",
            dict(
                name= "TestDb{ID}.testTbl",
                path= "/tmp/testdb{ID}/testTbl"
            )
) 

# name is mandatory,
# path is optional
# format is optional. Must equal 'db' if provided
db = DbHandle.from_tc('MyDb')

# quickly create the database
db.create()

# name is mandatory,
# path is optional
# format is optional. Must equal 'delta' if provided
dh = DeltaHandle.from_tc('MyTblId')

# quickly create table without schema
# this only works if data already exists in storage
dh.create_hive_table()
df = dh.read()
dh.overwrite(df)
```

The `{ID}` parts are either replaced with an empty string (production) or with a uuid
if `tc.reset(debug=True)` has been set.

## DeltaHandle Upsert

The method upserts (updates or inserts) a databricks dataframe into a target delta table. 

``` python
def upsert(
        self,
        df: DataFrame,
        join_cols: List[str],
    ) -> Union[DataFrame, None]:   
    ...
```
Usage example: 
``` python
target_dh.upsert(df_new, ["Id"])

```

### Example

The following queries create a test table with two rows containing guitar data. 
Let's assume that the TableConfigurator is configured as in the section 
[DeltaHandle and DbHandle](#deltaHandle-and-dbhandle), but the testTbl has the following
schema:

``` python
%sql
(
  Id STRING,
  Brand STRING,
  Model STRING
)

INSERT INTO TestDb.testTbl values ("2","Gibson","Les Paul");

select * from TestDb.testTbl
+----+-----+----+-----------+
|Id  |    Brand |      Model|
+----+-----+----+-----------+
|   2|    Gibson|   Les Paul|
+----+----------+-----------+
```
The following dataframe has one row that will be merged with Id=2, and the other rows 
are going to be inserted:
``` python 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
df_new=spark.createDataFrame(
        spark.sparkContext.parallelize([
            ("1", "Fender", "Jaguar"),
            ("2", "Gibson","Starfire"),
            ("3", "Ibanez", "RG")
        ]),
        StructType([
            StructField("Id", StringType(), False),
            StructField("Brand", StringType(), True),
          StructField("Model", StringType(), True),
        ]))

```
Use the upsert method to upsert data into the test delta table:
``` python 
target_dh = DeltaHandle.from_tc("MyTblId")

target_dh.upsert(df_new, ["Id"])

%sql
select * from test.testTarget order by Id

+----+-----+----+-----------+
|Id  |    Brand |      Model|
+----+-----+----+-----------+
|   1|    Fender|     Jaguar|
|   2|    Gibson|   Starfire|
|   3|    Ibanez|         RG|
+----+----------+-----------+
```
As one can see, the row with id=2 is now upserted such that the model went from 
"Les Paul" to "Starfire". The two other rows where inserted. 

# DeltaHandle Schema Ownership

In some cases it may be desirable to control the schema of the delta table together 
with name, path and partitioning as part of the delta handle. This is now supported.

Complete example:
```python
from atc.config_master import TableConfigurator
from atc.delta import DeltaHandle, DbHandle
from atc.spark import Spark

tc = TableConfigurator()
tc.register("MyDb",
            dict(
                name= "TestDb",
                path= "/tmp/testdb",
                format="db"
            )
) 
db = DbHandle.from_tc('MyDb')
db.create()
Spark.get().sql("""
CREATE TABLE TestDb.testTbl
(
  Id STRING,
  Brand STRING,
  Model STRING
)
USING DELTA
TBLPROPERTIES (delta.autoOptimize.autoCompact = true)
PARTITION BY (Id)
LOCATION "/tmp/testdb/testTbl";
""")

tc.register("MyTblId",
            dict(
                name= "TestDb.testTbl",
                path= "/tmp/testdb/testTbl"
            )
)
dh1 = DeltaHandle.from_tc("MyTblId")
assert dh1.get_partitioning()==["Id"]


tc.register("MyTblId2",
            dict(
                name= "TestDb.testTbl2",
                path= "/tmp/testdb/testTbl2",
                schema= """
                (
                  Id STRING,
                  Brand STRING,
                  Model STRING
                )
                """,
                partitioning=['Id'],
                tblproperties={"delta.autoOptimize.autoCompact":"true"}
            )
)
dh2 = DeltaHandle.from_tc("MyTblId2")
dh2.create_hive_table()
df = dh2.read()
assert df.schema == dh1.read().schema
```



# Footnotes
- For a complete guide to how the TableConfigurator can be configured, see elsewhere.
  In these examples, we always configure it in code.
