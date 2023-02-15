## Hudi conf example

## hudi开启下列选项，提高性能
clustering enable
Metadata Table enable
clean
compaction

- https://mp.weixin.qq.com/s/fi80UhvTLsiXL32IXm5SvA
```scala
spark.sql(
  """
    |create table prestoc(
    |c1 int,
    |c11 int,
    |c12 int,
    |c2 string,
    |c3 decimal(38, 10),
    |c4 timestamp,
    |c5 int,
    |c6 date,
    |c7 binary,
    |c8 int
    |) using hudi
    |tblproperties (
    |primaryKey = 'c1',
    |preCombineField = 'c11',
    |hoodie.upsert.shuffle.parallelism = 8,
    |hoodie.table.keygenerator.class = 'org.apache.hudi.keygen.SimpleKeyGenerator',
    |hoodie.metadata.enable = "true",
    |hoodie.metadata.index.column.stats.enable = "true",
    |hoodie.metadata.index.column.stats.file.group.count = "2",
    |hoodie.metadata.index.column.stats.column.list = 'c1,c2',
    |hoodie.metadata.index.bloom.filter.enable = "true",
    |hoodie.metadata.index.bloom.filter.column.list = 'c1',
    |hoodie.enable.data.skipping = "true",
    |hoodie.cleaner.policy.failed.writes = "LAZY",
    |hoodie.clean.automatic = "false",
    |hoodie.metadata.compact.max.delta.commits = "1"
    |)
    |
    |""".stripMargin)
```