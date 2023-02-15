## Q

### Flink默认的state-based index如果状态丢失会怎样，是否会导致后续的数据重复
- 在Flink-hudi中默认是全局索引
```java
  public static final ConfigOption<Boolean> INDEX_GLOBAL_ENABLED = ConfigOptions
      .key("index.global.enabled")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to update index for the old partition path\n"
          + "if same key record with different partition path came in, default true");
```
一个可行的想法是：
Flink state index + state ttl + no global index
大于5亿行 bucket index, 需要考量bucket的数量，有最佳实践：
bucket index + no global index

```java
  public static final ConfigOption<Double> INDEX_STATE_TTL = ConfigOptions
      .key("index.state.ttl")
      .doubleType()
      .defaultValue(0D)
      .withDescription("Index state ttl in days, default stores the index permanently");
```

- state-based index索引数据丢失怎么办？
当state-based index数据丢失后，是不是需要设置此参数重建索引。：也是，丢失后类似全量接增量
```java
  public static final ConfigOption<Boolean> INDEX_BOOTSTRAP_ENABLED = ConfigOptions
      .key("index.bootstrap.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to bootstrap the index state from existing hoodie table, default false");
```

### hoodie.datasource.hive_sync.create_managed_table配置为hive内部表
- 说是0.12.0后才有这个参数

