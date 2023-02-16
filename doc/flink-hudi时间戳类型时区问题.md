## 问题描述
在Flink写Hudi中，对于带有时间戳timestamp类型的record，Flink不会对timestamp类型字段进行时区转换，   
结果就是不管什么时区的timestamp，都会被当成UTC时区的时间戳，最终写入parquet文件中。   
> parquet文件格式存储时间戳类型，要求统一是UTC时区，各个跑在各个地方的各种引擎读取parquet文件时，需要根据当地时区
> 读出后进行转换，以在各个地方显示正确的当地的时间。
因此这属于时Flink的问题！！！


## 但是，我为什么不会去修改这个问题？
- 1、flink sql从别的数据源读取数据，并写到hudi中，需要进行数据类型的转换。
- 2、`org.apache.flink.table.data.RowData`是Flink中描述一条记录的通用类型，数据在Flink DAG任务间流转，
都以这种类型表示。
- 3、但是数据写入hudi要是`org.apache.hudi.common.model.HoodieRecord`类型，因此写入hudi前需要将RowData转换为
  `HoodieRecord`类型。
- 4、因此我们要修正为正确的时区，需要在第三步的转换中去做。

### xx
RowData转换为`HoodieRecord`类型,集中在`org.apache.hudi.sink.transform.RowDataToHoodieFunction#toHoodieRecord`方法中。

- 1、Flink DataStream DAG构建代码。
org.apache.hudi.sink.utils.Pipelines#rowDataToHoodieRecord
```java
  /**
   * Transforms the row data to hoodie records.
   */
  public static DataStream<HoodieRecord> rowDataToHoodieRecord(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    return dataStream.map(RowDataToHoodieFunctions.create(rowType, conf), TypeInformation.of(HoodieRecord.class))
        .setParallelism(dataStream.getParallelism()).name("row_data_to_hoodie_record");
  }
```

- `org.apache.hudi.sink.transform.RowDataToHoodieFunction#toHoodieRecord`
```java
  /**
   * Converts the give record to a {@link HoodieRecord}.
   *
   * @param record The input record
   * @return HoodieRecord based on the configuration
   * @throws IOException if error occurs
   */
  @SuppressWarnings("rawtypes")
  private HoodieRecord toHoodieRecord(I record) throws Exception {
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
    final HoodieKey hoodieKey = keyGenerator.getKey(gr);

    HoodieRecordPayload payload = payloadCreation.createPayload(gr);
    HoodieOperation operation = HoodieOperation.fromValue(record.getRowKind().toByteValue());
    return new HoodieAvroRecord<>(hoodieKey, payload, operation);
  }
```
该方法重要的一行是`GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);`,
实际进行转换的converter类是`org.apache.hudi.util.RowDataToAvroConverters#createRowConverter`类.

- 根据给定的逻辑类型创建运行时转换器，将 Flink Table & SQL 内部数据结构的对象转换为相应的 Avro 数据结构.
```java
  /**
   * Creates a runtime converter according to the given logical type that converts objects of
   * Flink Table & SQL internal data structures to corresponding Avro data structures.
   */
  public static RowDataToAvroConverter createConverter(LogicalType type) {
    final RowDataToAvroConverter converter;
    switch (type.getTypeRoot()) {
      case NULL:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(Schema schema, Object object) {
                return null;
              }
            };
        break;
      case TINYINT:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(Schema schema, Object object) {
                return ((Byte) object).intValue();
              }
            };
        break;
      case SMALLINT:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(Schema schema, Object object) {
                return ((Short) object).intValue();
              }
            };
        break;
      case BOOLEAN: // boolean
      case INTEGER: // int
      case INTERVAL_YEAR_MONTH: // long
      case BIGINT: // long
      case INTERVAL_DAY_TIME: // long
      case FLOAT: // float
      case DOUBLE: // double
      case TIME_WITHOUT_TIME_ZONE: // int
      case DATE: // int
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(Schema schema, Object object) {
                return object;
              }
            };
        break;
      case CHAR:
      case VARCHAR:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(Schema schema, Object object) {
                return new Utf8(object.toString());
              }
            };
        break;
      case BINARY:
      case VARBINARY:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(Schema schema, Object object) {
                return ByteBuffer.wrap((byte[]) object);
              }
            };
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        final int precision = DataTypeUtils.precision(type);
        if (precision <= 3) {
          converter =
              new RowDataToAvroConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object convert(Schema schema, Object object) {
                  return ((TimestampData) object).toInstant().toEpochMilli();
                }
              };
        } else if (precision <= 6) {
          converter =
              new RowDataToAvroConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object convert(Schema schema, Object object) {
                  return ChronoUnit.MICROS.between(Instant.EPOCH, ((TimestampData) object).toInstant());
                }
              };
        } else {
          throw new UnsupportedOperationException("Unsupported timestamp precision: " + precision);
        }
        break;
      case DECIMAL:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(Schema schema, Object object) {
                BigDecimal javaDecimal = ((DecimalData) object).toBigDecimal();
                return decimalConversion.toFixed(javaDecimal, schema, schema.getLogicalType());
              }
            };
        break;
      case ARRAY:
        converter = createArrayConverter((ArrayType) type);
        break;
      case ROW:
        converter = createRowConverter((RowType) type);
        break;
      case MAP:
      case MULTISET:
        converter = createMapConverter(type);
        break;
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    // wrap into nullable converter
    return new RowDataToAvroConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(Schema schema, Object object) {
        if (object == null) {
          return null;
        }

        // get actual schema if it is a nullable schema
        Schema actualSchema;
        if (schema.getType() == Schema.Type.UNION) {
          List<Schema> types = schema.getTypes();
          int size = types.size();
          if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
            actualSchema = types.get(0);
          } else if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
            actualSchema = types.get(1);
          } else {
            throw new IllegalArgumentException(
                "The Avro schema is not a nullable type: " + schema);
          }
        } else {
          actualSchema = schema;
        }
        return converter.convert(actualSchema, object);
      }
    };
  }
```