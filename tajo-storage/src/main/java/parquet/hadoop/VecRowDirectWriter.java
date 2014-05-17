package parquet.hadoop;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.newtuple.VecRowBlock;
import parquet.Log;
import parquet.column.ParquetProperties;
import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.io.IOException;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static parquet.Log.DEBUG;

class VecRowDirectWriter {
  private static final Log LOG = Log.getLog(VecRowDirectWriter.class);

  private static final int MINIMUM_BUFFER_SIZE = 64 * 1024;
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  private final ParquetFileWriter w;
  private final MessageType schema;
  private final Map<String, String> extraMetaData;
  private final int blockSize;
  private final int pageSize;
  private final CodecFactory.BytesCompressor compressor;
  private final int dictionaryPageSize;
  private final boolean enableDictionary;
  private final boolean validating;
  private final ParquetProperties.WriterVersion writerVersion;

  RecordConsumer recordConsumer;

  private long recordCount = 0;
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

  private ColumnWriteStoreImpl store;
  private ColumnChunkPageWriteStore pageStore;

  private int columnNum;
  private Type[] fieldTypes;
  private TajoDataTypes.Type [] tajoDataTypes;

  public VecRowDirectWriter(
      ParquetFileWriter w,
      MessageType schema,
      Map<String, String> extraMetaData,
      int blockSize,
      int pageSize,
      CodecFactory.BytesCompressor compressor,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      ParquetProperties.WriterVersion writerVersion,
      Schema tajoSchema) {
    this.w = w;
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.blockSize = blockSize;
    this.pageSize = pageSize;
    this.compressor = compressor;
    this.dictionaryPageSize = dictionaryPageSize;
    this.enableDictionary = enableDictionary;
    this.validating = validating;
    this.writerVersion = writerVersion;

    this.columnNum = tajoSchema.size();
    fieldTypes = new Type[schema.getFieldCount()];
    for (int i = 0; i < schema.getFieldCount(); i++) {
      fieldTypes[i] = schema.getType(i);
    }
    tajoDataTypes = new TajoDataTypes.Type[tajoSchema.size()];
    for (int i = 0; i < tajoSchema.size(); i++) {
      tajoDataTypes[i] = tajoSchema.getColumn(i).getDataType().getType();
    }

    initStore();
  }

  private void initStore() {
    // we don't want this number to be too small
    // ideally we divide the block equally across the columns
    // it is unlikely all columns are going to be the same size.
    int initialBlockBufferSize = max(MINIMUM_BUFFER_SIZE, blockSize / schema.getColumns().size() / 5);
    pageStore = new ColumnChunkPageWriteStore(compressor, schema, initialBlockBufferSize);
    // we don't want this number to be too small either
    // ideally, slightly bigger than the page size, but not bigger than the block buffer
    int initialPageBufferSize = max(MINIMUM_BUFFER_SIZE, min(pageSize + pageSize / 10, initialBlockBufferSize));
    store = new ColumnWriteStoreImpl(pageStore, pageSize, initialPageBufferSize, dictionaryPageSize, enableDictionary, writerVersion);
    MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
    recordConsumer = columnIO.getRecordWriter(store);
  }

  public void close() throws IOException, InterruptedException {
    flushStore();
    w.end(extraMetaData);
  }

  public byte [] buffers = new byte[Short.MAX_VALUE];

  public void write(VecRowBlock vecRowBlock) throws IOException, InterruptedException {


    for (int rowIdx = 0; rowIdx < vecRowBlock.limitedVecSize(); rowIdx++) {
      recordConsumer.startMessage();

      for (int columnIdx = 0; columnIdx < columnNum; ++columnIdx) {
        recordConsumer.startField(fieldTypes[columnIdx].getName(), columnIdx);
        switch (tajoDataTypes[columnIdx]) {
        case BOOLEAN:
          recordConsumer.addBoolean(vecRowBlock.getBool(columnIdx, rowIdx) == 1);
          break;
        case BIT:
        case INT2:
          recordConsumer.addInteger(vecRowBlock.getInt2(columnIdx, rowIdx));
          break;
        case INT4:
          recordConsumer.addInteger(vecRowBlock.getInt4(columnIdx, rowIdx));
          break;
        case INT8:
          recordConsumer.addLong(vecRowBlock.getInt8(columnIdx, rowIdx));
          break;
        case FLOAT4:
          recordConsumer.addFloat(vecRowBlock.getFloat4(columnIdx, rowIdx));
          break;
        case FLOAT8:
          recordConsumer.addDouble(vecRowBlock.getFloat8(columnIdx, rowIdx));
          break;
        case CHAR:
        case TEXT:
          recordConsumer.addBinary(Binary.fromByteArray(vecRowBlock.getBytes(columnIdx, rowIdx)));
          break;
        case PROTOBUF:
        case BLOB:
        case INET4:
        case INET6:
          recordConsumer.addBinary(Binary.fromByteArray(vecRowBlock.getBytes(columnIdx, rowIdx)));
          break;
        default:
          break;
        }

        recordConsumer.endField(fieldTypes[columnIdx].getName(), columnIdx);
      }
      recordConsumer.endMessage();
      ++ recordCount;
      checkBlockSizeReached();
    }
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = store.memSize();
      if (memSize > blockSize) {
        LOG.info(format("mem size %,d > %,d: flushing %,d records to disk.", memSize, blockSize, recordCount));
        flushStore();
        initStore();
        recordCountForNextMemCheck = min(max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCount / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK);
      } else {
        float recordSize = (float) memSize / recordCount;
        recordCountForNextMemCheck = min(
            max(MINIMUM_RECORD_COUNT_FOR_CHECK, (recordCount + (long)(blockSize / recordSize)) / 2), // will check halfway
            recordCount + MAXIMUM_RECORD_COUNT_FOR_CHECK // will not look more than max records ahead
        );
        if (DEBUG) LOG.debug(format("Checked mem at %,d will check again at: %,d ", recordCount, recordCountForNextMemCheck));
      }
    }
  }

  private void flushStore()
      throws IOException {
    LOG.info(format("Flushing mem store to file. allocated memory: %,d", store.allocatedSize()));
    if (store.allocatedSize() > 3 * blockSize) {
      LOG.warn("Too much memory used: " + store.memUsageString());
    }
    w.startBlock(recordCount);
    store.flush();
    pageStore.flushToFileWriter(w);
    recordCount = 0;
    w.endBlock();
    store = null;
    pageStore = null;
  }
}
