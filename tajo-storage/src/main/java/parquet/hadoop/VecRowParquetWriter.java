package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.newtuple.VecRowBlock;
import org.apache.tajo.storage.parquet.TajoSchemaConverter;
import parquet.column.ParquetProperties;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class VecRowParquetWriter implements Closeable {
    public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
    public static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;
    public static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME = CompressionCodecName.UNCOMPRESSED;
    public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
    public static final boolean DEFAULT_IS_VALIDATING_ENABLED = false;
    public static final ParquetProperties.WriterVersion DEFAULT_WRITER_VERSION =
        ParquetProperties.WriterVersion.PARQUET_1_0;

    private final VecRowDirectWriter writer;

    /**
     * Create a new ParquetWriter.
     * (with dictionary encoding enabled and validation off)
     *
     * @param file the file to create
     * @param tajoSchema
     * @param extraDataMeta
     * @param compressionCodecName the compression codec to use
     * @param blockSize the block size threshold
     * @param pageSize the page size threshold
     * @throws java.io.IOException
     */
    public VecRowParquetWriter(Path file, Schema tajoSchema,Map<String, String> extraDataMeta,
                               CompressionCodecName compressionCodecName, int blockSize, int pageSize) throws IOException {
      this(file, tajoSchema, extraDataMeta, compressionCodecName, blockSize, pageSize,
          DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED);
    }

    /**
     * Create a new ParquetWriter.
     *
     * @param file the file to create
     * @param tajoSchema
     * @param extraDataMeta
     * @param compressionCodecName the compression codec to use
     * @param blockSize the block size threshold
     * @param pageSize the page size threshold (both data and dictionary)
     * @param enableDictionary to turn dictionary encoding on
     * @param validating to turn on validation using the schema
     * @throws IOException
     */
    public VecRowParquetWriter(
        Path file,
        Schema tajoSchema,
        Map<String, String> extraDataMeta,
        CompressionCodecName compressionCodecName,
        int blockSize,
        int pageSize,
        boolean enableDictionary,
        boolean validating) throws IOException {
      this(file, tajoSchema, extraDataMeta, compressionCodecName, blockSize, pageSize, pageSize, enableDictionary, validating);
    }

    /**
     * Create a new ParquetWriter.
     *
     * @param file the file to create
     * @param tajoSchema
     * @param extraDataMeta
     * @param compressionCodecName the compression codec to use
     * @param blockSize the block size threshold
     * @param pageSize the page size threshold
     * @param dictionaryPageSize the page size threshold for the dictionary pages
     * @param enableDictionary to turn dictionary encoding on
     * @param validating to turn on validation using the schema
     * @throws IOException
     */
    public VecRowParquetWriter(
        Path file,
        Schema tajoSchema,
        Map<String, String> extraDataMeta,
        CompressionCodecName compressionCodecName,
        int blockSize,
        int pageSize,
        int dictionaryPageSize,
        boolean enableDictionary,
        boolean validating) throws IOException {
      this(file, tajoSchema, extraDataMeta, compressionCodecName, blockSize, pageSize,
          dictionaryPageSize, enableDictionary, validating,
          DEFAULT_WRITER_VERSION);
    }

    /**
     * Create a new ParquetWriter.
     *
     * Directly instantiates a Hadoop {@link org.apache.hadoop.conf.Configuration} which reads
     * configuration from the classpath.
     *
     * @param file the file to create
     * @param tajoSchema
     * @param extraDataMeta
     * @param compressionCodecName the compression codec to use
     * @param blockSize the block size threshold
     * @param pageSize the page size threshold
     * @param dictionaryPageSize the page size threshold for the dictionary pages
     * @param enableDictionary to turn dictionary encoding on
     * @param validating to turn on validation using the schema
     * @param writerVersion version of parquetWriter from {@link ParquetProperties.WriterVersion}
     * @throws IOException
     */
    public VecRowParquetWriter(
        Path file,
        Schema tajoSchema,
        Map<String,String> extraDataMeta,
        CompressionCodecName compressionCodecName,
        int blockSize,
        int pageSize,
        int dictionaryPageSize,
        boolean enableDictionary,
        boolean validating,
        ParquetProperties.WriterVersion writerVersion) throws IOException {
      this(file, tajoSchema, extraDataMeta, compressionCodecName, blockSize, pageSize, dictionaryPageSize, enableDictionary, validating, writerVersion, new Configuration());
    }

    /**
     * Create a new ParquetWriter.
     *
     * @param file the file to create
     * @param tajoSchema
     * @param extraDataMeta
     * @param compressionCodecName the compression codec to use
     * @param blockSize the block size threshold
     * @param pageSize the page size threshold
     * @param dictionaryPageSize the page size threshold for the dictionary pages
     * @param enableDictionary to turn dictionary encoding on
     * @param validating to turn on validation using the schema
     * @param writerVersion version of parquetWriter from {@link ParquetProperties.WriterVersion}
     * @param conf Hadoop configuration to use while accessing the filesystem
     * @throws IOException
     */
    public VecRowParquetWriter(
        Path file,
        Schema tajoSchema,
        Map<String,String> extraDataMeta,
        CompressionCodecName compressionCodecName,
        int blockSize,
        int pageSize,
        int dictionaryPageSize,
        boolean enableDictionary,
        boolean validating,
        ParquetProperties.WriterVersion writerVersion,
        Configuration conf
        ) throws IOException {

      MessageType schema = new TajoSchemaConverter().convert(tajoSchema);

      ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, file);
      fileWriter.start();

      CodecFactory codecFactory = new CodecFactory(conf);
      CodecFactory.BytesCompressor compressor =	codecFactory.getCompressor(compressionCodecName, 0);
      this.writer = new VecRowDirectWriter(fileWriter,
          schema,
          extraDataMeta,
          blockSize,
          pageSize,
          compressor,
          dictionaryPageSize,
          enableDictionary,
          validating,
          writerVersion,
          tajoSchema);
    }

    public void write(VecRowBlock object) throws IOException {
      try {
        writer.write(object);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        writer.close();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
}
