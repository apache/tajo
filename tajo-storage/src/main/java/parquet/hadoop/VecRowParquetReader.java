package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.storage.newtuple.VecRowBlock;
import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.GlobalMetaData;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public abstract class VecRowParquetReader implements Closeable {

  private ReadSupport<T> readSupport;
  private UnboundRecordFilter filter;
  private Configuration conf;
  private ReadSupport.ReadContext readContext;
  private Iterator<Footer> footersIterator;
  private VecRowParquetReader reader;
  private GlobalMetaData globalMetaData;

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws java.io.IOException
   */
  public VecRowParquetReader(Path file, ReadSupport<T> readSupport) throws IOException {
    this(file, readSupport, null);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   */
  public VecRowParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport) throws IOException {
    this(conf, file, readSupport, null);
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @param filter the filter to use to filter records
   * @throws IOException
   */
  public VecRowParquetReader(Path file, ReadSupport<T> readSupport, UnboundRecordFilter filter) throws IOException {
    this(new Configuration(), file, readSupport, filter);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @param filter the filter to use to filter records
   * @throws IOException
   */
  public VecRowParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport, UnboundRecordFilter filter) throws IOException {
    this.readSupport = readSupport;
    this.filter = filter;
    this.conf = conf;

    FileSystem fs = file.getFileSystem(conf);
    List<FileStatus> statuses = Arrays.asList(fs.listStatus(file));
    List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses);
    this.footersIterator = footers.iterator();
    globalMetaData = ParquetFileWriter.getGlobalMetaData(footers);

    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
    for (Footer footer : footers) {
      blocks.addAll(footer.getParquetMetadata().getBlocks());
    }

    MessageType schema = globalMetaData.getSchema();
    Map<String, Set<String>> extraMetadata = globalMetaData.getKeyValueMetaData();
    readContext = readSupport.init(new InitContext(conf, extraMetadata, schema));
  }

  /**
   * @return the next record or null if finished
   * @throws IOException
   */
  public VecRowBlock read() throws IOException {
    try {
      if (reader != null && reader.nextKeyValue()) {
        return reader.getCurrentValue();
      } else {
        initReader();
        return reader == null ? null : read();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void initReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (footersIterator.hasNext()) {
      Footer footer = footersIterator.next();
      reader = new InternalParquetRecordReader<T>(readSupport, filter);
      reader.initialize(
          readContext.getRequestedSchema(), globalMetaData.getSchema(), footer.getParquetMetadata().getFileMetaData().getKeyValueMetaData(),
          readContext.getReadSupportMetadata(), footer.getFile(), footer.getParquetMetadata().getBlocks(), conf);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }
}
