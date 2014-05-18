/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.columnar.VecRowBlock;
import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.GlobalMetaData;
import parquet.io.api.Binary;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class VecRowParquetReader implements Closeable {

  private ReadSupport<Object []> readSupport;
  private UnboundRecordFilter filter;
  private Configuration conf;
  private ReadSupport.ReadContext readContext;
  private Iterator<Footer> footersIterator;
  private VecRowDirectReader reader;
  private GlobalMetaData globalMetaData;

  public VecRowParquetReader(Path file, Schema schema, Schema target) throws IOException {
    this(file, new VecRowReadSupport(schema, target));
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws java.io.IOException
   */
  public VecRowParquetReader(Path file, VecRowReadSupport readSupport) throws IOException {
    this(file, readSupport, null);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   */
  public VecRowParquetReader(Configuration conf, Path file, VecRowReadSupport readSupport) throws IOException {
    this(conf, file, readSupport, null);
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @param filter the filter to use to filter records
   * @throws IOException
   */
  private VecRowParquetReader(Path file, VecRowReadSupport readSupport, UnboundRecordFilter filter)
      throws IOException {
    this(new Configuration(), file, readSupport, filter);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @param filter the filter to use to filter records
   * @throws IOException
   */
  public VecRowParquetReader(Configuration conf, Path file, VecRowReadSupport readSupport,
                             UnboundRecordFilter filter) throws IOException {
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

    this.tajoSchema = readSupport.getSchema();
    this.columnNum = tajoSchema.size();
    tajoTypes = new TajoDataTypes.Type[columnNum];
    for (int i = 0; i < columnNum; i++) {
      tajoTypes[i] = tajoSchema.getColumn(i).getDataType().getType();
    }
  }

  Schema tajoSchema;
  int columnNum;
  TajoDataTypes.Type [] tajoTypes;

  /**
   * @return the next record or null if finished
   * @throws IOException
   */
  public boolean nextFetch(VecRowBlock vecRowBlock) throws IOException {
    try {
      if (reader != null) {
        int rowIdx = 0;
        while(rowIdx < vecRowBlock.maxVecSize() && reader.nextKeyValue()) {
          Object [] values = reader.getCurrentValue();
          for (int columnIdx = 0; columnIdx < columnNum; columnIdx++) {
            if (values[columnIdx] != null) {
              switch (tajoTypes[columnIdx]) {
              case BOOLEAN: vecRowBlock.putBool(columnIdx, rowIdx, (Boolean) values[columnIdx]); break;
              case INT1:
              case INT2: vecRowBlock.putInt2(columnIdx, rowIdx, (Short) values[columnIdx]); break;
              case INT4: vecRowBlock.putInt4(columnIdx, rowIdx, (Integer) values[columnIdx]); break;
              case INT8: vecRowBlock.putInt8(columnIdx, rowIdx, (Long) values[columnIdx]); break;
              case FLOAT4: vecRowBlock.putFloat4(columnIdx, rowIdx, (Float) values[columnIdx]); break;
              case FLOAT8: vecRowBlock.putFloat8(columnIdx, rowIdx, (Double) values[columnIdx]); break;
              case TEXT: vecRowBlock.putText(columnIdx, rowIdx, ((Binary) values[columnIdx]).getBytes()); break;
              case BLOB: vecRowBlock.putText(columnIdx, rowIdx, ((Binary) values[columnIdx]).getBytes()); break;
              default:
                throw new IOException("Not supported type: " + tajoTypes[columnIdx].name());
              }
            }
          }
          rowIdx++;
        }
        vecRowBlock.setLimitedVecSize(rowIdx);

        return rowIdx > 0;
      } else {
        initReader();
        return reader == null ? null : nextFetch(vecRowBlock);
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
      reader = new VecRowDirectReader(readSupport, filter);
      reader.initialize(
          readContext.getRequestedSchema(), globalMetaData.getSchema(), footer.getParquetMetadata().getFileMetaData()
              .getKeyValueMetaData(),
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
