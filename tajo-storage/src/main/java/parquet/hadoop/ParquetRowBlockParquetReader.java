/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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
import org.apache.tajo.tuple.offheap.OffHeapRowBlock;
import org.apache.tajo.tuple.offheap.RowWriter;
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

public class ParquetRowBlockParquetReader implements Closeable {

  private ReadSupport<Object []> readSupport;
  private UnboundRecordFilter filter;
  private Configuration conf;
  private ReadSupport.ReadContext readContext;
  private Iterator<Footer> footersIterator;
  private ParquetRowDirectReader reader;
  private GlobalMetaData globalMetaData;
  private final int [] projectedMap;

  public ParquetRowBlockParquetReader(Path file, Schema schema, Schema target) throws IOException {
    this(file, new ParquetRowReadSupport(schema, target));
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws java.io.IOException
   */
  public ParquetRowBlockParquetReader(Path file, ParquetRowReadSupport readSupport) throws IOException {
    this(file, readSupport, null);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws java.io.IOException
   */
  public ParquetRowBlockParquetReader(Configuration conf, Path file, ParquetRowReadSupport readSupport) throws IOException {
    this(conf, file, readSupport, null);
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @param filter the filter to use to filter records
   * @throws java.io.IOException
   */
  private ParquetRowBlockParquetReader(Path file, ParquetRowReadSupport readSupport, UnboundRecordFilter filter)
      throws IOException {
    this(new Configuration(), file, readSupport, filter);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @param filter the filter to use to filter records
   * @throws java.io.IOException
   */
  public ParquetRowBlockParquetReader(Configuration conf, Path file, ParquetRowReadSupport readSupport,
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

    projectedMap = new int[readSupport.getTargetSchema().size()];
    for (int i = 0; i < readSupport.getTargetSchema().size(); i++) {
      projectedMap[i] = tajoSchema.getColumnId(readSupport.getTargetSchema().getColumn(i).getQualifiedName());
    }
  }

  Schema tajoSchema;
  int columnNum;
  TajoDataTypes.Type [] tajoTypes;

  /**
   * @return the next record or null if finished
   * @throws java.io.IOException
   */
  public boolean nextFetch(OffHeapRowBlock rowBlock) throws IOException {
    rowBlock.clear();

    try {
      if (reader != null) {
        RowWriter writer = rowBlock.getWriter();

        while(rowBlock.rows() < rowBlock.maxRowNum() && reader.nextKeyValue()) {

          writer.startRow();
          int prevId = -1;
          Object [] values = reader.getCurrentValue();
          for (int columnIdx = 0; columnIdx < projectedMap.length; columnIdx++) {
            int actualId = projectedMap[columnIdx];

            if (actualId - prevId > 1) {
              writer.skipField((actualId - prevId) - 1);
            }

            if (values[actualId] != null) {
              switch (tajoTypes[actualId]) {
              case BOOLEAN:
                writer.putBool((Boolean) values[actualId]);
                break;
              case CHAR:
                writer.putText(((Binary) values[actualId]).getBytes());
                break;
              case INT1:
              case INT2:
                writer.putInt2((Short) values[actualId]);
                break;
              case INT4:
              case INET4:
              case DATE:
                 writer.putInt4((Integer) values[actualId]);
                break;
              case INT8:
              case TIMESTAMP:
              case TIME:
                writer.putInt8((Long) values[actualId]);
                break;
              case FLOAT4:
                writer.putFloat4((Float) values[actualId]);
                break;
              case FLOAT8:
                writer.putFloat8((Double) values[actualId]);
                break;
              case TEXT:
                writer.putText(((Binary) values[actualId]).getBytes());
                break;
              case BLOB:
                writer.putBlob(((Binary) values[actualId]).getBytes());
                break;

              default:
                throw new IOException("Not supported type: " + tajoTypes[actualId].name());
              }
            } else {
              writer.skipField();
            }

            prevId = actualId;
          }

          writer.endRow();
        }

        return rowBlock.rows() > 0;
      } else {
        initReader();
        return reader == null ? null : nextFetch(rowBlock);
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
      reader = new ParquetRowDirectReader(readSupport, filter);
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
