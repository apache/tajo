/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.storage.trevni;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ValueType;
import tajo.catalog.Column;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.storage.FileAppender;
import tajo.storage.TableStatistics;
import tajo.storage.Tuple;

import java.io.FileNotFoundException;
import java.io.IOException;

import static tajo.catalog.proto.CatalogProtos.DataType;

public class TrevniAppender extends FileAppender {
  private FileSystem fs;
  private ColumnFileWriter writer;
  private FSDataOutputStream fos;

  private TableStatistics stats = null;
  private boolean flushed = false;

  public TrevniAppender(Configuration conf, TableMeta meta, Path path) throws IOException {
    super(conf, meta, path);
  }

  public void init() throws IOException {
    fs = path.getFileSystem(conf);

    if (!fs.exists(path.getParent())) {
      throw new FileNotFoundException(path.toString());
    }

    fos = fs.create(path);

    ColumnMetaData [] trevniMetas =
        new ColumnMetaData[meta.getSchema().getColumnNum()];
    int i = 0;
    for (Column column : meta.getSchema().getColumns()) {
      trevniMetas[i++] = new ColumnMetaData(column.getColumnName(),
          getType(column.getDataType()));
    }

    writer = new ColumnFileWriter(createFileMeta(), trevniMetas);

    if (enabledStats) {
      this.stats = new TableStatistics(this.schema);
    }

    super.init();
  }

  private ColumnFileMetaData createFileMeta() {
    return new ColumnFileMetaData()
        .setCodec("null")
        .setChecksum("null");
  }

  private static ValueType getType(DataType type) {
    switch (type) {
      case BOOLEAN:
        return ValueType.INT;
      case BYTE:
        return ValueType.INT;
      case CHAR:
        return ValueType.INT;
      case SHORT:
        return ValueType.INT;
      case INT:
        return ValueType.INT;
      case LONG:
        return ValueType.LONG;
      case FLOAT:
        return ValueType.FLOAT;
      case DOUBLE:
        return ValueType.DOUBLE;
      case STRING:
      case STRING2:
        return ValueType.STRING;
      case BYTES:
        return ValueType.BYTES;
      case IPv4:
        return ValueType.BYTES;
      case IPv6:
        return ValueType.BYTES;
      case ARRAY:
        return ValueType.STRING;
      default:
        return null;
    }
  }

  @Override
  public long getOffset() throws IOException {
    return 0;
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    Column col;
    writer.startRow();
    for (int i = 0; i < schema.getColumnNum(); i++) {
      if (enabledStats) {
        stats.analyzeField(i, t.get(i));
      }

      if (!t.isNull(i)) {
        col = schema.getColumn(i);
        switch (col.getDataType()) {
          case BOOLEAN:
          case BYTE:
          case CHAR:
          case SHORT:
          case INT:
            writer.writeValue(t.get(i).asInt(), i);
            break;
          case LONG:
            writer.writeValue(t.get(i).asLong(), i);
            break;
          case FLOAT:
            writer.writeValue(t.get(i).asFloat(), i);
            break;
          case DOUBLE:
            writer.writeValue(t.get(i).asDouble(), i);
            break;
          case STRING:
          case STRING2:
            writer.writeValue(t.get(i).asChars(), i);
            break;
          case IPv4:
          case IPv6:
            writer.writeValue(t.get(i).asByteArray(), i);
            break;
          case ARRAY:
            writer.writeValue(t.get(i).asChars(), i);
            break;
          case BYTES:
            writer.writeValue(t.get(i).asByteArray(), i);
          default:
            break;
        }
      }
    }
    writer.endRow();

    // Statistical section
    if (enabledStats) {
      stats.incrementRow();
    }
  }

  @Override
  public void flush() throws IOException {
    if (!flushed) {
      writer.writeTo(fos);
      fos.flush();
      flushed = true;
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    IOUtils.closeQuietly(fos);
  }

  @Override
  public TableStat getStats() {
    if (enabledStats) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
