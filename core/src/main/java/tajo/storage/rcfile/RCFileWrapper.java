/*
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

package tajo.storage.rcfile;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.DefaultCodec;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.catalog.statistics.TableStatistics;
import tajo.datum.ArrayDatum;
import tajo.datum.DatumFactory;
import tajo.ipc.protocolrecords.Fragment;
import tajo.storage.FileAppender;
import tajo.storage.FileScanner;
import tajo.storage.Tuple;
import tajo.storage.VTuple;
import tajo.storage.exception.AlreadyExistsStorageException;
import tajo.util.Bytes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class RCFileWrapper {

  public static class RCFileAppender extends FileAppender {
    private FileSystem fs;
    private Path path;
    private RCFile.Writer writer;

    private final boolean statsEnabled;
    private TableStatistics stats = null;

    public RCFileAppender(Configuration conf, TableMeta meta, Path path,
                          boolean statsEnabled) throws IOException {
      super(conf, meta, path);

      fs = path.getFileSystem(conf);

      if (!fs.exists(path.getParent())) {
//        throw new FileNotFoundException(this.path.toString());
        throw new FileNotFoundException(path.toString());
      }

      if (fs.exists(path)) {
//        throw new AlreadyExistsStorageException(this.path);
        throw new AlreadyExistsStorageException(path);
      }

      conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, schema.getColumnNum());
      writer = new RCFile.Writer(fs, conf, path, null, new DefaultCodec());

      this.statsEnabled = statsEnabled;
      if (statsEnabled) {
        this.stats = new TableStatistics(this.schema);
      }
    }

    @Override
    public long getOffset() throws IOException {
      return 0;
    }

    @Override
    public void addTuple(Tuple t) throws IOException {

      BytesRefArrayWritable byteRef =
          new BytesRefArrayWritable(schema.getColumnNum());
      BytesRefWritable cu;
      Column col;
      byte [] bytes;
      for (int i = 0; i < schema.getColumnNum(); i++) {
        if (statsEnabled) {
          stats.analyzeField(i, t.get(i));
        }

        if (t.isNull(i)) {
          cu = new BytesRefWritable(new byte[0]);
          byteRef.set(i, cu);
        } else {
          col = schema.getColumn(i);
          switch (col.getDataType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
              cu = new BytesRefWritable(t.get(i).asByteArray(), 0, 1);
              byteRef.set(i, cu);
              break;

            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case IPv4:
            case IPv6:
              bytes = t.get(i).asByteArray();
              cu = new BytesRefWritable(bytes, 0, bytes.length);
              byteRef.set(i, cu);
              break;
            case ARRAY: {
              ArrayDatum array = (ArrayDatum) t.get(i);
              String json = array.toJSON();
              bytes = json.getBytes();
              cu = new BytesRefWritable(bytes, 0, bytes.length);
              byteRef.set(i, cu);
              break;
            }
            default:
              break;
          }
        }
      }

      writer.append(byteRef);

      // Statistical section
      if (statsEnabled) {
        stats.incrementRow();
      }
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {
      writer.close();
    }

    @Override
    public TableStat getStats() {
      return stats.getTableStat();
    }
  }

  public static class RCFileScanner extends FileScanner {
    private FileSystem fs;
    private RCFile.Reader reader;
    private LongWritable rowId;
    private boolean [] projectMap;

    BytesRefArrayWritable column;
    private boolean more;
    long end;

    public RCFileScanner(Configuration conf, final Schema schema,
                          final Fragment fragment,
                          Schema target) throws IOException {
      super(conf, schema, new Fragment[] {fragment});
      fs = fragment.getPath().getFileSystem(conf);

      ArrayList<Integer> projIds = Lists.newArrayList();
      projectMap = new boolean[schema.getColumnNum()];
      for (int i = 0; i < schema.getColumnNum(); i++) {
        projectMap[i] =
            target.contains(schema.getColumn(i).getQualifiedName());
        if (projectMap[i]) {
          projIds.add(i);
        }
      }
      ColumnProjectionUtils.setReadColumnIDs(conf, projIds);

      reader = new RCFile.Reader(fs, fragment.getPath(), conf);
      if (fragment.getStartOffset() > reader.getPosition()) {
        reader.sync(fragment.getStartOffset()); // sync to start
      }

      end = fragment.getStartOffset() + fragment.getLength();
      more = fragment.getStartOffset() < end;

      rowId = new LongWritable();

      column = new BytesRefArrayWritable();
    }

    @Override
    public long getNextOffset() throws IOException {
      return reader.getPosition();
    }

    @Override
    public void seek(long offset) throws IOException {
      reader.seek(offset);
    }

    protected boolean next(LongWritable key) throws IOException {
      if (!more) {
        return false;
      }

      more = reader.next(key);
      if (!more) {
        return false;
      }

      long lastSeenSyncPos = reader.lastSeenSyncPos();
      if (lastSeenSyncPos >= end) {
        more = false;
        return more;
      }
      return more;
    }

    @Override
    public Tuple next() throws IOException {
      if (!next(rowId)) {
        return null;
      }

      column.clear();
      reader.getCurrentRow(column);
      column.resetValid(schema.getColumnNum());
      Tuple tuple = new VTuple(schema.getColumnNum());
      for (int i = 0; i < projectMap.length; i++) {
        if (!projectMap[i]) {
          tuple.put(i, DatumFactory.createNullDatum());
        } else {
          switch (schema.getColumn(i).getDataType()) {
            case BOOLEAN:
              tuple.put(i,
                  DatumFactory.createBool(column.get(i).getBytesCopy()[0]));
              break;
            case BYTE:
              tuple.put(i,
                  DatumFactory.createByte(column.get(i).getBytesCopy()[0]));
              break;
            case CHAR:
              tuple.put(i,
                  DatumFactory.createChar(column.get(i).getBytesCopy()[0]));
              break;

            case SHORT:
              tuple.put(i,
                  DatumFactory.createShort(Bytes.toShort(
                      column.get(i).getBytesCopy())));
              break;
            case INT:
              tuple.put(i,
                  DatumFactory.createInt(Bytes.toInt(
                      column.get(i).getBytesCopy())));
              break;

            case LONG:
              tuple.put(i,
                  DatumFactory.createLong(Bytes.toLong(
                      column.get(i).getBytesCopy())));
              break;

            case FLOAT:
              tuple.put(i,
                  DatumFactory.createFloat(Bytes.toFloat(
                      column.get(i).getBytesCopy())));
              break;

            case DOUBLE:
              tuple.put(i,
                  DatumFactory.createDouble(Bytes.toDouble(
                      column.get(i).getBytesCopy())));
              break;

            case IPv4:
              tuple.put(i,
                  DatumFactory.createIPv4(column.get(i).getBytesCopy()));
              break;

            case STRING:
              tuple.put(i,
                  DatumFactory.createString(
                      Bytes.toString(column.get(i).getBytesCopy())));
              break;

            case BYTES:
              tuple.put(i,
                  DatumFactory.createBytes(column.get(i).getBytesCopy()));
              break;

            default:
              throw new IOException("Unsupport data type");
          }
        }
      }

      return tuple;
    }

    @Override
    public void reset() throws IOException {
      reader.seek(0);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
