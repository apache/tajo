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

package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.statistics.TableStat;
import tajo.datum.ArrayDatum;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.json.GsonCreator;
import tajo.util.BitArray;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class RawFile {
  public static class RawFileScanner extends FileScanner implements SeekableScanner {
    private FileChannel channel;
    private DataType [] columnTypes;
    private Path path;

    private ByteBuffer buffer;
    private Tuple tuple;

    private int headerSize = 0;
    private BitArray nullFlags;
    private static final int RECORD_SIZE = 4;
    private int numBitsOfNullFlags;

    public RawFileScanner(Configuration conf, TableMeta meta, Path path) throws IOException {
      super(conf, meta, null);
      this.path = path;
      init();
    }

    @SuppressWarnings("unused")
    public RawFileScanner(Configuration conf, TableMeta meta, Fragment fragment) throws IOException {
      this(conf, meta, fragment.getPath());
    }

    public void init() throws IOException {
      //Preconditions.checkArgument(FileUtil.isLocalPath(path));
      // TODO - to make it unified one.
      URI uri = path.toUri();
      RandomAccessFile raf = new RandomAccessFile(new File(uri), "r");
      channel = raf.getChannel();

      buffer = ByteBuffer.allocateDirect(65535);

      columnTypes = new DataType[schema.getColumnNum()];
      for (int i = 0; i < schema.getColumnNum(); i++) {
        columnTypes[i] = schema.getColumn(i).getDataType();
      }

      tuple = new VTuple(columnTypes.length);

      // initial read
      channel.read(buffer);
      buffer.flip();

      numBitsOfNullFlags = (int) Math.ceil((double)schema.getColumnNum());
      nullFlags = new BitArray(numBitsOfNullFlags);
      headerSize = RECORD_SIZE + 2 + nullFlags.size();

      super.init();
    }

    @Override
    public long getNextOffset() throws IOException {
      return channel.position();
    }

    @Override
    public void seek(long offset) throws IOException {
      channel.position(offset);
    }

    private boolean fillBuffer() throws IOException {
      buffer.compact();
      if (channel.read(buffer) == -1) {
        return false;
      } else {
        buffer.flip();
        return true;
      }
    }

    @Override
    public Tuple next() throws IOException {

      if (buffer.remaining() < headerSize) {
        if (!fillBuffer()) {
          return null;
        }
      }

      // backup the buffer state
      int recordOffset = buffer.position();
      int bufferLimit = buffer.limit();

      int recordSize = buffer.getInt();
      int nullFlagSize = buffer.getShort();
      buffer.limit(buffer.position() + nullFlagSize);
      nullFlags.fromByteBuffer(buffer);

      // restore the start of record contents
      buffer.limit(bufferLimit);
      buffer.position(recordOffset + headerSize);

      if (buffer.remaining() < (recordSize - headerSize)) {
        if (!fillBuffer()) {
          return null;
        }
      }

      for (int i = 0; i < columnTypes.length; i++) {
        // check if the i'th column is null
        if (nullFlags.get(i)) {
          tuple.put(i, DatumFactory.createNullDatum());
          continue;
        }

        switch (columnTypes[i]) {
          case BOOLEAN :
            tuple.put(i, DatumFactory.createBool(buffer.get()));
            break;

          case BYTE :
            tuple.put(i, DatumFactory.createByte(buffer.get()));
            break;

          case CHAR :
            tuple.put(i, DatumFactory.createChar(buffer.getChar()));
            break;

          case SHORT :
            tuple.put(i, DatumFactory.createShort(buffer.getShort()));
            break;

          case INT :
            tuple.put(i, DatumFactory.createInt(buffer.getInt()));
            break;

          case LONG :
            tuple.put(i, DatumFactory.createLong(buffer.getLong()));
            break;

          case FLOAT :
            tuple.put(i, DatumFactory.createFloat(buffer.getFloat()));
            break;

          case DOUBLE :
            tuple.put(i, DatumFactory.createDouble(buffer.getDouble()));
            break;

          case STRING :
            // TODO - shoud use CharsetEncoder / CharsetDecoder
            int strSize = buffer.getInt();
            byte [] strBytes = new byte[strSize];
            buffer.get(strBytes);
            tuple.put(i, DatumFactory.createString(new String(strBytes)));
            break;

          case STRING2 :
            // TODO - shoud use CharsetEncoder / CharsetDecoder
            int strSize2 = buffer.getInt();
            byte [] strBytes2 = new byte[strSize2];
            buffer.get(strBytes2);
            tuple.put(i, DatumFactory.createString2(new String(strBytes2)));
            break;

          case BYTES :
            int byteSize = buffer.getInt();
            byte [] rawBytes = new byte[byteSize];
            buffer.get(rawBytes);
            tuple.put(i, DatumFactory.createBytes(rawBytes));
            break;

          case IPv4 :
            byte [] ipv4Bytes = new byte[4];
            buffer.get(ipv4Bytes);
            tuple.put(i, DatumFactory.createIPv4(ipv4Bytes));
            break;

          case ARRAY:
            int arrayByteSize = buffer.getInt();
            byte [] arrayBytes = new byte[arrayByteSize];
            buffer.get(arrayBytes);
            String json = new String(arrayBytes);
            ArrayDatum array = (ArrayDatum) GsonCreator
                .getInstance().fromJson(json, Datum.class);
            tuple.put(i, array);
            break;

            default:
        }
      }

      return tuple;
    }

    @Override
    public void reset() throws IOException {
      // clear the buffer
      buffer.clear();
      // reload initial buffer
      channel.position(0);
      channel.read(buffer);
      buffer.flip();
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }

    @Override
    public boolean isProjectable() {
      return false;
    }

    @Override
    public boolean isSelectable() {
      return false;
    }
  }

  public static class RawFileAppender extends FileAppender {
    private FileChannel channel;
    private RandomAccessFile randomAccessFile;
    private DataType[] columnTypes;

    private ByteBuffer buffer;
    private BitArray nullFlags;
    private int headerSize = 0;
    private static final int RECORD_SIZE = 4;
    private int numBitsOfNullFlags;

    private TableStatistics stats;

    public RawFileAppender(Configuration conf, TableMeta meta, Path path) throws IOException {
      super(conf, meta, path);
    }

    public void init() throws IOException {
      // TODO - RawFile only works on Local File System.
      //Preconditions.checkArgument(FileUtil.isLocalPath(path));
      File file = new File(path.toUri());
      randomAccessFile = new RandomAccessFile(file, "rw");
      channel = randomAccessFile.getChannel();

      columnTypes = new DataType[schema.getColumnNum()];
      for (int i = 0; i < schema.getColumnNum(); i++) {
        columnTypes[i] = schema.getColumn(i).getDataType();
      }

      buffer = ByteBuffer.allocateDirect(65535);

      // comput the number of bytes, representing the null flags
      numBitsOfNullFlags = (int) Math.ceil((double)schema.getColumnNum());
      nullFlags = new BitArray(numBitsOfNullFlags);
      headerSize = RECORD_SIZE + 2 + nullFlags.size();

      if (enabledStats) {
        this.stats = new TableStatistics(this.schema);
      }

      super.init();
    }

    @Override
    public long getOffset() throws IOException {
      return channel.position();
    }

    private void flushBuffer() throws IOException {
      buffer.limit(buffer.position());
      buffer.flip();
      channel.write(buffer);
      buffer.clear();
    }

    private boolean flushBufferAndReplace(int recordOffset, int sizeToBeWritten)
        throws IOException {

      // if the buffer reaches the limit,
      // write the bytes from 0 to the previous record.
      if (buffer.remaining() < sizeToBeWritten) {

        int limit = buffer.position();
        buffer.limit(recordOffset);
        buffer.flip();
        channel.write(buffer);
        buffer.position(recordOffset);
        buffer.limit(limit);
        buffer.compact();

        return true;
      } else {
        return false;
      }
    }

    @Override
    public void addTuple(Tuple t) throws IOException {

      if (buffer.remaining() < headerSize) {
        flushBuffer();
      }

      // skip the row header
      int recordOffset = buffer.position();
      buffer.position(buffer.position() + headerSize);

      // reset the null flags
      nullFlags.clear();
      for (int i = 0; i < schema.getColumnNum(); i++) {
        if (enabledStats) {
          stats.analyzeField(i, t.get(i));
        }

        if (t.isNull(i)) {
          nullFlags.set(i);
          continue;
        }

        // 8 is the maximum bytes size of all types
        if (flushBufferAndReplace(recordOffset, 8)) {
          recordOffset = 0;
        }

        switch(columnTypes[i]) {
          case BOOLEAN :
          case BYTE :
            buffer.put(t.get(i).asByte());
            break;

          case CHAR :
            buffer.putChar(t.get(i).asChar());
            break;

          case SHORT :
            buffer.putShort(t.get(i).asShort());
            break;

          case INT :
            buffer.putInt(t.get(i).asInt());
            break;

          case LONG :
            buffer.putLong(t.get(i).asLong());
            break;

          case FLOAT :
            buffer.putFloat(t.get(i).asFloat());
            break;

          case DOUBLE:
            buffer.putDouble(t.get(i).asDouble());
            break;

          case STRING:
            byte [] strBytes = t.get(i).asByteArray();
            if (flushBufferAndReplace(recordOffset, strBytes.length + 4)) {
              recordOffset = 0;
            }
            buffer.putInt(strBytes.length);
            buffer.put(strBytes);
            break;

          case STRING2:
            byte [] strBytes2 = t.get(i).asByteArray();
            if (flushBufferAndReplace(recordOffset, strBytes2.length + 4)) {
              recordOffset = 0;
            }
            buffer.putInt(strBytes2.length);
            buffer.put(strBytes2);
            break;

          case BYTES:
            byte [] rawBytes = t.get(i).asByteArray();
            if (flushBufferAndReplace(recordOffset, rawBytes.length + 4)) {
              recordOffset = 0;
            }
            buffer.putInt(rawBytes.length);
            buffer.put(rawBytes);
            break;

          case IPv4:
            buffer.put(t.get(i).asByteArray());
            break;

          case ARRAY:
            ArrayDatum array = (ArrayDatum) t.get(i);
            String json = array.toJSON();
            byte [] jsonBytes = json.getBytes();
            if (flushBufferAndReplace(recordOffset, jsonBytes.length + 4)) {
              recordOffset = 0;
            }
            buffer.putInt(jsonBytes.length);
            buffer.put(jsonBytes);
            break;

          default:
        }
      }

      // write a record header
      int pos = buffer.position();
      buffer.position(recordOffset);
      buffer.putInt(pos - recordOffset);
      byte [] flags = nullFlags.toArray();
      buffer.putShort((short) flags.length);
      buffer.put(flags);
      buffer.position(pos);

      if (enabledStats) {
        stats.incrementRow();
      }
    }

    @Override
    public void flush() throws IOException {
      flushBuffer();
      channel.force(true);
    }

    @Override
    public void close() throws IOException {
      flush();
      randomAccessFile.close();
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
}
