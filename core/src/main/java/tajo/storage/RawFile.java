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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.statistics.TableStat;
import tajo.catalog.statistics.TableStatistics;
import tajo.datum.DatumFactory;
import tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.BitSet;

public class RawFile {
  public static class Scanner extends FileScanner {
    private SeekableByteChannel channel;
    private DataType [] columnTypes;
    private Path path;

    private ByteBuffer buffer1;
    private ByteBuffer buffer2;
    private ByteBuffer current;
    private ByteBuffer post;
    private Tuple tuple;

    private int headerSize = 0;
    private BitSet nullFlags;
    private static final int RECORD_SIZE = 4;
    private int numBytesOfNullFlags;

    public Scanner(Configuration conf, TableMeta meta, Path path) {
      super(conf, meta.getSchema(), null);
      this.path = path;
    }

    public void init() throws IOException {
      Preconditions.checkArgument(FileUtil.isLocalPath(path));
      channel = Files.newByteChannel(Paths.get(path.toUri()));

      buffer1 = ByteBuffer.allocateDirect(65535);
      buffer2 = ByteBuffer.allocateDirect(65535);

      columnTypes = new DataType[schema.getColumnNum()];
      for (int i = 0; i < schema.getColumnNum(); i++) {
        columnTypes[i] = schema.getColumn(i).getDataType();
      }

      tuple = new VTuple(columnTypes.length);

      // initial read
      channel.read(buffer1);
      buffer1.flip();

      current = buffer1;
      post = buffer2;

      numBytesOfNullFlags = (int) Math.ceil(((double)schema.getColumnNum()) / 8);
      nullFlags = new BitSet(numBytesOfNullFlags);
      headerSize = RECORD_SIZE + numBytesOfNullFlags;
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
      if (current.hasRemaining()) {
        post.put(current);
      }

      if (channel.read(post) == -1 && post.position() == 0) {
        return false;
      }

      // switch
      ByteBuffer tmp = current;
      this.current = post;
      this.post = tmp;

      this.current.flip();
      this.post.flip();

      return true;
    }

    @Override
    public Tuple next() throws IOException {

      if (current.remaining() < headerSize) {
        if (fillBuffer() == false) {
          return null;
        }
      }

      int recordSize = current.getInt();
      int bufferLimit = current.limit();
      current.limit(current.position() + numBytesOfNullFlags);
      nullFlags = BitSet.valueOf(current);
      current.position(current.limit());
      current.limit(bufferLimit);

      if (current.remaining() < recordSize) {
        if (fillBuffer() == false) {
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
            tuple.put(i, DatumFactory.createBool(current.get()));
            break;
          case BYTE :
            tuple.put(i, DatumFactory.createByte(current.get()));
            break;
          case CHAR :
            tuple.put(i, DatumFactory.createChar(current.getChar()));
            break;
          case SHORT :
            tuple.put(i, DatumFactory.createShort(current.getShort()));
            break;
          case INT :
            tuple.put(i, DatumFactory.createInt(current.getInt()));;
            break;
          case LONG :
            tuple.put(i, DatumFactory.createLong(current.getLong()));
            break;
          case FLOAT :
            tuple.put(i, DatumFactory.createFloat(current.getFloat()));
            break;
          case DOUBLE :
            tuple.put(i, DatumFactory.createDouble(current.getDouble()));
            break;
          case STRING :
            // TODO - shoud use CharsetEncoder / CharsetDecoder
            int strSize = current.getInt();
            byte [] strBytes = new byte[strSize];
            current.get(strBytes);
            tuple.put(i, DatumFactory.createString(new String(strBytes)));
            break;
          case BYTES :
            int byteSize = current.getInt();
            byte [] rawBytes = new byte[byteSize];
            current.get(rawBytes);
            tuple.put(i, DatumFactory.createBytes(rawBytes));
            break;
          case IPv4 :
            byte [] ipv4Bytes = new byte[4];
            current.get(ipv4Bytes);
            tuple.put(i, DatumFactory.createIPv4(ipv4Bytes));
            break;
        }
      }

      return tuple;
    }

    @Override
    public void reset() throws IOException {
      buffer1.flip();
      buffer2.flip();
      channel.position(0);
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }

  public static class Appender extends FileAppender {
    private FileChannel channel;
    private RandomAccessFile randomAccessFile;
    private DataType[] columnTypes;

    private ByteBuffer buffer1;
    private ByteBuffer buffer2;
    private ByteBuffer current;
    private ByteBuffer next;

    private BitSet nullFlags;
    private int headerSize = 0;
    private static final int RECORD_SIZE = 4;
    private int numBytesOfNullFlags;

    private boolean enabledStat = false;
    private TableStatistics stats;

    public Appender(Configuration conf, TableMeta meta, Path path) {
      super(conf, meta, path);
    }

    public void init() throws IOException {
      Preconditions.checkArgument(FileUtil.isLocalPath(path));
      File file = new File(path.toUri());
      randomAccessFile = new RandomAccessFile(file, "rw");
      channel = randomAccessFile.getChannel();

      columnTypes = new DataType[schema.getColumnNum()];
      for (int i = 0; i < schema.getColumnNum(); i++) {
        columnTypes[i] = schema.getColumn(i).getDataType();
      }

      buffer1 = ByteBuffer.allocateDirect(65535);
      buffer2 = ByteBuffer.allocateDirect(65535);
      current = buffer1;
      next = buffer2;

      // comput the number of bytes, representing the null flags
      numBytesOfNullFlags = (int) Math.ceil(((double)schema.getColumnNum()) / 8);
      nullFlags = new BitSet(numBytesOfNullFlags);
      headerSize = RECORD_SIZE + numBytesOfNullFlags;

      if (enabledStat) {
        this.stats = new TableStatistics(this.schema);
      }
    }

    @Override
    public long getOffset() throws IOException {
      return channel.position();
    }

    private void flushBuffer() throws IOException {
      current.flip();
      channel.write(current);
      current.flip();
    }

    private void flushBufferAndReplace(int recordOffset, int sizeToBeWritten)
        throws IOException {

      // if current buffer reaches the limit,
      // copy the remain bytes to the next buffer and switch both.
      if (current.remaining() < sizeToBeWritten) {

        int limit = current.position();
        current.limit(recordOffset);
        current.flip();
        channel.write(current);
        current.position(recordOffset);
        current.limit(limit);
        next.put(current);

        ByteBuffer tmp = current;
        current = next;
        next = tmp;
        next.clear();
      }
    }

    @Override
    public void addTuple(Tuple t) throws IOException {

      if (current.remaining() < headerSize) {
        flushBuffer();
      }

      // skip the row header
      int recordOffset = current.position();
      current.position(current.position() + headerSize);

      // reset the null flags
      nullFlags.clear();
      for (int i = 0; i < schema.getColumnNum(); i++) {
        if (enabledStat) {
          stats.analyzeField(i, t.get(i));
        }

        if (t.isNull(i)) {
          nullFlags.set(i);
        }

        // 8 is the maximum bytes size of all types
        flushBufferAndReplace(recordOffset, 8);

        switch(columnTypes[i]) {
          case BOOLEAN :
          case BYTE :
            current.put(t.get(i).asByte());
            break;
          case CHAR :
            current.putChar(t.get(i).asChar());
            break;
          case SHORT :
            current.putShort(t.get(i).asShort());
            break;
          case INT :
            current.putInt(t.get(i).asInt());
            break;
          case LONG :
            current.putLong(t.get(i).asLong());
            break;
          case FLOAT :
            current.putFloat(t.get(i).asFloat());
            break;
          case DOUBLE:
            current.putDouble(t.get(i).asDouble());
            break;
          case STRING:
            byte [] strBytes = t.get(i).asByteArray();
            flushBufferAndReplace(recordOffset, strBytes.length + 4);
            current.putInt(strBytes.length);
            current.put(strBytes);
            break;
          case BYTES:
            byte [] rawBytes = t.get(i).asByteArray();
            flushBufferAndReplace(recordOffset, rawBytes.length + 4);
            current.putInt(rawBytes.length);
            current.put(rawBytes);
            break;
          case IPv4:
            current.put(t.get(i).asByteArray());
            break;
        }
      }

      // write a record header
      int pos = current.position();
      current.putInt(recordOffset, pos);
      current.position(recordOffset + RECORD_SIZE);
      current.put(nullFlags.toByteArray());
      current.position(pos);

      if (enabledStat) {
        stats.incrementRow();
      }
    }

    @Override
    public void flush() throws IOException {
      current.flip();
      channel.write(current);
      channel.force(true);
    }

    @Override
    public void close() throws IOException {
      flush();
      randomAccessFile.close();
    }

    @Override
    public TableStat getStats() {
      if (enabledStat) {
        return stats.getTableStat();
      } else {
        return null;
      }
    }
  }
}
