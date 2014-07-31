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

package org.apache.tajo.storage.rcfile;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;

import java.io.Closeable;
import java.io.*;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * <code>RCFile</code>s, short of Record Columnar File, are flat files
 * consisting of binary key/value pairs, which shares much similarity with
 * <code>SequenceFile</code>.
 * <p/>
 * RCFile stores columns of a table in a record columnar way. It first
 * partitions rows horizontally into row splits. and then it vertically
 * partitions each row split in a columnar way. RCFile first stores the meta
 * data of a row split, as the key part of a record, and all the data of a row
 * split as the value part. When writing, RCFile.Writer first holds records'
 * value bytes in memory, and determines a row split if the raw bytes size of
 * buffered records overflow a given parameter<tt>Writer.columnsBufferSize</tt>,
 * which can be set like: <code>conf.setInt(COLUMNS_BUFFER_SIZE_CONF_STR,
 * 4 * 1024 * 1024)</code> .
 * <p>
 * <code>RCFile</code> provides {@link Writer}, {@link Reader} and classes for
 * writing, reading respectively.
 * </p>
 * <p/>
 * <p>
 * RCFile stores columns of a table in a record columnar way. It first
 * partitions rows horizontally into row splits. and then it vertically
 * partitions each row split in a columnar way. RCFile first stores the meta
 * data of a row split, as the key part of a record, and all the data of a row
 * split as the value part.
 * </p>
 * <p/>
 * <p>
 * RCFile compresses values in a more fine-grained manner then record level
 * compression. However, It currently does not support compress the key part
 * yet. The actual compression algorithm used to compress key and/or values can
 * be specified by using the appropriate {@link CompressionCodec}.
 * </p>
 * <p/>
 * <p>
 * The {@link Reader} is used to read and explain the bytes of RCFile.
 * </p>
 * <p/>
 * <h4 id="Formats">RCFile Formats</h4>
 * <p/>
 * <p/>
 * <h5 id="Header">RC Header</h5>
 * <ul>
 * <li>version - 3 bytes of magic header <b>RCF</b>, followed by 1 byte of
 * actual version number (e.g. RCF1)</li>
 * <li>compression - A boolean which specifies if compression is turned on for
 * keys/values in this file.</li>
 * <li>compression codec - <code>CompressionCodec</code> class which is used
 * for compression of keys and/or values (if compression is enabled).</li>
 * <li>metadata - {@link Metadata} for this file.</li>
 * <li>sync - A sync marker to denote end of the header.</li>
 * </ul>
 * <p/>
 * <h5>RCFile Format</h5>
 * <ul>
 * <li><a href="#Header">Header</a></li>
 * <li>Record
 * <li>Key part
 * <ul>
 * <li>Record length in bytes</li>
 * <li>Key length in bytes</li>
 * <li>Number_of_rows_in_this_record(vint)</li>
 * <li>Column_1_ondisk_length(vint)</li>
 * <li>Column_1_row_1_value_plain_length</li>
 * <li>Column_1_row_2_value_plain_length</li>
 * <li>...</li>
 * <li>Column_2_ondisk_length(vint)</li>
 * <li>Column_2_row_1_value_plain_length</li>
 * <li>Column_2_row_2_value_plain_length</li>
 * <li>...</li>
 * </ul>
 * </li>
 * </li>
 * <li>Value part
 * <ul>
 * <li>Compressed or plain data of [column_1_row_1_value,
 * column_1_row_2_value,....]</li>
 * <li>Compressed or plain data of [column_2_row_1_value,
 * column_2_row_2_value,....]</li>
 * </ul>
 * </li>
 * </ul>
 * <p>
 * <pre>
 * {@code
 * The following is a pseudo-BNF grammar for RCFile. Comments are prefixed
 * with dashes:
 *
 * rcfile ::=
 *   <file-header>
 *   <rcfile-rowgroup>+
 *
 * file-header ::=
 *   <file-version-header>
 *   <file-key-class-name>              (only exists if version is seq6)
 *   <file-value-class-name>            (only exists if version is seq6)
 *   <file-is-compressed>
 *   <file-is-block-compressed>         (only exists if version is seq6)
 *   [<file-compression-codec-class>]
 *   <file-header-metadata>
 *   <file-sync-field>
 *
 * -- The normative RCFile implementation included with Hive is actually
 * -- based on a modified version of Hadoop's SequenceFile code. Some
 * -- things which should have been modified were not, including the code
 * -- that writes out the file version header. Consequently, RCFile and
 * -- SequenceFile originally shared the same version header.  A newer
 * -- release has created a unique version string.
 *
 * file-version-header ::= Byte[4] {'S', 'E', 'Q', 6}
 *                     |   Byte[4] {'R', 'C', 'F', 1}
 *
 * -- The name of the Java class responsible for reading the key buffer
 * -- component of the rowgroup.
 *
 * file-key-class-name ::=
 *   Text {"org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer"}
 *
 * -- The name of the Java class responsible for reading the value buffer
 * -- component of the rowgroup.
 *
 * file-value-class-name ::=
 *   Text {"org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer"}
 *
 * -- Boolean variable indicating whether or not the file uses compression
 * -- for the key and column buffer sections.
 *
 * file-is-compressed ::= Byte[1]
 *
 * -- A boolean field indicating whether or not the file is block compressed.
 * -- This field is *always* false. According to comments in the original
 * -- RCFile implementation this field was retained for backwards
 * -- compatability with the SequenceFile format.
 *
 * file-is-block-compressed ::= Byte[1] {false}
 *
 * -- The Java class name of the compression codec iff <file-is-compressed>
 * -- is true. The named class must implement
 * -- org.apache.hadoop.io.compress.CompressionCodec.
 * -- The expected value is org.apache.hadoop.io.compress.GzipCodec.
 *
 * file-compression-codec-class ::= Text
 *
 * -- A collection of key-value pairs defining metadata values for the
 * -- file. The Map is serialized using standard JDK serialization, i.e.
 * -- an Int corresponding to the number of key-value pairs, followed by
 * -- Text key and value pairs. The following metadata properties are
 * -- mandatory for all RCFiles:
 * --
 * -- hive.io.rcfile.column.number: the number of columns in the RCFile
 *
 * file-header-metadata ::= Map<Text, Text>
 *
 * -- A 16 byte marker that is generated by the writer. This marker appears
 * -- at regular intervals at the beginning of rowgroup-headers, and is
 * -- intended to enable readers to skip over corrupted rowgroups.
 *
 * file-sync-hash ::= Byte[16]
 *
 * -- Each row group is split into three sections: a header, a set of
 * -- key buffers, and a set of column buffers. The header section includes
 * -- an optional sync hash, information about the size of the row group, and
 * -- the total number of rows in the row group. Each key buffer
 * -- consists of run-length encoding data which is used to decode
 * -- the length and offsets of individual fields in the corresponding column
 * -- buffer.
 *
 * rcfile-rowgroup ::=
 *   <rowgroup-header>
 *   <rowgroup-key-data>
 *   <rowgroup-column-buffers>
 *
 * rowgroup-header ::=
 *   [<rowgroup-sync-marker>, <rowgroup-sync-hash>]
 *   <rowgroup-record-length>
 *   <rowgroup-key-length>
 *   <rowgroup-compressed-key-length>
 *
 * -- rowgroup-key-data is compressed if the column data is compressed.
 * rowgroup-key-data ::=
 *   <rowgroup-num-rows>
 *   <rowgroup-key-buffers>
 *
 * -- An integer (always -1) signaling the beginning of a sync-hash
 * -- field.
 *
 * rowgroup-sync-marker ::= Int
 *
 * -- A 16 byte sync field. This must match the <file-sync-hash> value read
 * -- in the file header.
 *
 * rowgroup-sync-hash ::= Byte[16]
 *
 * -- The record-length is the sum of the number of bytes used to store
 * -- the key and column parts, i.e. it is the total length of the current
 * -- rowgroup.
 *
 * rowgroup-record-length ::= Int
 *
 * -- Total length in bytes of the rowgroup's key sections.
 *
 * rowgroup-key-length ::= Int
 *
 * -- Total compressed length in bytes of the rowgroup's key sections.
 *
 * rowgroup-compressed-key-length ::= Int
 *
 * -- Number of rows in the current rowgroup.
 *
 * rowgroup-num-rows ::= VInt
 *
 * -- One or more column key buffers corresponding to each column
 * -- in the RCFile.
 *
 * rowgroup-key-buffers ::= <rowgroup-key-buffer>+
 *
 * -- Data in each column buffer is stored using a run-length
 * -- encoding scheme that is intended to reduce the cost of
 * -- repeated column field values. This mechanism is described
 * -- in more detail in the following entries.
 *
 * rowgroup-key-buffer ::=
 *   <column-buffer-length>
 *   <column-buffer-uncompressed-length>
 *   <column-key-buffer-length>
 *   <column-key-buffer>
 *
 * -- The serialized length on disk of the corresponding column buffer.
 *
 * column-buffer-length ::= VInt
 *
 * -- The uncompressed length of the corresponding column buffer. This
 * -- is equivalent to column-buffer-length if the RCFile is not compressed.
 *
 * column-buffer-uncompressed-length ::= VInt
 *
 * -- The length in bytes of the current column key buffer
 *
 * column-key-buffer-length ::= VInt
 *
 * -- The column-key-buffer contains a sequence of serialized VInt values
 * -- corresponding to the byte lengths of the serialized column fields
 * -- in the corresponding rowgroup-column-buffer. For example, consider
 * -- an integer column that contains the consecutive values 1, 2, 3, 44.
 * -- The RCFile format stores these values as strings in the column buffer,
 * -- e.g. "12344". The length of each column field is recorded in
 * -- the column-key-buffer as a sequence of VInts: 1,1,1,2. However,
 * -- if the same length occurs repeatedly, then we replace repeated
 * -- run lengths with the complement (i.e. negative) of the number of
 * -- repetitions, so 1,1,1,2 becomes 1,~2,2.
 *
 * column-key-buffer ::= Byte[column-key-buffer-length]
 *
 * rowgroup-column-buffers ::= <rowgroup-value-buffer>+
 *
 * -- RCFile stores all column data as strings regardless of the
 * -- underlying column type. The strings are neither length-prefixed or
 * -- null-terminated, and decoding them into individual fields requires
 * -- the use of the run-length information contained in the corresponding
 * -- column-key-buffer.
 *
 * rowgroup-column-buffer ::= Byte[column-buffer-length]
 *
 * Byte ::= An eight-bit byte
 *
 * VInt ::= Variable length integer. The high-order bit of each byte
 * indicates whether more bytes remain to be read. The low-order seven
 * bits are appended as increasingly more significant bits in the
 * resulting integer value.
 *
 * Int ::= A four-byte integer in big-endian format.
 *
 * Text ::= VInt, Chars (Length prefixed UTF-8 characters)
 * }
 * </pre>
 * </p>
 */
public class RCFile {

  private static final Log LOG = LogFactory.getLog(RCFile.class);

  public static final String RECORD_INTERVAL_CONF_STR = "hive.io.rcfile.record.interval";
  public static final String COLUMN_NUMBER_METADATA_STR = "hive.io.rcfile.column.number";

  // All of the versions should be place in this list.
  private static final int ORIGINAL_VERSION = 0;  // version with SEQ
  private static final int NEW_MAGIC_VERSION = 1; // version with RCF

  private static final int CURRENT_VERSION = NEW_MAGIC_VERSION;

  // The first version of RCFile used the sequence file header.
  private static final byte[] ORIGINAL_MAGIC = new byte[]{
      (byte) 'S', (byte) 'E', (byte) 'Q'};
  // the version that was included with the original magic, which is mapped
  // into ORIGINAL_VERSION
  private static final byte ORIGINAL_MAGIC_VERSION_WITH_METADATA = 6;

  private static final byte[] ORIGINAL_MAGIC_VERSION = new byte[]{
      (byte) 'S', (byte) 'E', (byte) 'Q', ORIGINAL_MAGIC_VERSION_WITH_METADATA
  };

  // The 'magic' bytes at the beginning of the RCFile
  private static final byte[] MAGIC = new byte[]{
      (byte) 'R', (byte) 'C', (byte) 'F'};

  private static final int SYNC_ESCAPE = -1; // "length" of sync entries
  private static final int SYNC_HASH_SIZE = 16; // number of bytes in hash
  private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash

  /**
   * The number of bytes between sync points.
   */
  public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;
  public static final String NULL = "rcfile.null";
  public static final String SERDE = "rcfile.serde";

  /**
   * KeyBuffer is the key of each record in RCFile. Its on-disk layout is as
   * below:
   * <p/>
   * <ul>
   * <li>record length in bytes,it is the sum of bytes used to store the key
   * part and the value part.</li>
   * <li>Key length in bytes, it is how many bytes used by the key part.</li>
   * <li>number_of_rows_in_this_record(vint),</li>
   * <li>column_1_ondisk_length(vint),</li>
   * <li>column_1_row_1_value_plain_length,</li>
   * <li>column_1_row_2_value_plain_length,</li>
   * <li>....</li>
   * <li>column_2_ondisk_length(vint),</li>
   * <li>column_2_row_1_value_plain_length,</li>
   * <li>column_2_row_2_value_plain_length,</li>
   * <li>.... .</li>
   * <li>{the end of the key part}</li>
   * </ul>
   */
  public static class KeyBuffer {
    // each column's length in the value
    private int[] eachColumnValueLen = null;
    private int[] eachColumnUncompressedValueLen = null;
    // stores each cell's length of a column in one DataOutputBuffer element
    private NonSyncByteArrayOutputStream[] allCellValLenBuffer = null;
    // how many rows in this split
    private int numberRows = 0;
    // how many columns
    private int columnNumber = 0;

    KeyBuffer(int columnNum) {
      columnNumber = columnNum;
      eachColumnValueLen = new int[columnNumber];
      eachColumnUncompressedValueLen = new int[columnNumber];
      allCellValLenBuffer = new NonSyncByteArrayOutputStream[columnNumber];
    }

    public void readFields(DataInput in) throws IOException {
      eachColumnValueLen = new int[columnNumber];
      eachColumnUncompressedValueLen = new int[columnNumber];
      allCellValLenBuffer = new NonSyncByteArrayOutputStream[columnNumber];

      numberRows = WritableUtils.readVInt(in);
      for (int i = 0; i < columnNumber; i++) {
        eachColumnValueLen[i] = WritableUtils.readVInt(in);
        eachColumnUncompressedValueLen[i] = WritableUtils.readVInt(in);
        int bufLen = WritableUtils.readVInt(in);
        if (allCellValLenBuffer[i] == null) {
          allCellValLenBuffer[i] = new NonSyncByteArrayOutputStream();
        } else {
          allCellValLenBuffer[i].reset();
        }
        allCellValLenBuffer[i].write(in, bufLen);
      }
    }

    /**
     * @return the numberRows
     */
    public int getNumberRows() {
      return numberRows;
    }
  }

  /**
   * ValueBuffer is the value of each record in RCFile. Its on-disk layout is as
   * below:
   * <ul>
   * <li>Compressed or plain data of [column_1_row_1_value,
   * column_1_row_2_value,....]</li>
   * <li>Compressed or plain data of [column_2_row_1_value,
   * column_2_row_2_value,....]</li>
   * </ul>
   */
  public static class ValueBuffer implements Closeable{

    // used to load columns' value into memory
    private NonSyncByteArrayOutputStream[] loadedColumnsValueBuffer = null;

    boolean inited = false;

    // used for readFields
    KeyBuffer keyBuffer;
    private int columnNumber = 0;

    // set true for columns that needed to skip loading into memory.
    boolean[] skippedColIDs = null;

    CompressionCodec codec;
    Decompressor decompressor = null;
    NonSyncDataInputBuffer decompressBuffer = new NonSyncDataInputBuffer();
    private long readBytes = 0;


    public ValueBuffer(KeyBuffer currentKey, int columnNumber,
                       int[] targets, CompressionCodec codec, boolean[] skippedIDs)
        throws IOException {
      keyBuffer = currentKey;
      this.columnNumber = columnNumber;
      this.skippedColIDs = skippedIDs;
      this.codec = codec;
      loadedColumnsValueBuffer = new NonSyncByteArrayOutputStream[targets.length];
      if (codec != null) {
        decompressor = org.apache.tajo.storage.compress.CodecPool.getDecompressor(codec);
      }

      for (int i = 0; i < targets.length; i++) {
        loadedColumnsValueBuffer[i] = new NonSyncByteArrayOutputStream();
      }
    }

    public void readFields(DataInput in) throws IOException {
      int addIndex = 0;
      int skipTotal = 0;


      for (int i = 0; i < columnNumber; i++) {
        int vaRowsLen = keyBuffer.eachColumnValueLen[i];
        // skip this column
        if (skippedColIDs[i]) {
          skipTotal += vaRowsLen;
          continue;
        }

        if (skipTotal != 0) {
          StorageUtil.skipFully(in, skipTotal);
          skipTotal = 0;
        }

        NonSyncByteArrayOutputStream valBuf;
        if (codec != null) {
          // load into compressed buf first

          byte[] compressedBytes = new byte[vaRowsLen];
          in.readFully(compressedBytes, 0, vaRowsLen);

          decompressBuffer.reset(compressedBytes, vaRowsLen);
          if(decompressor != null) decompressor.reset();

          DataInputStream is;
          if (codec instanceof SplittableCompressionCodec) {
            SplitCompressionInputStream deflatFilter = ((SplittableCompressionCodec) codec).createInputStream(
                decompressBuffer, decompressor, 0, vaRowsLen, SplittableCompressionCodec.READ_MODE.BYBLOCK);
            is = new DataInputStream(deflatFilter);
          } else {
            CompressionInputStream deflatFilter = codec.createInputStream(decompressBuffer, decompressor);
            is = new DataInputStream(deflatFilter);
          }

          valBuf = loadedColumnsValueBuffer[addIndex];
          valBuf.reset();
          valBuf.write(is, keyBuffer.eachColumnUncompressedValueLen[i]);
          is.close();
          decompressBuffer.close();
        } else {
          valBuf = loadedColumnsValueBuffer[addIndex];
          valBuf.reset();
          valBuf.write(in, vaRowsLen);
        }
        readBytes += keyBuffer.eachColumnUncompressedValueLen[i];
        addIndex++;
      }

      if (skipTotal != 0) {
        StorageUtil.skipFully(in, skipTotal);
      }
    }

    public long getReadBytes() {
      return readBytes;
    }

    public void clearColumnBuffer() throws IOException {
      decompressBuffer.reset();
      readBytes = 0;
    }

    @Override
    public void close() {
      for (NonSyncByteArrayOutputStream element : loadedColumnsValueBuffer) {
        IOUtils.closeStream(element);
      }
      if (codec != null) {
        IOUtils.closeStream(decompressBuffer);
        if (decompressor != null) {
          // Make sure we only return decompressor once.
          org.apache.tajo.storage.compress.CodecPool.returnDecompressor(decompressor);
          decompressor = null;
        }
      }
    }
  }

  /**
   * Create a metadata object with alternating key-value pairs.
   * Eg. metadata(key1, value1, key2, value2)
   */
  public static Metadata createMetadata(Text... values) {
    if (values.length % 2 != 0) {
      throw new IllegalArgumentException("Must have a matched set of " +
          "key-value pairs. " + values.length +
          " strings supplied.");
    }
    Metadata result = new Metadata();
    for (int i = 0; i < values.length; i += 2) {
      result.set(values[i], values[i + 1]);
    }
    return result;
  }

  /**
   * Write KeyBuffer/ValueBuffer pairs to a RCFile. RCFile's format is
   * compatible with SequenceFile's.
   */
  public static class RCFileAppender extends FileAppender {
    FSDataOutputStream out;

    CompressionCodec codec = null;
    Metadata metadata = null;
    FileSystem fs = null;
    TableStatistics stats = null;
    int columnNumber = 0;

    // how many records the writer buffers before it writes to disk
    private int RECORD_INTERVAL = Integer.MAX_VALUE;
    // the max size of memory for buffering records before writes them out
    private int COLUMNS_BUFFER_SIZE = 16 * 1024 * 1024; // 16M
    // the conf string for COLUMNS_BUFFER_SIZE
    public static final String COLUMNS_BUFFER_SIZE_CONF_STR = "hive.io.rcfile.record.buffer.size";

    // how many records already buffered
    private int bufferedRecords = 0;
    private ColumnBuffer[] columnBuffers = null;
    boolean useNewMagic = true;
    private byte[] nullChars;
    private SerializerDeserializer serde;
    private boolean isShuffle;

    // Insert a globally unique 16-byte value every few entries, so that one
    // can seek into the middle of a file and then synchronize with record
    // starts and ends by scanning for this value.
    long lastSyncPos; // position of last sync
    byte[] sync; // 16 random bytes

    {
      try {
        MessageDigest digester = MessageDigest.getInstance("MD5");
        long time = System.currentTimeMillis();
        digester.update((new UID() + "@" + time).getBytes());
        sync = digester.digest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /*
     * used for buffering appends before flush them out
     */
    class ColumnBuffer {
      // used for buffer a column's values
      NonSyncByteArrayOutputStream columnValBuffer;
      // used to store each value's length
      NonSyncByteArrayOutputStream valLenBuffer;

      /*
       * use a run-length encoding. We only record run length if a same
       * 'prevValueLen' occurs more than one time. And we negative the run
       * length to distinguish a runLength and a normal value length. For
       * example, if the values' lengths are 1,1,1,2, we record 1, ~2,2. And for
       * value lengths 1,2,3 we record 1,2,3.
       */
      int columnValueLength = 0;
      int uncompressedColumnValueLength = 0;
      int columnKeyLength = 0;
      int runLength = 0;
      int prevValueLength = -1;

      ColumnBuffer() throws IOException {
        columnValBuffer = new NonSyncByteArrayOutputStream();
        valLenBuffer = new NonSyncByteArrayOutputStream();
      }

      public int append(Column column, Datum datum) throws IOException {
        int currentLen = serde.serialize(column, datum, columnValBuffer, nullChars);
        columnValueLength += currentLen;
        uncompressedColumnValueLength += currentLen;

        if (prevValueLength < 0) {
          startNewGroup(currentLen);
          return currentLen;
        }

        if (currentLen != prevValueLength) {
          flushGroup();
          startNewGroup(currentLen);
        } else {
          runLength++;
        }
        return currentLen;
      }

      private void startNewGroup(int currentLen) {
        prevValueLength = currentLen;
        runLength = 0;
      }

      public void clear() {
        valLenBuffer.reset();
        columnValBuffer.reset();
        prevValueLength = -1;
        runLength = 0;
        columnValueLength = 0;
        columnKeyLength = 0;
        uncompressedColumnValueLength = 0;
      }

      public int flushGroup() {
        int len = 0;
        if (prevValueLength >= 0) {
          len += valLenBuffer.writeVLong(prevValueLength);
          if (runLength > 0) {
            len += valLenBuffer.writeVLong(~runLength);
          }
          columnKeyLength += len;
          runLength = -1;
          prevValueLength = -1;
        }
        return len;
      }

      public int UnFlushedGroupSize() {
        int len = 0;
        if (prevValueLength >= 0) {
          len += WritableUtils.getVIntSize(prevValueLength);
          if (runLength > 0) {
            len += WritableUtils.getVIntSize(~runLength);
          }
        }
        return len;
      }
    }

    public long getLength() throws IOException {
      return out.getPos();
    }

    public RCFileAppender(Configuration conf, final Schema schema, final TableMeta meta, final Path path) throws IOException {
      super(conf, schema, meta, path);

      RECORD_INTERVAL = conf.getInt(RECORD_INTERVAL_CONF_STR, RECORD_INTERVAL);
      COLUMNS_BUFFER_SIZE = conf.getInt(COLUMNS_BUFFER_SIZE_CONF_STR, COLUMNS_BUFFER_SIZE);
      columnNumber = schema.size();
    }

    public void init() throws IOException {
      fs = path.getFileSystem(conf);

      if (!fs.exists(path.getParent())) {
        throw new FileNotFoundException(path.toString());
      }

      //determine the intermediate file type
      String store = conf.get(TajoConf.ConfVars.SHUFFLE_FILE_FORMAT.varname,
          TajoConf.ConfVars.SHUFFLE_FILE_FORMAT.defaultVal);
      if (enabledStats && CatalogProtos.StoreType.RCFILE == CatalogProtos.StoreType.valueOf(store.toUpperCase())) {
        isShuffle = true;
      } else {
        isShuffle = false;
      }

      String codecClassname = this.meta.getOption(StorageConstants.COMPRESSION_CODEC);
      if (!StringUtils.isEmpty(codecClassname)) {
        try {
          Class<? extends CompressionCodec> codecClass = conf.getClassByName(
              codecClassname).asSubclass(CompressionCodec.class);
          codec = ReflectionUtils.newInstance(codecClass, conf);
        } catch (ClassNotFoundException cnfe) {
          throw new IllegalArgumentException(
              "Unknown codec: " + codecClassname, cnfe);
        }
      }

      String nullCharacters = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.RCFILE_NULL));
      if (StringUtils.isEmpty(nullCharacters)) {
        nullChars = NullDatum.get().asTextBytes();
      } else {
        nullChars = nullCharacters.getBytes();
      }

      if (metadata == null) {
        metadata = new Metadata();
      }

      metadata.set(new Text(COLUMN_NUMBER_METADATA_STR), new Text("" + columnNumber));

      String serdeClass = this.meta.getOption(StorageConstants.RCFILE_SERDE,
          BinarySerializerDeserializer.class.getName());
      try {
        serde = (SerializerDeserializer) Class.forName(serdeClass).newInstance();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new IOException(e);
      }
      metadata.set(new Text(StorageConstants.RCFILE_SERDE), new Text(serdeClass));

      columnBuffers = new ColumnBuffer[columnNumber];
      for (int i = 0; i < columnNumber; i++) {
        columnBuffers[i] = new ColumnBuffer();
      }

      init(conf, fs.create(path, true, 4096, (short) 3, fs.getDefaultBlockSize(), null), codec, metadata);
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();

      if (enabledStats) {
        this.stats = new TableStatistics(this.schema);
      }
      super.init();
    }

    /**
     * Write the initial part of file header.
     */
    void initializeFileHeader() throws IOException {
      if (useNewMagic) {
        out.write(MAGIC);
        out.write(CURRENT_VERSION);
      } else {
        out.write(ORIGINAL_MAGIC_VERSION);
      }
    }

    /**
     * Write the final part of file header.
     */
    void finalizeFileHeader() throws IOException {
      out.write(sync); // write the sync bytes
      out.flush(); // flush header
    }

    boolean isCompressed() {
      return codec != null;
    }

    /**
     * Write and flush the file header.
     */
    void writeFileHeader() throws IOException {
      if (useNewMagic) {
        out.writeBoolean(isCompressed());
      } else {
        Text.writeString(out, "org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer");
        Text.writeString(out, "org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer");
        out.writeBoolean(isCompressed());
        out.writeBoolean(false);
      }

      if (isCompressed()) {
        Text.writeString(out, (codec.getClass()).getName());
      }
      metadata.write(out);
    }

    void init(Configuration conf, FSDataOutputStream out,
              CompressionCodec codec, Metadata metadata) throws IOException {
      this.out = out;
      this.codec = codec;
      this.metadata = metadata;
      this.useNewMagic = conf.getBoolean(TajoConf.ConfVars.HIVEUSEEXPLICITRCFILEHEADER.varname, true);
    }

    /**
     * create a sync point.
     */
    public void sync() throws IOException {
      if (sync != null && lastSyncPos != out.getPos()) {
        out.writeInt(SYNC_ESCAPE); // mark the start of the sync
        out.write(sync); // write sync
        lastSyncPos = out.getPos(); // update lastSyncPos
      }
    }

    private void checkAndWriteSync() throws IOException {
      if (sync != null && out.getPos() >= lastSyncPos + SYNC_INTERVAL) {
        sync();
      }
    }

    private int columnBufferSize = 0;

    @Override
    public long getOffset() throws IOException {
      return out.getPos();
    }

    @Override
    public void flush() throws IOException {
      flushRecords();
      out.flush();
    }

    @Override
    public void addTuple(Tuple t) throws IOException {
      append(t);
      // Statistical section

      if (enabledStats) {
        stats.incrementRow();
      }
    }

    /**
     * Append a row of values. Currently it only can accept <
     * {@link Tuple}. If its <code>size()</code> is less than the
     * column number in the file, zero bytes are appended for the empty columns.
     * If its size() is greater then the column number in the file, the exceeded
     * columns' bytes are ignored.
     *
     * @param tuple a Tuple with the list of serialized columns
     * @throws IOException
     */
    public void append(Tuple tuple) throws IOException {
      int size = schema.size();

      for (int i = 0; i < size; i++) {
        Datum datum = tuple.get(i);
        int length = columnBuffers[i].append(schema.getColumn(i), datum);
        columnBufferSize += length;
        if (isShuffle) {
          // it is to calculate min/max values, and it is only used for the intermediate file.
          stats.analyzeField(i, datum);
        }
      }

      if (size < columnNumber) {
        for (int i = size; i < columnNumber; i++) {
          columnBuffers[i].append(schema.getColumn(i), NullDatum.get());
          if (isShuffle) {
            stats.analyzeField(i, NullDatum.get());
          }
        }
      }

      bufferedRecords++;
      //TODO compression rate base flush
      if ((columnBufferSize > COLUMNS_BUFFER_SIZE)
          || (bufferedRecords >= RECORD_INTERVAL)) {
        flushRecords();
      }
    }

    /**
     * get number of bytes to store the keyBuffer.
     *
     * @return number of bytes used to store this KeyBuffer on disk
     * @throws IOException
     */
    public int getKeyBufferSize() throws IOException {
      int ret = 0;
      ret += WritableUtils.getVIntSize(bufferedRecords);
      for (int i = 0; i < columnBuffers.length; i++) {
        ColumnBuffer currentBuf = columnBuffers[i];
        ret += WritableUtils.getVIntSize(currentBuf.columnValueLength);
        ret += WritableUtils.getVIntSize(currentBuf.uncompressedColumnValueLength);
        ret += WritableUtils.getVIntSize(currentBuf.columnKeyLength);
        ret += currentBuf.columnKeyLength;
      }

      return ret;
    }

    /**
     * get number of bytes to store the key part.
     *
     * @return number of bytes used to store this Key part on disk
     * @throws IOException
     */
    public int getKeyPartSize() throws IOException {
      int ret = 12; //12 bytes |record count, key length, compressed key length|

      ret += WritableUtils.getVIntSize(bufferedRecords);
      for (int i = 0; i < columnBuffers.length; i++) {
        ColumnBuffer currentBuf = columnBuffers[i];
        ret += WritableUtils.getVIntSize(currentBuf.columnValueLength);
        ret += WritableUtils.getVIntSize(currentBuf.uncompressedColumnValueLength);
        ret += WritableUtils.getVIntSize(currentBuf.columnKeyLength);
        ret += currentBuf.columnKeyLength;
        ret += currentBuf.UnFlushedGroupSize();
      }

      return ret;
    }

    private void WriteKeyBuffer(DataOutputStream out) throws IOException {
      WritableUtils.writeVLong(out, bufferedRecords);
      for (int i = 0; i < columnBuffers.length; i++) {
        ColumnBuffer currentBuf = columnBuffers[i];
        WritableUtils.writeVLong(out, currentBuf.columnValueLength);
        WritableUtils.writeVLong(out, currentBuf.uncompressedColumnValueLength);
        WritableUtils.writeVLong(out, currentBuf.columnKeyLength);
        currentBuf.valLenBuffer.writeTo(out);
      }
    }

    private void flushRecords() throws IOException {

      Compressor compressor = null;
      NonSyncByteArrayOutputStream valueBuffer = null;
      CompressionOutputStream deflateFilter = null;
      DataOutputStream deflateOut = null;
      boolean isCompressed = isCompressed();

      int valueLength = 0;
      if (isCompressed) {
        compressor = org.apache.tajo.storage.compress.CodecPool.getCompressor(codec);
        if (compressor != null) compressor.reset();  //builtin gzip is null

        valueBuffer = new NonSyncByteArrayOutputStream();
        deflateFilter = codec.createOutputStream(valueBuffer, compressor);
        deflateOut = new DataOutputStream(deflateFilter);
      }

      try {
        for (int columnIndex = 0; columnIndex < columnNumber; columnIndex++) {
          ColumnBuffer currentBuf = columnBuffers[columnIndex];
          currentBuf.flushGroup();

          NonSyncByteArrayOutputStream columnValue = currentBuf.columnValBuffer;
          int colLen;
          int plainLen = columnValue.getLength();
          if (isCompressed) {
            deflateFilter.resetState();
            deflateOut.write(columnValue.getData(), 0, columnValue.getLength());
            deflateOut.flush();
            deflateFilter.finish();
            columnValue.close();
            // find how much compressed data was added for this column
            colLen = valueBuffer.getLength() - valueLength;
            currentBuf.columnValueLength = colLen;
          } else {
            colLen = plainLen;
          }
          valueLength += colLen;
        }
      } catch (IOException e) {
        IOUtils.cleanup(LOG, deflateOut, out);
        throw e;
      }

      if (compressor != null) {
        org.apache.tajo.storage.compress.CodecPool.returnCompressor(compressor);
      }

      int keyLength = getKeyBufferSize();
      if (keyLength < 0) {
        throw new IOException("negative length keys not allowed: " + keyLength);
      }
      // Write the key out
      writeKey(keyLength + valueLength, keyLength);
      // write the value out
      if (isCompressed) {
        try {
          out.write(valueBuffer.getData(), 0, valueBuffer.getLength());
        } finally {
          IOUtils.cleanup(LOG, valueBuffer);
        }
      } else {
        for (int columnIndex = 0; columnIndex < columnNumber; ++columnIndex) {
          columnBuffers[columnIndex].columnValBuffer.writeTo(out);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Column#" + columnIndex + " : Plain Total Column Value Length: "
                + columnBuffers[columnIndex].uncompressedColumnValueLength
                + ",  Compr Total Column Value Length: " + columnBuffers[columnIndex].columnValueLength);
          }
        }
      }
      // clear the columnBuffers
      clearColumnBuffers();

      bufferedRecords = 0;
      columnBufferSize = 0;
    }

    private void writeKey(int recordLen, int keyLength) throws IOException {
      checkAndWriteSync(); // sync
      out.writeInt(recordLen); // total record length
      out.writeInt(keyLength); // key portion length

      if (this.isCompressed()) {
        Compressor compressor = org.apache.tajo.storage.compress.CodecPool.getCompressor(codec);
        if (compressor != null) compressor.reset();  //builtin gzip is null

        NonSyncByteArrayOutputStream compressionBuffer = new NonSyncByteArrayOutputStream();
        CompressionOutputStream deflateFilter = codec.createOutputStream(compressionBuffer, compressor);
        DataOutputStream deflateOut = new DataOutputStream(deflateFilter);

        //compress key and write key out
        compressionBuffer.reset();
        deflateFilter.resetState();
        WriteKeyBuffer(deflateOut);
        deflateOut.flush();
        deflateFilter.finish();
        int compressedKeyLen = compressionBuffer.getLength();
        out.writeInt(compressedKeyLen);
        compressionBuffer.writeTo(out);
        compressionBuffer.reset();
        deflateOut.close();
        org.apache.tajo.storage.compress.CodecPool.returnCompressor(compressor);
      } else {
        out.writeInt(keyLength);
        WriteKeyBuffer(out);
      }
    }

    private void clearColumnBuffers() throws IOException {
      for (int i = 0; i < columnNumber; i++) {
        columnBuffers[i].clear();
      }
    }

    @Override
    public TableStats getStats() {
      if (enabledStats) {
        return stats.getTableStat();
      } else {
        return null;
      }
    }

    @Override
    public void close() throws IOException {
      if (bufferedRecords > 0) {
        flushRecords();
      }
      clearColumnBuffers();

      if (out != null) {
        // Statistical section
        if (enabledStats) {
          stats.setNumBytes(getOffset());
        }
        // Close the underlying stream if we own it...
        out.flush();
        IOUtils.cleanup(LOG, out);
        out = null;
      }
    }
  }

  /**
   * Read KeyBuffer/ValueBuffer pairs from a RCFile.
   */
  public static class RCFileScanner extends FileScanner {
    private static class SelectedColumn {
      public int colIndex;
      public int rowReadIndex;
      public int runLength;
      public int prvLength;
      public boolean isNulled;
    }

    private FSDataInputStream in;

    private byte version;

    private CompressionCodec codec = null;
    private Metadata metadata = null;

    private byte[] sync;
    private byte[] syncCheck;
    private boolean syncSeen;
    private long lastSeenSyncPos = 0;

    private long headerEnd;
    private long start, end;
    private final long startOffset, endOffset;
    private int[] targetColumnIndexes;

    private int currentKeyLength;
    private int currentRecordLength;

    private ValueBuffer currentValue;

    private int readRowsIndexInBuffer = 0;

    private int recordsNumInValBuffer = 0;

    private int columnNumber = 0;

    private boolean more = true;

    private int passedRowsNum = 0;

    private boolean decompress = false;

    private Decompressor keyDecompressor;

    private long readBytes = 0;

    //Current state of each selected column - e.g. current run length, etc.
    // The size of the array is equal to the number of selected columns
    private SelectedColumn[] selectedColumns;

    // column value lengths for each of the selected columns
    private NonSyncDataInputBuffer[] colValLenBufferReadIn;

    private LongWritable rowId;
    private byte[] nullChars;
    private SerializerDeserializer serde;

    public RCFileScanner(Configuration conf, final Schema schema, final TableMeta meta,
                         final FileFragment fragment) throws IOException {
      super(conf, schema, meta, fragment);
      conf.setInt("io.file.buffer.size", 4096); //TODO remove

      startOffset = fragment.getStartKey();
      endOffset = startOffset + fragment.getEndKey();
      start = 0;
    }

    @Override
    public void init() throws IOException {
      sync = new byte[SYNC_HASH_SIZE];
      syncCheck = new byte[SYNC_HASH_SIZE];

      more = startOffset < endOffset;
      rowId = new LongWritable();
      readBytes = 0;

      String nullCharacters = StringEscapeUtils.unescapeJava(meta.getOption(StorageConstants.RCFILE_NULL));
      if (StringUtils.isEmpty(nullCharacters)) {
        nullChars = NullDatum.get().asTextBytes();
      } else {
        nullChars = nullCharacters.getBytes();
      }

      // projection
      if (targets == null) {
        targets = schema.toArray();
      }

      targetColumnIndexes = new int[targets.length];
      for (int i = 0; i < targets.length; i++) {
        targetColumnIndexes[i] = schema.getColumnId(targets[i].getQualifiedName());
      }
      Arrays.sort(targetColumnIndexes);

      FileSystem fs = fragment.getPath().getFileSystem(conf);
      end = fs.getFileStatus(fragment.getPath()).getLen();
      in = openFile(fs, fragment.getPath(), 4096);
      if (LOG.isDebugEnabled()) {
        LOG.debug("RCFile open:" + fragment.getPath() + "," + start + "," + (endOffset - startOffset) +
            "," + fs.getFileStatus(fragment.getPath()).getLen());
      }
      //init RCFILE Header
      boolean succeed = false;
      try {
        if (start > 0) {
          seek(0);
          initHeader();
        } else {
          initHeader();
        }
        succeed = true;
      } finally {
        if (!succeed) {
          if (in != null) {
            try {
              in.close();
            } catch (IOException e) {
              if (LOG != null && LOG.isDebugEnabled()) {
                LOG.debug("Exception in closing " + in, e);
              }
            }
          }
        }
      }

      columnNumber = Integer.parseInt(metadata.get(new Text(COLUMN_NUMBER_METADATA_STR)).toString());
      selectedColumns = new SelectedColumn[targetColumnIndexes.length];
      colValLenBufferReadIn = new NonSyncDataInputBuffer[targetColumnIndexes.length];
      boolean[] skippedColIDs = new boolean[columnNumber];
      Arrays.fill(skippedColIDs, true);
      super.init();

      for (int i = 0; i < targetColumnIndexes.length; i++) {
        int tid = targetColumnIndexes[i];
        if (tid < columnNumber) {
          skippedColIDs[tid] = false;

          SelectedColumn col = new SelectedColumn();
          col.colIndex = tid;
          col.runLength = 0;
          col.prvLength = -1;
          col.rowReadIndex = 0;
          selectedColumns[i] = col;
          colValLenBufferReadIn[i] = new NonSyncDataInputBuffer();
        }
      }

      currentKey = createKeyBuffer();
      currentValue = new ValueBuffer(null, columnNumber, targetColumnIndexes, codec, skippedColIDs);

      if (startOffset > getPosition()) {    // TODO use sync cache
        sync(startOffset); // sync to start
      }
    }

    /**
     * Return the metadata (Text to Text map) that was written into the
     * file.
     */
    public Metadata getMetadata() {
      return metadata;
    }

    /**
     * Return the metadata value associated with the given key.
     *
     * @param key the metadata key to retrieve
     */
    public Text getMetadataValueOf(Text key) {
      return metadata.get(key);
    }

    /**
     * Override this method to specialize the type of
     * {@link FSDataInputStream} returned.
     */
    protected FSDataInputStream openFile(FileSystem fs, Path file, int bufferSize) throws IOException {
      return fs.open(file, bufferSize);
    }

    private void initHeader() throws IOException {
      byte[] magic = new byte[MAGIC.length];
      in.readFully(magic);

      if (Arrays.equals(magic, ORIGINAL_MAGIC)) {
        byte vers = in.readByte();
        if (vers != ORIGINAL_MAGIC_VERSION_WITH_METADATA) {
          throw new IOException(fragment.getPath() + " is a version " + vers +
              " SequenceFile instead of an RCFile.");
        }
        version = ORIGINAL_VERSION;
      } else {
        if (!Arrays.equals(magic, MAGIC)) {
          throw new IOException(fragment.getPath() + " not a RCFile and has magic of " +
              new String(magic));
        }

        // Set 'version'
        version = in.readByte();
        if (version > CURRENT_VERSION) {
          throw new VersionMismatchException((byte) CURRENT_VERSION, version);
        }
      }

      if (version == ORIGINAL_VERSION) {
        try {
          Class<?> keyCls = conf.getClassByName(Text.readString(in));
          Class<?> valCls = conf.getClassByName(Text.readString(in));
          if (!keyCls.equals(KeyBuffer.class)
              || !valCls.equals(ValueBuffer.class)) {
            throw new IOException(fragment.getPath() + " not a RCFile");
          }
        } catch (ClassNotFoundException e) {
          throw new IOException(fragment.getPath() + " not a RCFile", e);
        }
      }

      decompress = in.readBoolean(); // is compressed?

      if (version == ORIGINAL_VERSION) {
        // is block-compressed? it should be always false.
        boolean blkCompressed = in.readBoolean();
        if (blkCompressed) {
          throw new IOException(fragment.getPath() + " not a RCFile.");
        }
      }

      // setup the compression codec
      if (decompress) {
        String codecClassname = Text.readString(in);
        try {
          Class<? extends CompressionCodec> codecClass = conf.getClassByName(
              codecClassname).asSubclass(CompressionCodec.class);
          codec = ReflectionUtils.newInstance(codecClass, conf);
        } catch (ClassNotFoundException cnfe) {
          throw new IllegalArgumentException(
              "Unknown codec: " + codecClassname, cnfe);
        }

        keyDecompressor = org.apache.tajo.storage.compress.CodecPool.getDecompressor(codec);
      }

      metadata = new Metadata();
      metadata.readFields(in);

      Text text = metadata.get(new Text(StorageConstants.RCFILE_SERDE));

      try {
        String serdeClass;
        if(text != null && !text.toString().isEmpty()){
          serdeClass = text.toString();
        } else{
          serdeClass = this.meta.getOption(StorageConstants.RCFILE_SERDE, BinarySerializerDeserializer.class.getName());
        }
        serde = (SerializerDeserializer) Class.forName(serdeClass).newInstance();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new IOException(e);
      }

      in.readFully(sync); // read sync bytes
      headerEnd = in.getPos();
      lastSeenSyncPos = headerEnd; //initial sync position
      readBytes += headerEnd;
    }

    /**
     * Return the current byte position in the input file.
     */
    public long getPosition() throws IOException {
      return in.getPos();
    }

    /**
     * Set the current byte position in the input file.
     * <p/>
     * <p/>
     * The position passed must be a position returned by
     * {@link RCFile.RCFileAppender#getLength()} when writing this file. To seek to an
     * arbitrary position, use {@link RCFile.RCFileScanner#sync(long)}. In another
     * words, the current seek can only seek to the end of the file. For other
     * positions, use {@link RCFile.RCFileScanner#sync(long)}.
     */
    public void seek(long position) throws IOException {
      in.seek(position);
    }

    /**
     * Resets the values which determine if there are more rows in the buffer
     * <p/>
     * This can be used after one calls seek or sync, if one called next before that.
     * Otherwise, the seek or sync will have no effect, it will continue to get rows from the
     * buffer built up from the call to next.
     */
    public void resetBuffer() {
      readRowsIndexInBuffer = 0;
      recordsNumInValBuffer = 0;
    }

    /**
     * Seek to the next sync mark past a given position.
     */
    public void sync(long position) throws IOException {
      if (position + SYNC_SIZE >= end) {
        seek(end);
        return;
      }

      //this is to handle syn(pos) where pos < headerEnd.
      if (position < headerEnd) {
        // seek directly to first record
        in.seek(headerEnd);
        // note the sync marker "seen" in the header
        syncSeen = true;
        return;
      }

      try {
        seek(position + 4); // skip escape

        int prefix = sync.length;
        int n = conf.getInt("io.bytes.per.checksum", 512);
        byte[] buffer = new byte[prefix + n];
        n = (int) Math.min(n, end - in.getPos());
        /* fill array with a pattern that will never match sync */
        Arrays.fill(buffer, (byte) (~sync[0]));
        while (n > 0 && (in.getPos() + n) <= end) {
          position = in.getPos();
          in.readFully(buffer, prefix, n);
          readBytes += n;
          /* the buffer has n+sync bytes */
          for (int i = 0; i < n; i++) {
            int j;
            for (j = 0; j < sync.length && sync[j] == buffer[i + j]; j++) {
              /* nothing */
            }
            if (j == sync.length) {
              /* simplified from (position + (i - prefix) + sync.length) - SYNC_SIZE */
              in.seek(position + i - SYNC_SIZE);
              return;
            }
          }
          /* move the last 16 bytes to the prefix area */
          System.arraycopy(buffer, buffer.length - prefix, buffer, 0, prefix);
          n = (int) Math.min(n, end - in.getPos());
        }
      } catch (ChecksumException e) { // checksum failure
        handleChecksumException(e);
      }
    }

    private void handleChecksumException(ChecksumException e) throws IOException {
      if (conf.getBoolean("io.skip.checksum.errors", false)) {
        LOG.warn("Bad checksum at " + getPosition() + ". Skipping entries.");
        sync(getPosition() + conf.getInt("io.bytes.per.checksum", 512));
      } else {
        throw e;
      }
    }

    private KeyBuffer createKeyBuffer() {
      return new KeyBuffer(columnNumber);
    }

    /**
     * Read and return the next record length, potentially skipping over a sync
     * block.
     *
     * @return the length of the next record or -1 if there is no next record
     * @throws IOException
     */
    private int readRecordLength() throws IOException {
      if (in.getPos() >= end) {
        return -1;
      }
      int length = in.readInt();
      readBytes += 4;
      if (sync != null && length == SYNC_ESCAPE) { // process
        // a
        // sync entry
        lastSeenSyncPos = in.getPos() - 4; // minus SYNC_ESCAPE's length
        in.readFully(syncCheck); // read syncCheck
        readBytes += SYNC_HASH_SIZE;
        if (!Arrays.equals(sync, syncCheck)) {
          throw new IOException("File is corrupt!");
        }
        syncSeen = true;
        if (in.getPos() >= end) {
          return -1;
        }
        length = in.readInt(); // re-read length
        readBytes += 4;
      } else {
        syncSeen = false;
      }
      return length;
    }

    private void seekToNextKeyBuffer() throws IOException {
      if (!keyInit) {
        return;
      }
      if (!currentValue.inited) {
        IOUtils.skipFully(in, currentRecordLength - currentKeyLength);
      }
    }

    private int compressedKeyLen = 0;
    NonSyncDataInputBuffer keyDataIn = new NonSyncDataInputBuffer();
    NonSyncDataInputBuffer keyDecompressBuffer = new NonSyncDataInputBuffer();

    KeyBuffer currentKey = null;
    boolean keyInit = false;

    protected int nextKeyBuffer() throws IOException {
      seekToNextKeyBuffer();
      currentRecordLength = readRecordLength();
      if (currentRecordLength == -1) {
        keyInit = false;
        return -1;
      }
      currentKeyLength = in.readInt();
      compressedKeyLen = in.readInt();
      readBytes += 8;
      if (decompress) {

        byte[] compressedBytes = new byte[compressedKeyLen];
        in.readFully(compressedBytes, 0, compressedKeyLen);

        if (keyDecompressor != null) keyDecompressor.reset();
        keyDecompressBuffer.reset(compressedBytes, compressedKeyLen);

        DataInputStream is;
        if (codec instanceof SplittableCompressionCodec) {
          SplitCompressionInputStream deflatFilter = ((SplittableCompressionCodec) codec).createInputStream(
              keyDecompressBuffer, keyDecompressor, 0, compressedKeyLen, SplittableCompressionCodec.READ_MODE.BYBLOCK);

          keyDecompressBuffer.seek(deflatFilter.getAdjustedStart());
          is = new DataInputStream(deflatFilter);
        } else {
          CompressionInputStream deflatFilter = codec.createInputStream(keyDecompressBuffer, keyDecompressor);
          is = new DataInputStream(deflatFilter);
        }

        byte[] deCompressedBytes = new byte[currentKeyLength];

        is.readFully(deCompressedBytes, 0, currentKeyLength);
        keyDataIn.reset(deCompressedBytes, currentKeyLength);
        currentKey.readFields(keyDataIn);
        is.close();
      } else {
        currentKey.readFields(in);
      }
      readBytes += currentKeyLength;
      keyInit = true;
      currentValue.inited = false;

      readRowsIndexInBuffer = 0;
      recordsNumInValBuffer = currentKey.numberRows;

      for (int selIx = 0; selIx < selectedColumns.length; selIx++) {
        SelectedColumn col = selectedColumns[selIx];
        if (col == null) {
          col = new SelectedColumn();
          col.isNulled = true;
          selectedColumns[selIx] = col;
          continue;
        }

        int colIx = col.colIndex;
        NonSyncByteArrayOutputStream buf = currentKey.allCellValLenBuffer[colIx];
        colValLenBufferReadIn[selIx].reset(buf.getData(), buf.getLength());
        col.rowReadIndex = 0;
        col.runLength = 0;
        col.prvLength = -1;
        col.isNulled = buf.getLength() == 0;
      }

      return currentKeyLength;
    }

    protected void currentValueBuffer() throws IOException {
      if (!keyInit) {
        nextKeyBuffer();
      }
      currentValue.keyBuffer = currentKey;
      currentValue.clearColumnBuffer();
      currentValue.readFields(in);
      currentValue.inited = true;
      readBytes += currentValue.getReadBytes();

      if (tableStats != null) {
        tableStats.setReadBytes(readBytes);
        tableStats.setNumRows(passedRowsNum);
      }
    }

    private boolean rowFetched = false;

    @Override
    public Tuple next() throws IOException {
      if (!more) {
        return null;
      }

      more = nextBuffer(rowId);
      long lastSeenSyncPos = lastSeenSyncPos();
      if (lastSeenSyncPos >= endOffset) {
        more = false;
        return null;
      }

      if (!more) {
        return null;
      }

      Tuple tuple = new VTuple(schema.size());
      getCurrentRow(tuple);
      return tuple;
    }

    @Override
    public float getProgress() {
      try {
        if(!more) {
          return 1.0f;
        }
        long filePos = getPosition();
        if (startOffset == filePos) {
          return 0.0f;
        } else {
          //if scanner read the header, filePos moved to zero
          return Math.min(1.0f, (float)(Math.max(filePos - startOffset, 0)) / (float)(fragment.getEndKey()));
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        return 0.0f;
      }
    }

    /**
     * Returns how many rows we fetched with nextBuffer(). It only means how many rows
     * are read by nextBuffer(). The returned result may be smaller than actual number
     * of rows passed by, because {@link #seek(long)} can change the underlying key buffer and
     * value buffer.
     *
     * @return next row number
     * @throws IOException
     */
    public boolean nextBuffer(LongWritable readRows) throws IOException {
      if (readRowsIndexInBuffer < recordsNumInValBuffer) {
        readRows.set(passedRowsNum);
        readRowsIndexInBuffer++;
        passedRowsNum++;
        rowFetched = false;
        return true;
      } else {
        keyInit = false;
      }

      int ret = -1;
      try {
        ret = nextKeyBuffer();
      } catch (EOFException eof) {
        eof.printStackTrace();
      }
      return (ret > 0) && nextBuffer(readRows);
    }

    /**
     * get the current row used,make sure called {@link #next()}
     * first.
     *
     * @throws IOException
     */
    public void getCurrentRow(Tuple tuple) throws IOException {
      if (!keyInit || rowFetched) {
        return;
      }

      if (!currentValue.inited) {
        currentValueBuffer();
      }

      for (int j = 0; j < selectedColumns.length; ++j) {
        SelectedColumn col = selectedColumns[j];
        int i = col.colIndex;

        if (col.isNulled) {
          tuple.put(i, NullDatum.get());
        } else {
          colAdvanceRow(j, col);

          Datum datum = serde.deserialize(schema.getColumn(i),
              currentValue.loadedColumnsValueBuffer[j].getData(), col.rowReadIndex, col.prvLength, nullChars);
          tuple.put(i, datum);
          col.rowReadIndex += col.prvLength;
        }
      }
      rowFetched = true;
    }

    /**
     * Advance column state to the next now: update offsets, run lengths etc
     *
     * @param selCol - index among selectedColumns
     * @param col    - column object to update the state of.  prvLength will be
     *               set to the new read position
     * @throws IOException
     */
    private void colAdvanceRow(int selCol, SelectedColumn col) throws IOException {
      if (col.runLength > 0) {
        --col.runLength;
      } else {
        int length = (int) WritableUtils.readVLong(colValLenBufferReadIn[selCol]);
        if (length < 0) {
          // we reach a runlength here, use the previous length and reset
          // runlength
          col.runLength = (~length) - 1;
        } else {
          col.prvLength = length;
          col.runLength = 0;
        }
      }
    }

    /**
     * Returns true if the previous call to next passed a sync mark.
     */
    public boolean syncSeen() {
      return syncSeen;
    }

    /**
     * Returns the last seen sync position.
     */
    public long lastSeenSyncPos() {
      return lastSeenSyncPos;
    }

    /**
     * Returns the name of the file.
     */
    @Override
    public String toString() {
      return fragment.getPath().toString();
    }

    @Override
    public void reset() throws IOException {
      seek(startOffset);
    }

    @Override
    public boolean isProjectable() {
      return true;
    }

    @Override
    public boolean isSelectable() {
      return false;
    }

    @Override
    public boolean isSplittable() {
      return true;
    }

    @Override
    public void close() throws IOException {
      if (tableStats != null) {
        tableStats.setReadBytes(readBytes);  //Actual Processed Bytes. (decompressed bytes + header - seek)
        tableStats.setNumRows(passedRowsNum);
      }

      IOUtils.cleanup(LOG, in, currentValue);
      if (keyDecompressor != null) {
        // Make sure we only return decompressor once.
        org.apache.tajo.storage.compress.CodecPool.returnDecompressor(keyDecompressor);
        keyDecompressor = null;
      }
    }
  }
}
