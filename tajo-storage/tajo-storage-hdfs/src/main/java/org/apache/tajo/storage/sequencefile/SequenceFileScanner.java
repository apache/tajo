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

package org.apache.tajo.storage.sequencefile;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.text.ByteBufLineReader;
import org.apache.tajo.storage.text.DelimitedTextFile;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.storage.text.TextLineParsingError;

import java.io.IOException;

public class SequenceFileScanner extends FileScanner {
  private static final Log LOG = LogFactory.getLog(SequenceFileScanner.class);

  private FileSystem fs;
  private SequenceFile.Reader reader;
  private SerializerDeserializer serde;
  private byte[] nullChars;
  private char delimiter;

  private int currentIdx = 0;
  private int[] projectionMap;

  private boolean hasBinarySerDe = false;
  private long totalBytes = 0L;

  private long start, end;
  private boolean  more = true;

  /**
   * Whether a field is null or not. Because length is 0 does not means the
   * field is null. In particular, a 0-length string is not null.
   */
  private boolean[] fieldIsNull;

  /**
   * The start positions and lengths of fields. Only valid when the data is parsed.
   */
  private int[] fieldStart;
  private int[] fieldLength;

  private int elementOffset, elementSize;

  private Writable EMPTY_KEY;

  private TextLineDeserializer deserializer;
  private ByteBuf byteBuf = BufferPool.directBuffer(ByteBufLineReader.DEFAULT_BUFFER);

  private Tuple outTuple;

  public SequenceFileScanner(Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
    super(conf, schema, meta, fragment);
  }

  @Override
  public void init() throws IOException {
    // FileFragment information
    if(fs == null) {
      fs = FileScanner.getFileSystem((TajoConf)conf, fragment.getPath());
    }

    reader = new SequenceFile.Reader(fs, fragment.getPath(), conf);

    // Set value of non-deprecated key for backward compatibility.
    TableMeta tableMeta;
    try {
      tableMeta = (TableMeta) meta.clone();

      if (!tableMeta.containsProperty(StorageConstants.TEXT_DELIMITER)) {
        tableMeta.putProperty(StorageConstants.TEXT_DELIMITER, tableMeta.getProperty(StorageConstants
          .SEQUENCEFILE_DELIMITER));
      }

      if (!tableMeta.containsProperty(StorageConstants.TEXT_NULL) && tableMeta.containsProperty(StorageConstants
        .SEQUENCEFILE_NULL)) {
        tableMeta.putProperty(StorageConstants.TEXT_NULL, tableMeta.getProperty(StorageConstants.SEQUENCEFILE_NULL));
      }
    } catch (CloneNotSupportedException e) {
      throw new TajoInternalError(e);
    }

    String delim  = tableMeta.getProperty(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    this.delimiter = StringEscapeUtils.unescapeJava(delim).charAt(0);

    String nullCharacters = StringEscapeUtils.unescapeJava(tableMeta.getProperty(StorageConstants.TEXT_NULL,
        NullDatum.DEFAULT_TEXT));
    if (StringUtils.isEmpty(nullCharacters)) {
      nullChars = NullDatum.get().asTextBytes();
    } else {
      nullChars = nullCharacters.getBytes();
    }

    this.start = fragment.getStartKey();
    this.end = start + fragment.getLength();

    if (fragment.getStartKey() > reader.getPosition())
      reader.sync(this.start);

    more = start < end;

    if (targets == null) {
      targets = schema.toArray();
    }

    outTuple = new VTuple(targets.length);
    fieldIsNull = new boolean[schema.getRootColumns().size()];
    fieldStart = new int[schema.getRootColumns().size()];
    fieldLength = new int[schema.getRootColumns().size()];

    prepareProjection(targets);

    try {
      String serdeClass = tableMeta.getProperty(StorageConstants.SEQUENCEFILE_SERDE,
        TextSerializerDeserializer.class.getName());
      serde = (SerializerDeserializer) Class.forName(serdeClass).newInstance();
      serde.init(schema);

      if (serde instanceof BinarySerializerDeserializer) {
        hasBinarySerDe = true;
      } else {
        deserializer = DelimitedTextFile.getLineSerde(tableMeta).createDeserializer(schema, tableMeta, targets);
        deserializer.init();
      }

      Class<? extends Writable> keyClass = (Class<? extends Writable>)Class.forName(reader.getKeyClassName());
      EMPTY_KEY = keyClass.newInstance();

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }
    super.init();
  }

  public Writable getKey() {
    return EMPTY_KEY;
  }

  private void prepareProjection(Column [] targets) {
    projectionMap = new int[targets.length];

    int tid;
    for (int i = 0; i < targets.length; i++) {
      tid = schema.getColumnId(targets[i].getQualifiedName());
      projectionMap[i] = tid;
    }
  }

  Text text = new Text();

  @Override
  public Tuple next() throws IOException {
    if (!more) return null;

    long pos = reader.getPosition();
    boolean remaining = reader.next(EMPTY_KEY);

    if (pos >= end && reader.syncSeen()) {
      more = false;
    } else {
      more = remaining;
    }

    if (more) {
      if (hasBinarySerDe) {
        BytesWritable bytesWritable = new BytesWritable();
        reader.getCurrentValue(bytesWritable);
        makeTuple(bytesWritable);
        totalBytes += (long)bytesWritable.getBytes().length;
      } else {
        reader.getCurrentValue(text);

        byteBuf.clear();
        byteBuf.writeBytes(text.getBytes(), 0, text.getLength());

        try {
          deserializer.deserialize(byteBuf, outTuple);
        } catch (TextLineParsingError e) {
          throw new IOException(e);
        }
      }
      currentIdx++;
      return outTuple;
    } else {
      return null;
    }
  }

  /**
   * In hive, LazyBinarySerDe is serialized as follows: start A B A B A B end bytes[] ->
   * |-----|---------|--- ... ---|-----|---------|
   *
   * Section A is one null-byte, corresponding to eight struct fields in Section
   * B. Each bit indicates whether the corresponding field is null (0) or not null
   * (1). Each field is a LazyBinaryObject.
   *
   * Following B, there is another section A and B. This pattern repeats until the
   * all struct fields are serialized.
   *
   * So, tajo must make a tuple after parsing hive style BinarySerDe.
   */
  private Tuple makeTuple(BytesWritable value) throws IOException{
    int start = 0;
    int length = value.getLength();

    /**
     * Please note that one null byte is followed by eight fields, then more
     * null byte and fields.
     */
    int structByteEnd = start + length;
    byte[] bytes = value.getBytes();

    byte nullByte = bytes[start];
    int lastFieldByteEnd = start + 1;

    // Go through all bytes in the byte[]
    for (int i = 0; i < schema.getRootColumns().size(); i++) {
      fieldIsNull[i] = true;
      if ((nullByte & (1 << (i % 8))) != 0) {
        fieldIsNull[i] = false;
        parse(schema.getColumn(i), bytes, lastFieldByteEnd);

        fieldStart[i] = lastFieldByteEnd + elementOffset;
        fieldLength[i] = elementSize;
        lastFieldByteEnd = fieldStart[i] + fieldLength[i];

        for (int aProjectionMap : projectionMap) {
          if (aProjectionMap == i) {
            Datum datum = serde.deserialize(i, bytes, fieldStart[i], fieldLength[i], nullChars);
            outTuple.put(i, datum);
          }
        }
      }

      // next byte is a null byte if there are more bytes to go
      if (7 == (i % 8)) {
        if (lastFieldByteEnd < structByteEnd) {
          nullByte = bytes[lastFieldByteEnd];
          lastFieldByteEnd++;
        } else {
          // otherwise all null afterwards
          nullByte = 0;
          lastFieldByteEnd++;
        }
      }
    }

    return outTuple;
  }

  /**
   * Check a particular field and set its size and offset in bytes based on the
   * field type and the bytes arrays.
   *
   * For void, boolean, byte, short, int, long, float and double, there is no
   * offset and the size is fixed. For string, the first four bytes are used to store the size.
   * So the offset is 4 and the size is computed by concating the first four bytes together.
   * The first four bytes are defined with respect to the offset in the bytes arrays.
   *
   * @param col
   *          catalog column information
   * @param bytes
   *          bytes arrays store the table row
   * @param offset
   *          offset of this field
   */
  private void parse(Column col, byte[] bytes, int offset) throws
      IOException {
    switch (col.getDataType().getType()) {
      case BOOLEAN:
      case BIT:
        elementOffset = 0;
        elementSize = 1;
        break;
      case INT2:
        elementOffset = 0;
        elementSize = 2;
        break;
      case INT4:
      case INT8:
        elementOffset = 0;
        elementSize = WritableUtils.decodeVIntSize(bytes[offset]);
        break;
      case FLOAT4:
        elementOffset = 0;
        elementSize = 4;
        break;
      case FLOAT8:
        elementOffset = 0;
        elementSize = 8;
        break;
      case BLOB:
      case PROTOBUF:
      case CHAR:
      case TEXT:
        elementOffset = 1;
        elementSize = bytes[offset];
        break;
      default:
        elementOffset = 0;
        elementSize = 0;
    }
  }

  @Override
  public void reset() throws IOException {
    if (reader != null) {
      reader.sync(0);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null)
      reader.close();

    if (inputStats != null) {
      inputStats.setReadBytes(totalBytes);
      inputStats.setNumRows(currentIdx);
    }

    outTuple = null;
    if (this.byteBuf.refCnt() > 0) {
      this.byteBuf.release();
    }
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
  public void setFilter(EvalNode filter) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public boolean isSplittable(){
    return true;
  }

  @Override
  public float getProgress() {
    if (!inited) return super.getProgress();

    if (!more) {
      return 1.0f;
    } else {
      long filePos;
      float progress;
      try {
        filePos = reader.getPosition();
        if (start == filePos) {
          progress = 0.0f;
        } else {
          long readBytes = filePos - start;
          long remainingBytes = Math.max(end - filePos, 0);
          progress = Math.min(1.0f, (float) (readBytes) / (float) (readBytes + remainingBytes));
        }
      } catch (IOException e) {
        progress = 0.0f;
      }
      return progress;
    }
  }
}
