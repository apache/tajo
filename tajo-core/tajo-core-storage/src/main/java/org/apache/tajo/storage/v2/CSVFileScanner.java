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

package org.apache.tajo.storage.v2;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.storage.SeekableScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.json.StorageGsonHelper;

public class CSVFileScanner extends FileScannerV2 {
  public static final String DELIMITER = "csvfile.delimiter";
  public static final String DELIMITER_DEFAULT = "|";
  public static final byte LF = '\n';
  private static final Log LOG = LogFactory.getLog(CSVFileScanner.class);

  private final static int DEFAULT_BUFFER_SIZE = 256 * 1024;
  private int bufSize;
  private char delimiter;
  private FSDataInputStream fis;
  private InputStream is; //decompressd stream
  private CompressionCodecFactory factory;
  private CompressionCodec codec;
  private Decompressor decompressor;
  private Seekable filePosition;
  private boolean splittable = true;
  private long startOffset, length;
  private byte[] buf = null;
  private String[] tuples = null;
  private long[] tupleOffsets = null;
  private int currentIdx = 0, validIdx = 0;
  private byte[] tail = null;
  private long pageStart = -1;
  private long prevTailLen = -1;
  private int[] targetColumnIndexes;
  private boolean eof = false;

  public CSVFileScanner(Configuration conf, final TableMeta meta,
                    final Fragment fragment) throws IOException {
    super(conf, meta, fragment);
    factory = new CompressionCodecFactory(conf);
    codec = factory.getCodec(fragment.getPath());
    if (isCompress() && !(codec instanceof SplittableCompressionCodec)) {
      splittable = false;
    }
  }

  @Override
  public void init() throws IOException {
    // Buffer size, Delimiter
    this.bufSize = DEFAULT_BUFFER_SIZE;
    String delim  = fragment.getMeta().getOption(DELIMITER, DELIMITER_DEFAULT);
    this.delimiter = delim.charAt(0);

    super.init();
  }

  @Override
  protected void initFirstScan() throws IOException {
    if(!firstSchdeuled) {
      return;
    }
    firstSchdeuled = false;

    // Fragment information
    fis = fs.open(fragment.getPath(), 128 * 1024);
    startOffset = fragment.getStartOffset();
    length = fragment.getLength();

    if (startOffset > 0) {
      startOffset--; // prev line feed
    }

    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(
            fis, decompressor, startOffset, startOffset + length,
            SplittableCompressionCodec.READ_MODE.BYBLOCK);

        startOffset = cIn.getAdjustedStart();
        length = cIn.getAdjustedEnd() - startOffset;
        filePosition = cIn;
        is = cIn;
      } else {
        is = new DataInputStream(codec.createInputStream(fis, decompressor));
      }
    } else {
      fis.seek(startOffset);
      filePosition = fis;
      is = fis;
    }

    tuples = new String[0];
    if (targets == null) {
      targets = schema.toArray();
    }

    targetColumnIndexes = new int[targets.length];
    for (int i = 0; i < targets.length; i++) {
      targetColumnIndexes[i] = schema.getColumnIdByName(targets[i].getColumnName());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("CSVScanner open:" + fragment.getPath() + "," + startOffset + "," + length +
          "," + fs.getFileStatus(fragment.getPath()).getLen());
    }

    if (startOffset != 0) {
      int rbyte;
      while ((rbyte = is.read()) != LF) {
        if(rbyte == -1) break;
      }
    }

    if (fragmentable() < 1) {
      close();
      return;
    }
    page();
  }

  private long fragmentable() throws IOException {
    return startOffset + length - getFilePosition();
  }

  @Override
  protected long getFilePosition() throws IOException {
    long retVal;
    if (filePosition != null) {
      retVal = filePosition.getPos();
    } else {
      retVal = fis.getPos();
    }
    return retVal;
  }

  private void page() throws IOException {
    // Index initialization
    currentIdx = 0;

    // Buffer size set
    if (isSplittable() && fragmentable() < DEFAULT_BUFFER_SIZE) {
      bufSize = (int) fragmentable();
    }

    if (this.tail == null || this.tail.length == 0) {
      this.pageStart = getFilePosition();
      this.prevTailLen = 0;
    } else {
      this.pageStart = getFilePosition() - this.tail.length;
      this.prevTailLen = this.tail.length;
    }

    // Read
    int rbyte;
    buf = new byte[bufSize];
    rbyte = is.read(buf);

    if (rbyte < 0) {
      eof = true; // EOF
      return;
    }

    if (prevTailLen == 0) {
      tail = new byte[0];
      tuples = StringUtils.split(new String(buf, 0, rbyte), (char) LF);
    } else {
      tuples = StringUtils.split(new String(tail)
          + new String(buf, 0, rbyte), (char) LF);
      tail = null;
    }

    // Check tail
    if ((char) buf[rbyte - 1] != LF) {
      if ((fragmentable() < 1 || rbyte != bufSize)) {
        int cnt = 0;
        byte[] temp = new byte[DEFAULT_BUFFER_SIZE];
        // Read bytes
        while ((temp[cnt] = (byte) is.read()) != LF) {
          cnt++;
        }

        // Replace tuple
        tuples[tuples.length - 1] = tuples[tuples.length - 1]
            + new String(temp, 0, cnt);
        validIdx = tuples.length;
      } else {
        tail = tuples[tuples.length - 1].getBytes();
        validIdx = tuples.length - 1;
      }
    } else {
      tail = new byte[0];
      validIdx = tuples.length;
    }

    if(!isCompress()) makeTupleOffset();
  }

  private void makeTupleOffset() {
    long curTupleOffset = 0;
    this.tupleOffsets = new long[this.validIdx];
    for (int i = 0; i < this.validIdx; i++) {
      this.tupleOffsets[i] = curTupleOffset + this.pageStart;
      curTupleOffset += this.tuples[i].getBytes().length + 1;// tuple byte
      // + 1byte
      // line feed
    }
  }

  protected Tuple getNextTuple() throws IOException {
    try {
      if (currentIdx == validIdx) {
        if (isSplittable() && fragmentable() < 1) {
          close();
          return null;
        } else {
          page();
        }

        if(eof){
          close();
          return null;
        }
      }

      long offset = -1;
      if(!isCompress()){
        offset = this.tupleOffsets[currentIdx];
      }

      String[] cells = StringUtils.splitPreserveAllTokens(tuples[currentIdx++], delimiter);

      int targetLen = targets.length;

      VTuple tuple = new VTuple(columnNum);
      Column field;
      tuple.setOffset(offset);
      for (int i = 0; i < targetLen; i++) {
        field = targets[i];
        int tid = targetColumnIndexes[i];
        if (cells.length <= tid) {
          tuple.put(tid, DatumFactory.createNullDatum());
        } else {
          String cell = cells[tid].trim();

          if (cell.equals("")) {
            tuple.put(tid, DatumFactory.createNullDatum());
          } else {
            switch (field.getDataType().getType()) {
              case BOOLEAN:
                tuple.put(tid, DatumFactory.createBool(cell));
                break;
              case BIT:
                tuple.put(tid, DatumFactory.createBit(Base64.decodeBase64(cell)[0]));
                break;
              case CHAR:
                String trimmed = cell.trim();
                tuple.put(tid, DatumFactory.createChar(trimmed));
                break;
              case BLOB:
                tuple.put(tid, DatumFactory.createBlob(Base64.decodeBase64(cell)));
                break;
              case INT2:
                tuple.put(tid, DatumFactory.createInt2(cell));
                break;
              case INT4:
                tuple.put(tid, DatumFactory.createInt4(cell));
                break;
              case INT8:
                tuple.put(tid, DatumFactory.createInt8(cell));
                break;
              case FLOAT4:
                tuple.put(tid, DatumFactory.createFloat4(cell));
                break;
              case FLOAT8:
                tuple.put(tid, DatumFactory.createFloat8(cell));
                break;
              case TEXT:
                tuple.put(tid, DatumFactory.createText(cell));
                break;
              case INET4:
                tuple.put(tid, DatumFactory.createInet4(cell));
                break;
              case ARRAY:
                Datum data = StorageGsonHelper.getInstance().fromJson(cell,
                    Datum.class);
                tuple.put(tid, data);
                break;
            }
          }
        }
      }
      return tuple;
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    return null;
  }

  private boolean isCompress() {
    return codec != null;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
  }

  @Override
  public void close() throws IOException {
    if(closed.get()) {
      return;
    }
    try {
      is.close();
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
      tuples = null;
      super.close();
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
  public void setSearchCondition(Object expr) {
  }

  @Override
  public boolean isSplittable(){
    return splittable;
  }

}
