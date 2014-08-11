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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.LazyTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.BytesUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CSVFileScanner extends FileScannerV2 {
  public static final String DELIMITER = "csvfile.delimiter";
  public static final String DELIMITER_DEFAULT = "|";
  public static final byte LF = '\n';
  private static final Log LOG = LogFactory.getLog(CSVFileScanner.class);

  private final static int DEFAULT_BUFFER_SIZE = 256 * 1024;
  private int bufSize;
  private char delimiter;
  private ScheduledInputStream sin;
  private InputStream is; // decompressd stream
  private CompressionCodecFactory factory;
  private CompressionCodec codec;
  private Decompressor decompressor;
  private Seekable filePosition;
  private boolean splittable = true;
  private long startOffset, length;
  private byte[] buf = null;
  private byte[][] tuples = null;
  private long[] tupleOffsets = null;
  private int currentIdx = 0, validIdx = 0;
  private byte[] tail = null;
  private long pageStart = -1;
  private long prevTailLen = -1;
  private int[] targetColumnIndexes;
  private boolean eof = false;
  private boolean first = true;

  private long totalReadBytesForFetch;
  private long totalReadBytesFromDisk;

  public CSVFileScanner(Configuration conf, final Schema schema, final TableMeta meta, final FileFragment fragment)
      throws IOException {
    super(conf, meta, schema, fragment);
    factory = new CompressionCodecFactory(conf);
    codec = factory.getCodec(this.fragment.getPath());
    if (isCompress() && !(codec instanceof SplittableCompressionCodec)) {
      splittable = false;
    }
  }

  @Override
  public void init() throws IOException {
    // Buffer size, Delimiter
    this.bufSize = DEFAULT_BUFFER_SIZE;
    String delim  = meta.getOption(DELIMITER, DELIMITER_DEFAULT);
    this.delimiter = delim.charAt(0);

    super.init();
  }

  @Override
  protected boolean initFirstScan(int maxBytesPerSchedule) throws IOException {
    synchronized(this) {
      eof = false;
      first = true;
      if(sin == null) {
        FSDataInputStream fin = fs.open(fragment.getPath(), 128 * 1024);
        sin = new ScheduledInputStream(fragment.getPath(), fin,
            fragment.getStartKey(), fragment.getEndKey(), fs.getLength(fragment.getPath()));
        startOffset = fragment.getStartKey();
        length = fragment.getEndKey();

        if (startOffset > 0) {
          startOffset--; // prev line feed
        }
      }
    }
    return true;
  }

  private boolean scanFirst() throws IOException {
    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(
            sin, decompressor, startOffset, startOffset + length,
            SplittableCompressionCodec.READ_MODE.BYBLOCK);

        startOffset = cIn.getAdjustedStart();
        length = cIn.getAdjustedEnd() - startOffset;
        filePosition = cIn;
        is = cIn;
      } else {
        is = new DataInputStream(codec.createInputStream(sin, decompressor));
      }
    } else {
      sin.seek(startOffset);
      filePosition = sin;
      is = sin;
    }

    tuples = new byte[0][];
    if (targets == null) {
      targets = schema.toArray();
    }

    targetColumnIndexes = new int[targets.length];
    for (int i = 0; i < targets.length; i++) {
      targetColumnIndexes[i] = schema.getColumnId(targets[i].getQualifiedName());
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
      return false;
    }
    return true;
  }

  @Override
  public boolean isStopScanScheduling() {
    if(sin != null && sin.isEndOfStream()) {
      return true;
    } else {
      return false;
    }
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
      retVal = sin.getPos();
    }
    return retVal;
  }

  @Override
  public boolean isFetchProcessing() {
    if(sin != null &&
        (sin.getAvaliableSize() >= 64 * 1024 * 1024)) {
      return true;
    } else {
      return false;
    }
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
      tuples = BytesUtils.splitPreserveAllTokens(buf, rbyte, (char) LF);
    } else {
      byte[] lastRow = ArrayUtils.addAll(tail, buf);
      tuples = BytesUtils.splitPreserveAllTokens(lastRow, rbyte + tail.length, (char) LF);
      tail = null;
    }

    // Check tail
    if ((char) buf[rbyte - 1] != LF) {
      if ((fragmentable() < 1 || rbyte != bufSize)) {
        int lineFeedPos = 0;
        byte[] temp = new byte[DEFAULT_BUFFER_SIZE];

        // find line feed
        while ((temp[lineFeedPos] = (byte)is.read()) != (byte)LF) {
          if(temp[lineFeedPos] < 0) {
            break;
          }
          lineFeedPos++;
        }

        tuples[tuples.length - 1] = ArrayUtils.addAll(tuples[tuples.length - 1],
            ArrayUtils.subarray(temp, 0, lineFeedPos));
        validIdx = tuples.length;
      } else {
        tail = tuples[tuples.length - 1];
        validIdx = tuples.length - 1;
      }
    } else {
      tail = new byte[0];
      validIdx = tuples.length - 1;
    }

    if(!isCompress()) makeTupleOffset();
  }

  private void makeTupleOffset() {
    long curTupleOffset = 0;
    this.tupleOffsets = new long[this.validIdx];
    for (int i = 0; i < this.validIdx; i++) {
      this.tupleOffsets[i] = curTupleOffset + this.pageStart;
      curTupleOffset += this.tuples[i].length + 1;//tuple byte +  1byte line feed
    }
  }

  protected Tuple nextTuple() throws IOException {
    if(first) {
      boolean more = scanFirst();
      first = false;
      if(!more) {
        return null;
      }
    }
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

      byte[][] cells = BytesUtils.splitPreserveAllTokens(tuples[currentIdx++], delimiter, targetColumnIndexes);
      return new LazyTuple(schema, cells, offset);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    return null;
  }

  private boolean isCompress() {
    return codec != null;
  }

  @Override
  public void scannerReset() {
    if(sin != null) {
      try {
        filePosition.seek(0);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    if(sin != null) {
      try {
        sin.seek(0);
        sin.reset();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if(closed.get()) {
      return;
    }
    if(sin != null) {
      totalReadBytesForFetch = sin.getTotalReadBytesForFetch();
      totalReadBytesFromDisk = sin.getTotalReadBytesFromDisk();
    }
    try {
      if(is != null) {
        is.close();
      }
      is = null;
      sin = null;
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
  protected boolean scanNext(int length) throws IOException {
    synchronized(this) {
      if(isClosed()) {
        return false;
      }
      return sin.readNext(length);
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

  @Override
  protected long[] reportReadBytes() {
    return new long[]{totalReadBytesForFetch, totalReadBytesFromDisk};
  }
}
