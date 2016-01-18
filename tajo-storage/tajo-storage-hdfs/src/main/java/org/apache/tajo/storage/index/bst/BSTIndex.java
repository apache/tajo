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

package org.apache.tajo.storage.index.bst;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreDecoder;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.index.IndexMethod;
import org.apache.tajo.storage.index.IndexWriter;
import org.apache.tajo.storage.index.OrderIndexReader;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;

import java.io.*;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static org.apache.tajo.index.IndexProtos.TupleComparatorProto;

/**
 * This is two-level binary search tree index. This is one of the value-list 
 * index structure. Thus, it is inefficient in the case where 
 * the many of the values are same. Also, the BST shows the fast performance 
 * when the selectivity of rows to be retrieved is less than 5%.
 * BSTIndexWriter is not thread-safe, whereas BSTIndexReader is thread-safe.
 */
public class BSTIndex implements IndexMethod {
  private static final Log LOG = LogFactory.getLog(BSTIndex.class);

  public static final int ONE_LEVEL_INDEX = 1;
  public static final int TWO_LEVEL_INDEX = 2;
  public static final int DEFAULT_INDEX_LOAD = 4096;
  public static final int BUFFER_SIZE = 128 * StorageUnit.KB;
  public static final String WRITER_INDEX_LOAD = "tajo.executor.index.writer.load-num";

  private final Configuration conf;

  public BSTIndex(final Configuration conf) {
    this.conf = conf;
  }
  

  public BSTIndexWriter getIndexWriter(final Path fileName, int level, Schema keySchema,
      TupleComparator comparator, boolean sorted) throws IOException {
    return new BSTIndexWriter(fileName, level, keySchema, comparator, sorted);
  }

  @Override
  public BSTIndexWriter getIndexWriter(final Path fileName, int level, Schema keySchema,
                                       TupleComparator comparator) throws IOException {
    return getIndexWriter(fileName, level, keySchema, comparator, false);
  }

  @Override
  public BSTIndexReader getIndexReader(Path fileName, Schema keySchema, TupleComparator comparator) throws IOException {
    return new BSTIndexReader(fileName, keySchema, comparator);
  }

  public BSTIndexReader getIndexReader(Path fileName) throws IOException {
    return new BSTIndexReader(fileName);
  }

  public class BSTIndexWriter extends IndexWriter implements Closeable {
    private FileChannel outChannel;
    private RandomAccessFile outRandomAccessFile;
    private FSDataOutputStream out;
    private long filePos;

    private FileChannel rootOutChannel;
    private RandomAccessFile rootOutRandomAccessFile;
    private FSDataOutputStream rootOut;

    private boolean isLocal;

    private int level;
    private int loadNum;
    private Path fileName;
    // Target data set is sorted or not
    private boolean sorted;
    private boolean writeRootIndex;

    private final Schema keySchema;
    private final TupleComparator compartor;
    private final KeyOffsetCollector collector;
    private KeyOffsetCollector rootCollector;
    private ByteBuf indexBuffer;
    private ByteBuf rootIndexBuffer;

    private Tuple firstKey;
    private Tuple lastKey;

    private RowStoreEncoder rowStoreEncoder;
    private int loadCount;
    private int entrySize;
    private int rootEntrySize;

    /**
     * constructor
     *
     * @param level
     *          : IndexCreater.ONE_LEVEL_INDEX or IndexCreater.TWO_LEVEL_INDEX
     * @throws java.io.IOException
     */
    public BSTIndexWriter(final Path fileName, int level, Schema keySchema,
        TupleComparator comparator, boolean sorted) throws IOException {
      this.fileName = fileName;
      this.level = level;
      this.writeRootIndex = level == TWO_LEVEL_INDEX;
      this.keySchema = keySchema;
      this.compartor = comparator;
      this.collector = new KeyOffsetCollector(comparator);
      this.rootCollector = new KeyOffsetCollector(this.compartor);
      this.rowStoreEncoder = RowStoreUtil.createEncoder(keySchema);
      this.sorted = sorted;
      this.indexBuffer = BufferPool.directBuffer(BUFFER_SIZE, ByteOrder.nativeOrder());
      this.rootIndexBuffer = BufferPool.directBuffer(BUFFER_SIZE, ByteOrder.nativeOrder());
      this.loadCount = loadNum = conf.getInt(WRITER_INDEX_LOAD, DEFAULT_INDEX_LOAD);
    }

    public void setLoadNum(int loadNum) {
      this.loadNum = loadNum;
      this.loadCount = loadNum;
    }

    public void init() throws IOException {
      FileSystem fs = fileName.getFileSystem(conf);
      Path rootPath = new Path(fileName + ".root");
      if (fs.exists(fileName) || fs.exists(rootPath)) {
        throw new IOException("ERROR: index file " + fileName + " or " + rootPath + " already exists");
      }

      if (fs instanceof LocalFileSystem) {
        File outFile;
        try {
          if (!fs.exists(fileName.getParent())) {
            fs.mkdirs(fileName.getParent());
          }

          if (fileName.toUri().getScheme() != null) {
            outFile = new File(fileName.toUri());
          } else {
            outFile = new File(fileName.toString());
          }
        } catch (IllegalArgumentException iae) {
          throw new IOException(iae);
        }

        outRandomAccessFile = new RandomAccessFile(outFile, "rw");
        outChannel = outRandomAccessFile.getChannel();

        if (writeRootIndex) {
          rootOutRandomAccessFile = new RandomAccessFile(new File(outFile.getAbsolutePath() + ".root"), "rw");
          rootOutChannel = rootOutRandomAccessFile.getChannel();
        }
        isLocal = true;
      } else {
        out = fs.create(fileName, true);
        if (writeRootIndex) {
          rootOut = fs.create(rootPath, true);
        }
      }
    }

    @Override
    public void write(final Tuple key, final long offset) throws IOException {
      Tuple keyTuple;
      try {
        keyTuple = key.clone();
      } catch (CloneNotSupportedException e) {
        throw new IOException(e);
      }
      if (firstKey == null || compartor.compare(keyTuple, firstKey) < 0) {
        firstKey = keyTuple;
      }
      if (lastKey == null || compartor.compare(lastKey, keyTuple) < 0) {
        lastKey = keyTuple;
      }

      if (sorted) {
         /* root index writing */
        if (writeRootIndex) {
          if (loadCount == loadNum) {
            loadCount = 0;
            writeRootIndex(rootIndexBuffer, keyTuple, filePos + indexBuffer.writerIndex());
          }
          loadCount++;
        }

        /* leaf index writing */
        writeIndex(indexBuffer, keyTuple, offset);
      } else {
        collector.put(keyTuple, offset);
      }
    }

    private void writeIndex(ByteBuf byteBuf, Tuple tuple, Long... offsets) throws IOException {

      byte[] buf = rowStoreEncoder.toBytes(tuple);
      int size = buf.length + 8 + (offsets.length * 8);
      if (!byteBuf.isWritable(size)) {
        byteBuf.ensureWritable(size);
      }

      // key writing
      byteBuf.writeInt(buf.length);
      byteBuf.writeBytes(buf);

      //offset num writing
      byteBuf.writeInt(offsets.length);

      /* offset writing */
      for (long offset : offsets) {
        byteBuf.writeLong(offset);
      }

      entrySize++;
      // flush to file and reset buffer
      if (byteBuf.writerIndex() >= BUFFER_SIZE) {
        filePos += flushBuffer(byteBuf, outChannel, out);
      }
    }

    private void writeRootIndex(ByteBuf byteBuf, Tuple tuple, long offset) throws IOException {
      byte[] buf = rowStoreEncoder.toBytes(tuple);
      int size = buf.length + 12;
      if (!byteBuf.isWritable(size)) {
        byteBuf.ensureWritable(size);
      }

      // key writing
      byteBuf.writeInt(buf.length);
      byteBuf.writeBytes(buf);

      // leaf offset writing
      byteBuf.writeLong(offset);

      rootEntrySize++;
      // flush to file and reset buffer
      if (byteBuf.writerIndex() >= BUFFER_SIZE) {
        flushBuffer(byteBuf, rootOutChannel, rootOut);
      }
    }

    private int flushBuffer(ByteBuf byteBuf, FileChannel channel, FSDataOutputStream out) throws IOException {
      // write buffer to file
      int readableBytes = byteBuf.readableBytes();
      if (readableBytes > 0) {
        if (isLocal) {
          byteBuf.readBytes(channel, readableBytes);
        } else {
          byteBuf.readBytes(out, readableBytes);
        }
        byteBuf.clear();
      }
      return readableBytes;
    }

    public TupleComparator getComparator() {
      return this.compartor;
    }

    public void flush() throws IOException {
      if (out != null) {
        flushBuffer(indexBuffer, outChannel, out);
        out.flush();
      }

      if (writeRootIndex && rootOut != null) {
        flushBuffer(rootIndexBuffer, rootOutChannel, rootOut);
        rootOut.flush();
      }
    }

    public void writeFooter(int entryNum) throws IOException {
      indexBuffer.clear();

      long startPosition = filePos;
      // schema
      byte [] schemaBytes = keySchema.getProto().toByteArray();
      // comparator
      byte [] comparatorBytes = compartor.getProto().toByteArray();

      int size = schemaBytes.length + comparatorBytes.length + 16;
      if(!indexBuffer.isWritable(size)) {
        indexBuffer.ensureWritable(size);
      }

      indexBuffer.writeInt(schemaBytes.length);
      indexBuffer.writeBytes(schemaBytes);

      indexBuffer.writeInt(comparatorBytes.length);
      indexBuffer.writeBytes(comparatorBytes);

      // level
      indexBuffer.writeInt(this.level);
      // entry
      indexBuffer.writeInt(entryNum);
      if (entryNum > 0) {
        byte [] minBytes = rowStoreEncoder.toBytes(firstKey);
        byte [] maxBytes = rowStoreEncoder.toBytes(lastKey);

        size = minBytes.length + maxBytes.length + 12;
        if(!indexBuffer.isWritable(size)) {
          filePos += flushBuffer(indexBuffer, outChannel, out);
          indexBuffer.ensureWritable(size);
        }

        indexBuffer.writeInt(minBytes.length);
        indexBuffer.writeBytes(minBytes);
        indexBuffer.writeInt(maxBytes.length);
        indexBuffer.writeBytes(maxBytes);
      }

      // write footer length
      int footerSize = (int) (filePos + indexBuffer.readableBytes() + 4 - startPosition);
      indexBuffer.writeInt(footerSize);

      filePos += flushBuffer(indexBuffer, outChannel, out);
    }

    public void close() throws IOException {
      /* data writing phase */
      try {
        if (sorted) {
          // write remaining data to file
          filePos += flushBuffer(indexBuffer, outChannel, out);
        } else {
          // flush collected index data
          TreeMap<Tuple, LinkedList<Long>> keyOffsetMap = collector.getMap();
          for (Map.Entry<Tuple, LinkedList<Long>> entry : keyOffsetMap.entrySet()) {

            /* two level initialize */
            if (writeRootIndex) {
              if (loadCount == loadNum) {
                loadCount = 0;
                rootCollector.put(entry.getKey(), filePos + indexBuffer.writerIndex());
              }
              loadCount++;
            }

            LinkedList<Long> offsetList = entry.getValue();
            writeIndex(indexBuffer, entry.getKey(), offsetList.toArray(new Long[offsetList.size()]));
          }
          filePos += flushBuffer(indexBuffer, outChannel, out);
          collector.clear();
        }

        writeFooter(entrySize);

        /* root index creating phase */
        if (writeRootIndex) {
          if (sorted) {
            //write root index header
            rootIndexBuffer.writeInt(loadNum);
            rootIndexBuffer.writeInt(rootEntrySize);

            // write remaining data to file
            flushBuffer(rootIndexBuffer, rootOutChannel, rootOut);
          } else {
            TreeMap<Tuple, LinkedList<Long>> rootMap = rootCollector.getMap();
            rootIndexBuffer.clear();
            /* root key writing */
            for (Map.Entry<Tuple, LinkedList<Long>> entry : rootMap.entrySet()) {
              LinkedList<Long> offsetList = entry.getValue();
              if (offsetList.size() != 1) {
                throw new IOException("Why root index doen't have one offset? offsets:" + offsetList.size());
              }
              writeRootIndex(rootIndexBuffer, entry.getKey(), offsetList.getFirst());
            }

            //write root index header
            rootIndexBuffer.writeInt(this.loadNum);
            rootIndexBuffer.writeInt(rootEntrySize);

            flushBuffer(rootIndexBuffer, rootOutChannel, rootOut);
            rootCollector.clear();
          }
        }
      } finally {
        indexBuffer.release();
        rootIndexBuffer.release();

        FileUtil.cleanupAndthrowIfFailed(outChannel, outRandomAccessFile, out,
            rootOutChannel, rootOutRandomAccessFile, rootOut);
      }
    }

    private class KeyOffsetCollector {
      private TreeMap<Tuple, LinkedList<Long>> map;

      public KeyOffsetCollector(TupleComparator comparator) {
        map = new TreeMap<>(comparator);
      }

      public void put(final Tuple key, final long offset) {
        if (map.containsKey(key)) {
          map.get(key).add(offset);
        } else {
          LinkedList<Long> list = new LinkedList<>();
          list.add(offset);
          map.put(key, list);
        }
      }

      public TreeMap<Tuple, LinkedList<Long>> getMap() {
        return this.map;
      }

      public void clear() {
        this.map.clear();
      }
    }
  }

  private static final AtomicIntegerFieldUpdater<BSTIndexReader> REFERENCE_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(BSTIndexReader.class, "referenceNum");

  /**
   * BSTIndexReader is thread-safe.
   */
  public class BSTIndexReader implements OrderIndexReader , Closeable{
    private Path fileName;
    private Schema keySchema;
    private TupleComparator comparator;

    private FileSystem fs;
    private FSDataInputStream indexIn;

    private int level;
    private int entryNum;
    private int loadNum = -1;
    private Tuple firstKey;
    private Tuple lastKey;

    // the cursors of BST
    private int rootCursor;
    private int keyCursor;
    private int offsetCursor;
    private long dataLength;

    // mutex
    private final Object mutex = new Object();

    private RowStoreDecoder rowStoreDecoder;

    private AtomicBoolean inited = new AtomicBoolean(false);

    volatile int referenceNum;

    /**
     *
     * @param fileName
     * @param keySchema
     * @param comparator
     * @throws java.io.IOException
     */
    public BSTIndexReader(final Path fileName, Schema keySchema, TupleComparator comparator) throws IOException {
      this.fileName = fileName;
      this.keySchema = keySchema;
      this.comparator = comparator;
      this.rowStoreDecoder = RowStoreUtil.createDecoder(keySchema);
      open();
    }

    public BSTIndexReader(final Path fileName) throws IOException {
      this.fileName = fileName;
      open();
    }

    /**
     * Increase the reference number of the index reader.
     */
    public void retain() {
      REFERENCE_UPDATER.compareAndSet(this, referenceNum, referenceNum + 1);
    }

    /**
     * Decrease the reference number of the index reader.
     * This method must be called before {@link #close()}.
     */
    public void release() {
      REFERENCE_UPDATER.compareAndSet(this, referenceNum, referenceNum - 1);
    }

    public int getReferenceNum() {
      return referenceNum;
    }

    public Schema getKeySchema() {
      return this.keySchema;
    }

    public TupleComparator getComparator() {
      return this.comparator;
    }

    private void loadFooter() throws IOException {
      long fileLength = fs.getFileStatus(this.fileName).getLen();

      //read footer
      indexIn.seek(fileLength - 4);
      int footerSize = indexIn.readInt();
      dataLength = fileLength - footerSize;
      ByteBuf byteBuf = Unpooled.buffer(footerSize, footerSize);
      indexIn.seek(dataLength);
      byteBuf.writeBytes(indexIn, footerSize);

      // schema
      int schemaByteSize = byteBuf.readInt();
      byte [] schemaBytes = new byte[schemaByteSize];
      byteBuf.readBytes(schemaBytes);

      SchemaProto.Builder builder = SchemaProto.newBuilder();
      builder.mergeFrom(schemaBytes);
      SchemaProto proto = builder.build();
      this.keySchema = new Schema(proto);
      this.rowStoreDecoder = RowStoreUtil.createDecoder(keySchema);

      // comparator
      int compByteSize = byteBuf.readInt();
      byte [] compBytes = new byte[compByteSize];
      byteBuf.readBytes(compBytes);

      TupleComparatorProto.Builder compProto = TupleComparatorProto.newBuilder();
      compProto.mergeFrom(compBytes);
      this.comparator = new BaseTupleComparator(compProto.build());

      // level
      this.level = byteBuf.readInt();
      // entry
      this.entryNum = byteBuf.readInt();
      if (entryNum > 0) { // if there is no any entry, do not read firstKey/lastKey values
        byte [] minBytes = new byte[byteBuf.readInt()];
        byteBuf.readBytes(minBytes);
        this.firstKey = rowStoreDecoder.toTuple(minBytes);

        byte [] maxBytes = new byte[byteBuf.readInt()];
        byteBuf.readBytes(maxBytes);
        this.lastKey = rowStoreDecoder.toTuple(maxBytes);
      }
      byteBuf.release();
    }

    public synchronized void init() throws IOException {
      if (inited.compareAndSet(false, true)) {
        fillData();
      }
    }

    private void open()
        throws IOException {
      /* init the index file */
      fs = fileName.getFileSystem(conf);
      if (!fs.exists(fileName)) {
        throw new FileNotFoundException("ERROR: does not exist " + fileName.toString());
      }

      indexIn = fs.open(this.fileName);
      loadFooter();
    }

    private void fillData() throws IOException {
      indexIn.seek(0);
      /* load on memory */
      if (this.level == TWO_LEVEL_INDEX) {

        Path rootPath = new Path(this.fileName + ".root");
        if (!fs.exists(rootPath)) {
          throw new FileNotFoundException("root index did not created");
        }

        try (FSDataInputStream rootIndexIn = fs.open(rootPath)) {
          long fileLength = fs.getFileStatus(rootPath).getLen();
        /* root index header reading : type => loadNum => indexSize */
          rootIndexIn.seek(fileLength - 8);
          this.loadNum = rootIndexIn.readInt();
          this.entryNum = rootIndexIn.readInt();

          rootIndexIn.seek(0);
          fillRootIndex(entryNum, rootIndexIn);
        }

      } else {
        fillLeafIndex(entryNum, indexIn, -1);
      }
    }

    /**
     *
     * @return
     * @throws java.io.IOException
     */
    public long find(Tuple key) throws IOException {
      return find(key, false);
    }

    @Override
    public long find(Tuple key, boolean nextKey) throws IOException {
      synchronized (mutex) {
        int pos = -1;
        if (this.level == ONE_LEVEL_INDEX) {
            pos = oneLevBS(key);
        } else if (this.level == TWO_LEVEL_INDEX) {
            pos = twoLevBS(key, this.loadNum + 1);
        } else {
          throw new IOException("More than TWL_LEVEL_INDEX is not supported.");
        }

        if (nextKey) {
          if (pos + 1 >= this.offsetSubIndex.length) {
            return -1;
          }
          keyCursor = pos + 1;
          offsetCursor = 0;
        } else {
          if (correctable) {
            keyCursor = pos;
            offsetCursor = 0;
          } else {
            return -1;
          }
        }

        return this.offsetSubIndex[keyCursor][offsetCursor];
      }
    }

    public long next() throws IOException {
      synchronized (mutex) {
        if (offsetSubIndex[keyCursor].length - 1 > offsetCursor) {
          offsetCursor++;
        } else {
          if (offsetSubIndex.length - 1 > keyCursor) {
            keyCursor++;
            offsetCursor = 0;
          } else {
            if (offsetIndex.length -1 > rootCursor) {
              rootCursor++;
              fillLeafIndex(loadNum + 1, indexIn, this.offsetIndex[rootCursor]);
              keyCursor = 1;
              offsetCursor = 0;
            } else {
              return -1;
            }
          }
        }

        return this.offsetSubIndex[keyCursor][offsetCursor];
      }
    }
    
    public boolean isCurInMemory() {
      return (offsetSubIndex[keyCursor].length - 1 >= offsetCursor);
    }

    private void fillLeafIndex(int entryNum, FSDataInputStream in, long pos)
        throws IOException {
      int counter = 0;
      try {
        if (pos != -1) {
          in.seek(pos);
        }
        this.dataSubIndex = new Tuple[entryNum];
        this.offsetSubIndex = new long[entryNum][];

        byte[] buf;
        for (int i = 0; i < entryNum; i++) {
          counter++;

          if (in.getPos() >= dataLength)
            throw new EOFException("Path:" + fileName + ", Pos: " + in.getPos() + ", Data len:" + dataLength);

          buf = new byte[in.readInt()];
          StorageUtil.readFully(in, buf, 0, buf.length);
          dataSubIndex[i] = rowStoreDecoder.toTuple(buf);

          int offsetNum = in.readInt();
          this.offsetSubIndex[i] = new long[offsetNum];
          for (int j = 0; j < offsetNum; j++) {
            this.offsetSubIndex[i][j] = in.readLong();
          }
        }

      } catch (IOException e) {
        //TODO this block should fix correctly
        counter--;
        if (counter == 0)
          LOG.info("counter: " + counter);
        if (pos != -1) {
          in.seek(pos);
        }
        this.dataSubIndex = new Tuple[counter];
        this.offsetSubIndex = new long[counter][];

        byte[] buf;
        for (int i = 0; i < counter; i++) {
          buf = new byte[in.readInt()];
          StorageUtil.readFully(in, buf, 0, buf.length);
          dataSubIndex[i] = rowStoreDecoder.toTuple(buf);

          int offsetNum = in.readInt();
          this.offsetSubIndex[i] = new long[offsetNum];
          for (int j = 0; j < offsetNum; j++) {
            this.offsetSubIndex[i][j] = in.readLong();
          }

        }
      }
    }

    public Tuple getFirstKey() {
      return this.firstKey;
    }

    public Tuple getLastKey() {
      return this.lastKey;
    }

    private void fillRootIndex(int entryNum, FSDataInputStream in)
        throws IOException {
      this.dataIndex = new Tuple[entryNum];
      this.offsetIndex = new long[entryNum];
      Tuple keyTuple;
      byte[] buf;
      for (int i = 0; i < entryNum; i++) {
        buf = new byte[in.readInt()];
        StorageUtil.readFully(in, buf, 0, buf.length);
        keyTuple = rowStoreDecoder.toTuple(buf);
        dataIndex[i] = keyTuple;
        this.offsetIndex[i] = in.readLong();
      }
    }

    /* memory index, only one is used. */
    private Tuple[] dataIndex = null;
    private Tuple[] dataSubIndex = null;

    /* offset index */
    private long[] offsetIndex = null;
    private long[][] offsetSubIndex = null;

    private boolean correctable = true;

    private int oneLevBS(Tuple key) throws IOException {
      correctable = true;
      int pos = binarySearch(this.dataSubIndex, key, 0, this.dataSubIndex.length);
      return pos;
    }

    private int twoLevBS(Tuple key, int loadNum) throws IOException {
      int pos = binarySearch(this.dataIndex, key, 0, this.dataIndex.length);
      if(pos > 0) {
        rootCursor = pos;
      } else {
        rootCursor = 0;
      }
      fillLeafIndex(loadNum, indexIn, this.offsetIndex[rootCursor]);
      pos = binarySearch(this.dataSubIndex, key, 0, this.dataSubIndex.length);

      return pos;
    }

    private int binarySearch(Tuple[] arr, Tuple key, int startPos, int endPos) {
      int offset = -1;
      int start = startPos;
      int end = endPos;

      //http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6412541
      int centerPos = (start + end) >>> 1;
      if (arr.length == 0) {
        LOG.error("arr.length: 0, loadNum: " + loadNum + ", inited: " + inited.get());
      }
      while (true) {
        if (comparator.compare(arr[centerPos], key) > 0) {
          if (centerPos == 0) {
            correctable = false;
            break;
          } else if (comparator.compare(arr[centerPos - 1], key) < 0) {
            correctable = false;
            offset = centerPos - 1;
            break;
          } else {
            end = centerPos;
            centerPos = (start + end) / 2;
          }
        } else if (comparator.compare(arr[centerPos], key) < 0) {
          if (centerPos == arr.length - 1) {
            correctable = false;
            offset = centerPos;
            break;
          } else if (comparator.compare(arr[centerPos + 1], key) > 0) {
            correctable = false;
            offset = centerPos;
            break;
          } else {
            start = centerPos + 1;
            centerPos = (start + end) / 2;
          }
        } else {
          correctable = true;
          offset = centerPos;
          break;
        }
      }
      return offset;
    }

    /**
     * Close index reader only when it is not used anymore.
     */
    @Override
    public void close() throws IOException {
      if (referenceNum == 0) {
        this.indexIn.close();
      }
    }

    /**
     * Close index reader even though it is being used.
     *
     * @throws IOException
     */
    public void forceClose() throws IOException {
      REFERENCE_UPDATER.compareAndSet(this, referenceNum, 0);
      this.indexIn.close();
    }

    @Override
    public String toString() {
      return "BSTIndex (" + firstKey + ", " + lastKey + ") " + fileName;
    }
  }
}
