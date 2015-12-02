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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreDecoder;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.index.IndexMethod;
import org.apache.tajo.storage.index.IndexWriter;
import org.apache.tajo.storage.index.OrderIndexReader;
import org.apache.tajo.unit.StorageUnit;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

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
    private FSDataOutputStream out;
    private FSDataOutputStream rootOut;
    private FileSystem fs;
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

    // private Tuple lastestKey = null;

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
      this.indexBuffer = Unpooled.buffer(BUFFER_SIZE);
      this.rootIndexBuffer = Unpooled.buffer(BUFFER_SIZE);
      this.loadCount = loadNum = conf.getInt(WRITER_INDEX_LOAD, DEFAULT_INDEX_LOAD);
    }

    public void setLoadNum(int loadNum) {
      this.loadNum = loadNum;
      this.loadCount = loadNum;
    }

    public void init() throws IOException {
      fs = fileName.getFileSystem(conf);
      if (fs.exists(fileName)) {
        throw new IOException("ERROR: index file (" + fileName + " already exists");
      }
      out = fs.create(fileName);

      if(writeRootIndex) {
        Path rootPath = new Path(fileName + ".root");
        if (fs.exists(rootPath)) {
          throw new IOException("ERROR: index file (" + rootPath + " already exists");
        }
        rootOut = fs.create(rootPath);
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
            byte[] buf = rowStoreEncoder.toBytes(keyTuple);
            int size = buf.length + 12;
            if (!rootIndexBuffer.isWritable(size)) {
              rootIndexBuffer.ensureWritable(size);
            }

            // key writing
            rootIndexBuffer.writeInt(buf.length);
            rootIndexBuffer.writeBytes(buf);

            // leaf offset writing
            rootIndexBuffer.writeLong(out.getPos() + indexBuffer.writerIndex());

            rootEntrySize++;
            if (rootIndexBuffer.writerIndex() >= BUFFER_SIZE) {
              rootIndexBuffer.readBytes(rootOut, rootIndexBuffer.readableBytes());
              rootIndexBuffer.clear();
            }
          }

          loadCount++;
        }

        /* leaf index writing */
        byte[] buf = rowStoreEncoder.toBytes(keyTuple);
        int size = buf.length + 16;
        if (!indexBuffer.isWritable(size)) {
          indexBuffer.ensureWritable(size);
        }

        // key writing
        indexBuffer.writeInt(buf.length);
        indexBuffer.writeBytes(buf);

        //offset num writing
        indexBuffer.writeInt(1);
        // offset writing
        indexBuffer.writeLong(offset);

        entrySize++;
        if (indexBuffer.writerIndex() >= BUFFER_SIZE) {
          indexBuffer.readBytes(out, indexBuffer.readableBytes());
          indexBuffer.clear();
        }
      } else {
        collector.put(keyTuple, offset);
      }
    }

    public TupleComparator getComparator() {
      return this.compartor;
    }

    public void flush() throws IOException {
      out.flush();
      if(rootOut != null) rootOut.flush();
    }

    public void writeFooter(int entryNum) throws IOException {
      indexBuffer.clear();
      long startPosition = out.getPos();
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

        size = minBytes.length + maxBytes.length + 8;
        if(!indexBuffer.isWritable(size)) {
          indexBuffer.readBytes(out,indexBuffer.readableBytes());
          indexBuffer.clear();
          indexBuffer.ensureWritable(size);
        }

        indexBuffer.writeInt(minBytes.length);
        indexBuffer.writeBytes(minBytes);
        indexBuffer.writeInt(maxBytes.length);
        indexBuffer.writeBytes(maxBytes);
      }
      // write buffer to file
      indexBuffer.readBytes(out, indexBuffer.readableBytes());
      indexBuffer.clear();
      int footerSize = (int) (out.getPos() -  startPosition) + 4;
      out.writeInt(footerSize);
    }

    public void close() throws IOException {
      /* data writing phase */
      try {
        if (sorted) {
          // write remaining data to file
          if (indexBuffer.readableBytes() > 0) {
            indexBuffer.readBytes(out, indexBuffer.readableBytes());
            indexBuffer.clear();
          }
        } else {
          // flush collected index data
          TreeMap<Tuple, LinkedList<Long>> keyOffsetMap = collector.getMap();
          entrySize = keyOffsetMap.size();
          for (Map.Entry<Tuple,LinkedList<Long>> entry : keyOffsetMap.entrySet()) {

            /* two level initialize */
            if (writeRootIndex) {
              if (loadCount == loadNum) {
                rootCollector.put(entry.getKey(), out.getPos());
                loadCount = 0;
              }
              loadCount++;
            }
            /* key writing */
            byte[] buf = rowStoreEncoder.toBytes(entry.getKey());
            out.writeInt(buf.length);
            out.write(buf);

            LinkedList<Long> offsetList = entry.getValue();
            /* offset num writing */
            int offsetSize = offsetList.size();
            out.writeInt(offsetSize);
            /* offset writing */
            for (Long offset : offsetList) {
              out.writeLong(offset);
            }
          }

          collector.clear();
        }
        writeFooter(entrySize);
      } finally {
        out.close();
      }

      /* root index creating phase */
      if (writeRootIndex) {
        try {
          if (sorted) {
            rootIndexBuffer.writeInt(loadNum);
            rootIndexBuffer.writeInt(rootEntrySize);

            // write remaining data to file
            if (rootIndexBuffer.readableBytes() > 0) {
              rootIndexBuffer.readBytes(rootOut, rootIndexBuffer.readableBytes());
              rootIndexBuffer.clear();
            }
          } else {
            TreeMap<Tuple, LinkedList<Long>> rootMap = rootCollector.getMap();
            rootEntrySize = rootMap.size();
            rootIndexBuffer.clear();

            /* root key writing */
            for (Map.Entry<Tuple, LinkedList<Long>> entry : rootMap.entrySet()) {
              byte[] buf = rowStoreEncoder.toBytes(entry.getKey());
              int size = buf.length + 12;
              if (!rootIndexBuffer.isWritable(size)) {
                rootIndexBuffer.ensureWritable(size);
              }

              rootIndexBuffer.writeInt(buf.length);
              rootIndexBuffer.writeBytes(buf);

              LinkedList<Long> offsetList = entry.getValue();
              if (offsetList.size() != 1) {
                throw new IOException("Why root index doen't have one offset? offsets:" + offsetList.size());
              }
              rootIndexBuffer.writeLong(offsetList.getFirst());

              if (rootIndexBuffer.writerIndex() >= BUFFER_SIZE) {
                rootIndexBuffer.readBytes(rootOut, rootIndexBuffer.readableBytes());
                rootIndexBuffer.clear();
              }
            }
            rootIndexBuffer.writeInt(this.loadNum);
            rootIndexBuffer.writeInt(rootEntrySize);

            if (rootIndexBuffer.readableBytes() > 0) {
              rootIndexBuffer.readBytes(rootOut, rootIndexBuffer.readableBytes());
              rootIndexBuffer.clear();
            }
            rootCollector.clear();
          }
        } finally {
          rootOut.close();
        }
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
    }

    public void init() throws IOException {
      open();
      fillData();
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

    @Override
    public void close() throws IOException {
      this.indexIn.close();
    }

    @Override
    public String toString() {
      return "BSTIndex (" + firstKey + ", " + lastKey + ") " + fileName;
    }
  }
}
