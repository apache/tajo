package tajo.index.bst;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Schema;
import tajo.engine.planner.physical.TupleComparator;
import tajo.index.IndexMethod;
import tajo.index.IndexWriter;
import tajo.index.OrderIndexReader;
import tajo.storage.RowStoreUtil;
import tajo.storage.Tuple;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author Ryu Hyo Seok
 * @author Hyunsik Choi
 * 
 * This is two-level binary search tree index. This is one of the value-list 
 * index structure. Thus, it is inefficient in the case where 
 * the many of the values are same. Also, the BST shows the fast performance 
 * when the selectivity of rows to be retrieved is less than 5%.
 * BSTIndexWriter is not thread-safe, whereas BSTIndexReader is thread-safe.
 *
 */
public class BSTIndex implements IndexMethod {
  private static final Log LOG = LogFactory.getLog(BSTIndex.class);

  public static final int ONE_LEVEL_INDEX = 1;
  public static final int TWO_LEVEL_INDEX = 2;

  private final Configuration conf;

  public BSTIndex(final Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public BSTIndexWriter getIndexWriter(final Path fileName, int level, Schema keySchema,
      TupleComparator comparator) throws IOException {
    return new BSTIndexWriter(fileName, level, keySchema, comparator);
  }

  @Override
  public BSTIndexReader getIndexReader(Path fileName, Schema keySchema,
      TupleComparator comparator) throws IOException {
    return new BSTIndexReader(fileName, keySchema, comparator);
  }

  public class BSTIndexWriter extends IndexWriter implements Closeable {
    private FSDataOutputStream out;
    private FileSystem fs;
    private int level;
    private int loadNum = 4096;
    private Path fileName;

    private final Schema keySchema;
    private final TupleComparator compartor;
    private final KeyOffsetCollector collector;
    private KeyOffsetCollector rootCollector;

    private Tuple min;
    private Tuple max;


    // private Tuple lastestKey = null;

    /**
     * constructor
     *
     * @param level
     *          : IndexCreater.ONE_LEVEL_INDEX or IndexCreater.TWO_LEVEL_INDEX
     * @throws IOException
     */
    public BSTIndexWriter(final Path fileName, int level, Schema keySchema,
        TupleComparator comparator) throws IOException {
      this.fileName = fileName;
      this.level = level;
      this.keySchema = keySchema;
      this.compartor = comparator;
      this.collector = new KeyOffsetCollector(comparator);
    }

   public void setLoadNum(int loadNum) {
      this.loadNum = loadNum;
    }

    public void open() throws IOException {
      fs = fileName.getFileSystem(conf);
      if (fs.exists(fileName)) {
        throw new IOException("ERROR: index file (" + fileName + " already exists");
      }
      out = fs.create(fileName);
    }

    @Override
    public void write(Tuple key, long offset) throws IOException {
      if (min == null || compartor.compare(key, min) < 0) {
        min = key;
      }
      if (max == null || compartor.compare(max, key) < 0) {
        max = key;
      }

      collector.put(key, offset);
    }

    public Tuple getMin() {
      return this.min;
    }

    public Tuple getMax() {
      return this.max;
    }

    public TupleComparator getCompartor() {
      return this.compartor;
    }

    public void flush() throws IOException {
      out.flush();
    }

    public void close() throws IOException {
      /* two level initialize */
      if (this.level == TWO_LEVEL_INDEX) {
        rootCollector = new KeyOffsetCollector(this.compartor);
      }

      /* data writing phase */
      TreeMap<Tuple, LinkedList<Long>> keyOffsetMap = collector.getMap();
      Set<Tuple> keySet = keyOffsetMap.keySet();

      int entryNum = keySet.size();

      // header write => type = > level => entryNum
      out.writeInt(this.level);
      out.writeInt(entryNum);
      if (entryNum > 0) {
        byte [] minBytes = RowStoreUtil.RowStoreEncoder.toBytes(keySchema, min);
        out.writeInt(minBytes.length);
        out.write(minBytes);
        byte [] maxBytes = RowStoreUtil.RowStoreEncoder.toBytes(keySchema, max);
        out.writeInt(maxBytes.length);
        out.write(maxBytes);
      }
      out.flush();
      
      int loadCount = this.loadNum - 1;
      for (Tuple key : keySet) {

        if (this.level == TWO_LEVEL_INDEX) {
          loadCount++;
          if (loadCount == this.loadNum) {
            rootCollector.put(key, out.getPos());
            loadCount = 0;
          }
        }
        /* key writing */
        byte[] buf = RowStoreUtil.RowStoreEncoder.toBytes(this.keySchema, key);
        out.writeInt(buf.length);
        out.write(buf);
        
        /**/
        LinkedList<Long> offsetList = keyOffsetMap.get(key);
        /* offset num writing */
        int offsetSize = offsetList.size();
        out.writeInt(offsetSize);
        /* offset writing */
        for (Long offset : offsetList) {
          out.writeLong(offset);
        }
      }

      out.flush();
      out.close();
      keySet.clear();
      collector.clear();

      FSDataOutputStream rootOut = null;
      /* root index creating phase */
      if (this.level == TWO_LEVEL_INDEX) {
        TreeMap<Tuple, LinkedList<Long>> rootMap = rootCollector.getMap();
        keySet = rootMap.keySet();

        rootOut = fs.create(new Path(fileName + ".root"));
        rootOut.writeInt(this.loadNum);
        rootOut.writeInt(keySet.size());

        /* root key writing */
        for (Tuple key : keySet) {
          byte[] buf = RowStoreUtil.RowStoreEncoder.toBytes(keySchema, key);
          rootOut.writeInt(buf.length);
          rootOut.write(buf);

          LinkedList<Long> offsetList = rootMap.get(key);
          if (offsetList.size() > 1 || offsetList.size() == 0) {
            throw new IOException("Why root index doen't have one offset?");
          }
          rootOut.writeLong(offsetList.getFirst());

        }
        rootOut.flush();
        rootOut.close();

        keySet.clear();
        rootCollector.clear();
      }

      if (out != null) {
        out = null;
      }
      if (rootOut != null) {
        rootOut = null;
      }
    }

    private class KeyOffsetCollector {
      private TreeMap<Tuple, LinkedList<Long>> map;

      public KeyOffsetCollector(TupleComparator comparator) {
        map = new TreeMap<Tuple, LinkedList<Long>>(comparator);
      }

      public void put(Tuple key, long offset) {
        if (map.containsKey(key)) {
          map.get(key).add(offset);
        } else {
          LinkedList<Long> list = new LinkedList<Long>();
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
    private final Schema keySchema;
    private final TupleComparator comparator;

    private FileSystem fs;
    private FSDataInputStream indexIn;
    private FSDataInputStream subIn;

    private int level;
    private int entryNum;
    private int loadNum = -1;
    private Tuple min;
    private Tuple max;

    // the cursors of BST
    private int rootCursor;
    private int keyCursor;
    private int offsetCursor;

    // mutex
    private final Integer mutex = 0;

    /**
     *
     * @param fileName
     * @param keySchema
     * @param comparator
     * @throws IOException
     */
    public BSTIndexReader(final Path fileName, Schema keySchema, TupleComparator comparator) throws IOException {
      this.fileName = fileName;
      this.keySchema = keySchema;
      this.comparator = comparator;
    }

    public void open()
        throws IOException {
      /* open the index file */
      fs = fileName.getFileSystem(conf);
      if (!fs.exists(fileName)) {
        throw new FileNotFoundException("ERROR: does not exist " + fileName.toString());
      }

      indexIn = fs.open(this.fileName);
      this.level = indexIn.readInt();
      this.entryNum = indexIn.readInt();
      if (entryNum > 0) { // if there is no any entry, do not read min/max values
        byte [] minBytes = new byte[indexIn.readInt()];
        indexIn.read(minBytes);
        this.min = RowStoreUtil.RowStoreDecoder.toTuple(keySchema, minBytes);
        byte [] maxBytes = new byte[indexIn.readInt()];
        indexIn.read(maxBytes);
        this.max = RowStoreUtil.RowStoreDecoder.toTuple(keySchema, maxBytes);
      }

      fillData();
    }

    private void fillData() throws IOException {
      /* load on memory */
      if (this.level == TWO_LEVEL_INDEX) {

        Path rootPath = new Path(this.fileName + ".root");
        if (!fs.exists(rootPath)) {
          throw new FileNotFoundException("root index did not created");
        }

        subIn = indexIn;
        indexIn = fs.open(rootPath);
        /* root index header reading : type => loadNum => indexSize */
        this.loadNum = indexIn.readInt();
        this.entryNum = indexIn.readInt();
        /**/
        fillRootIndex(entryNum, indexIn);

      } else {
        fillLeafIndex(entryNum, indexIn, -1);
      }
    }

    /**
     *
     * @return
     * @throws IOException
     */
    public long find(Tuple key) throws IOException {
      return find(key, false);
    }

    @Override
    public long find(Tuple key, boolean nextKey) throws IOException {
      synchronized (mutex) {
        int pos = -1;
        switch (this.level) {
          case ONE_LEVEL_INDEX:
            pos = oneLevBS(key);
            break;
          case TWO_LEVEL_INDEX:
            pos = twoLevBS(key, this.loadNum + 1);
            break;
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
              fillLeafIndex(loadNum + 1, subIn, this.offsetIndex[rootCursor]);
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
          buf = new byte[in.readInt()];
          in.read(buf);
          dataSubIndex[i] = RowStoreUtil.RowStoreDecoder.toTuple(keySchema, buf);

          int offsetNum = in.readInt();
          this.offsetSubIndex[i] = new long[offsetNum];
          for (int j = 0; j < offsetNum; j++) {
            this.offsetSubIndex[i][j] = in.readLong();
          }

        }

      } catch (IOException e) {
        counter--;
        if (pos != -1) {
          in.seek(pos);
        }
        this.dataSubIndex = new Tuple[counter];
        this.offsetSubIndex = new long[counter][];

        byte[] buf;
        for (int i = 0; i < counter; i++) {
          buf = new byte[in.readInt()];
          in.read(buf);
          dataSubIndex[i] = RowStoreUtil.RowStoreDecoder.toTuple(keySchema, buf);

          int offsetNum = in.readInt();
          this.offsetSubIndex[i] = new long[offsetNum];
          for (int j = 0; j < offsetNum; j++) {
            this.offsetSubIndex[i][j] = in.readLong();
          }

        }
      }
    }

    public Tuple getMin() {
      return this.min;
    }

    public Tuple getMax() {
      return this.max;
    }

    private void fillRootIndex(int entryNum, FSDataInputStream in)
        throws IOException {
      this.dataIndex = new Tuple[entryNum];
      this.offsetIndex = new long[entryNum];
      Tuple keyTuple;
      byte[] buf;
      for (int i = 0; i < entryNum; i++) {
        buf = new byte[in.readInt()];
        in.read(buf);
        keyTuple = RowStoreUtil.RowStoreDecoder.toTuple(keySchema, buf);
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
      fillLeafIndex(loadNum, subIn, this.offsetIndex[rootCursor]);
      pos = binarySearch(this.dataSubIndex, key, 0, this.dataSubIndex.length);
       
      return pos;
    }

    private int binarySearch(Tuple[] arr, Tuple key, int startPos, int endPos) {
      int offset = -1;
      int start = startPos;
      int end = endPos;
      int centerPos = (start + end) / 2;
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
      this.subIn.close();
    }
  }
}
