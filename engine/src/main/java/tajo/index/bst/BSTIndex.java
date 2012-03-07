package tajo.index.bst;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.physical.TupleComparator;
import nta.storage.Tuple;
import nta.storage.TupleUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import tajo.index.IndexMethod;
import tajo.index.IndexWriter;
import tajo.index.OrderIndexReader;

/**
 * @author Ryu Hyo Seok
 * @author Hyunsik Choi
 * 
 * This is two-level binary search tree index. This is one of the value-list 
 * index structure. Thus, it is inefficient in the case where 
 * the many of the values are same. Also, the BST shows the fast performance 
 * when the selectivity of rows to be retrieved is less than 5%. 
 */
public class BSTIndex implements IndexMethod {
  public static final int ONE_LEVEL_INDEX = 1;
  public static final int TWO_LEVEL_INDEX = 2;

  private final Configuration conf;

  public BSTIndex(final Configuration conf) {
    this.conf = conf;
  }

  @Override
  public BSTIndexWriter getIndexWriter(int level, Schema keySchema,
      TupleComparator comparator) throws IOException {
    return new BSTIndexWriter(level, keySchema, comparator);
  }

  @Override
  public BSTIndexReader getIndexReader(Fragment tablets, Schema keySchema,
      TupleComparator comparator) throws IOException {
    return new BSTIndexReader(tablets, keySchema, comparator);
  }

  public class BSTIndexWriter extends IndexWriter implements Closeable {
    private FSDataOutputStream out;
    private FileSystem fs;
    private Path tablePath;
    private Path parentPath;
    private int level;
    private int loadNum = 4096;
    private String fileName;

    private final Schema keySchema;
    private final TupleComparator compartor;
    private final KeyOffsetCollector collector;
    private KeyOffsetCollector rootCollector;

    // private Tuple lastestKey = null;

    /**
     * constructor
     * 
     * @param conf
     * @param tablePath
     * @param level
     *          : : IndexCreater.ONE_LEVEL_INDEX or IndexCreater.TWO_LEVEL_INDEX
     * @throws IOException
     */
    public BSTIndexWriter(int level, Schema keySchema,
        TupleComparator comparator) throws IOException {
      this.level = level;

      this.keySchema = keySchema;
      this.compartor = comparator;
      this.collector = new KeyOffsetCollector(comparator);
    }

    /**
     * 
     * @param loadNum
     */

    public void setLoadNum(int loadNum) {
      this.loadNum = loadNum;
    }

    /**
     * 
     * @param conf
     * @param tablePath
     * @param tablets
     * @return
     * @throws IOException
     */
    public boolean createIndex(Fragment tablet) throws IOException {
      init(tablet);
      return true;
    }

    private void init(Fragment tablet) throws IOException {
      this.tablePath = tablet.getPath();
      this.parentPath = new Path(tablePath.getParent().getParent(), "index");
      this.fileName = tablet.getId() + "." + tablet.getStartOffset() + "."
          + "test.index";
      Path indexPath = new Path(this.parentPath, fileName);

      if (fs != null) {
        fs.close();
      }
      fs = parentPath.getFileSystem(conf);
      if (!fs.exists(parentPath)) {
        fs.mkdirs(parentPath);
      }

      if (!fs.exists(indexPath)) {
        out = fs.create(indexPath);
      } else {
        throw new IOException("index file is already created.");
      }
    }

    @Override
    public void write(Tuple key, long offset) throws IOException {
      /*
       * if (lastestKey == null) { lastestKey = key; } else { if
       * (compartor.compare(lastestKey, key) >= 0) { throw new
       * IndexKeyViolationExceotion("ERROR: Key order violation (latest: " +
       * lastestKey + ", current key: " + key); } }
       */
      collector.put(key, offset);
    }

    public void flush() throws IOException {
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
        byte[] buf = TupleUtil.toBytes(this.keySchema, key);
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

      /* root index creating phase */
      if (this.level == TWO_LEVEL_INDEX) {
        TreeMap<Tuple, LinkedList<Long>> rootMap = rootCollector.getMap();
        keySet = rootMap.keySet();

        out = fs.create(new Path(this.parentPath, fileName + ".root"));
        out.writeInt(this.loadNum);
        out.writeInt(keySet.size());

        /* root key writing */
        for (Tuple key : keySet) {
          byte[] buf = TupleUtil.toBytes(keySchema, key);
          out.writeInt(buf.length);
          out.write(buf);

          LinkedList<Long> offsetList = rootMap.get(key);
          if (offsetList.size() > 1 || offsetList.size() == 0) {
            throw new IOException("Why root index doen't have one offset?");
          }
          out.writeLong(offsetList.getFirst());

        }
        out.flush();

        keySet.clear();
        rootCollector.clear();
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

    @Override
    public void close() throws IOException {
      out.flush();
      out.close();
      fs.close();
    }
  }

  public class BSTIndexReader implements OrderIndexReader {
    private FileSystem fs;
    private FSDataInputStream indexIn;
    private FSDataInputStream subIn;

    private int level;
    private int entryNum;
    private int loadNum = -1;
    private Path tablePath;
    private Path parentPath;
    private String fileName;

    private final Schema keySchema;
    private final TupleComparator comparator;

    // the cursors of BST
    private int rootCursor;
    private int keyCursor;
    private int offsetCursor;

    /* functions */
    /**
     * 
     * @param conf
     * @param tablets
     * @throws IOException
     */
    public BSTIndexReader(final Fragment tablets, Schema keySchema,
        TupleComparator comparator) throws IOException {
      this.keySchema = keySchema;
      this.comparator = comparator;
      init(keySchema, tablets, null);
    }

    /**
     * 
     * @param conf
     * @param tablets
     * @param column
     * @throws IOException
     */
    public BSTIndexReader(final Fragment tablets, Column column,
        Schema keySchema, TupleComparator comparator) throws IOException {
      this.keySchema = keySchema;
      this.comparator = comparator;
      init(keySchema, tablets, column);
    }

    private void init(final Schema schema, final Fragment tablets, Column column)
        throws IOException {

      this.tablePath = tablets.getPath();
      this.parentPath = new Path(tablePath.getParent().getParent(), "index");

      fs = tablePath.getFileSystem(conf);
      if (!fs.exists(tablePath)) {
        throw new FileNotFoundException("data file did not created");
      }
      fs.close();

      /* index file access stream */
      fs = parentPath.getFileSystem(conf);
      if (!fs.exists(parentPath)) {
        throw new FileNotFoundException("index did not created");
      }
      Column col = column;
      if (col == null) {
        for (int i = 0; i < schema.getColumnNum(); i++) {
          col = schema.getColumn(i);
          this.fileName = tablets.getId() + "." + tablets.getStartOffset()
              + "." + "test.index";
          if (fs.exists(new Path(parentPath, this.fileName))) {
            break;
          }
        }
      } else {
        this.fileName = tablets.getId() + "." + tablets.getStartOffset() + "."
            + "test.index";
      }
      indexIn = fs.open(new Path(parentPath, this.fileName));
      /* read header */
      /* type => level => entrynum */
      /*
       * this.dataType = DataType.valueOf(indexIn.readInt());
       * 
       * if (this.dataType != col.getDataType()) { throw new
       * IOException("datatype is different"); }
       */

      this.level = indexIn.readInt();
      this.entryNum = indexIn.readInt();

      fillData();
    }

    private void fillData() throws IOException {
      /* load on memory */
      if (this.level == TWO_LEVEL_INDEX) {

        Path rootPath = new Path(parentPath, this.fileName + ".root");
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
     * @param offset
     * @return
     * @throws IOException
     */
    public long find(Tuple key) throws IOException {
      return find(key, false);
    }

    @Override
    public long find(Tuple key, boolean nextKey) throws IOException {
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

    public long next() throws IOException {
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

    private void fillLeafIndex(int entryNum, FSDataInputStream in, long pos)
        throws IOException {
      int counter = 0;
      try {
        if (pos != -1) {
          in.seek(pos);
        }
        this.dataSubIndex = new Tuple[entryNum];
        this.offsetSubIndex = new long[entryNum][];

        byte[] buf = null;

        for (int i = 0; i < entryNum; i++) {
          counter++;
          buf = new byte[in.readInt()];
          in.read(buf);
          dataSubIndex[i] = TupleUtil.toTuple(keySchema, buf);

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

        byte[] buf = null;
        for (int i = 0; i < counter; i++) {
          buf = new byte[in.readInt()];
          in.read(buf);
          dataSubIndex[i] = TupleUtil.toTuple(keySchema, buf);

          int offsetNum = in.readInt();
          this.offsetSubIndex[i] = new long[offsetNum];
          for (int j = 0; j < offsetNum; j++) {
            this.offsetSubIndex[i][j] = in.readLong();
          }

        }
      }
    }

    private void fillRootIndex(int entryNum, FSDataInputStream in)
        throws IOException {
      this.dataIndex = new Tuple[entryNum];
      this.offsetIndex = new long[entryNum];
      Tuple keyTuple = null;
      byte[] buf = null;
      for (int i = 0; i < entryNum; i++) {
        buf = new byte[in.readInt()];
        in.read(buf);
        keyTuple = TupleUtil.toTuple(keySchema, buf);
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
      int pos = -1;

      correctable = true;
      pos = binarySearch(this.dataSubIndex, key, 0, this.dataSubIndex.length);
      return pos;
    }

    private int twoLevBS(Tuple key, int loadNum) throws IOException {
      int pos = -1;

      pos = binarySearch(this.dataIndex, key, 0, this.dataIndex.length);
      rootCursor = pos;
      fillLeafIndex(loadNum, subIn, this.offsetIndex[pos]);
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
  }
}
