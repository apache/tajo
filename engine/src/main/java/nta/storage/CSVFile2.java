package nta.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.statistics.Stat;
import nta.catalog.statistics.StatSet;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.DatumType;
import nta.engine.TCommonProtos.StatType;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.storage.exception.AlreadyExistsStorageException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Haemi Yang
 * @author Jimin Kim
 * 
 */
public class CSVFile2 extends Storage {
  public static final String DELIMITER = "csvfile.delimiter";
  public static final String DELIMITER_DEFAULT = ",";
  private static final Log LOG = LogFactory.getLog(CSVFile2.class);

  public CSVFile2(Configuration conf) {
    super(conf);
  }

  @Override
  public Appender getAppender(TableMeta meta, Path path) throws IOException {
    return new CSVAppender(conf, meta, path, true);
  }

  @Override
  public Scanner openScanner(Schema schema, Fragment[] tablets)
      throws IOException {
    return new CSVScanner(conf, schema, tablets);
  }

  public static class CSVAppender extends FileAppender {
    private final Path path;
    private final TableMeta meta;
    private final Schema schema;
    private final FileSystem fs;
    private FSDataOutputStream fos;
    private String delimiter;
    
    private final boolean statsEnabled;
    private StatSet statSet = null;
    private Stat numRowStat = null;
    private Stat outputBytesStat = null;

    public CSVAppender(Configuration conf, final TableMeta meta,
        final Path path, boolean statsEnabled) throws IOException {
      super(conf, meta, path);
      this.path = new Path(path, "data");
      this.fs = path.getFileSystem(conf);
      this.meta = meta;
      this.schema = meta.getSchema();

      if (!fs.exists(path.getParent())) {
        throw new FileNotFoundException(this.path.toString());
      }

      if (fs.exists(path)) {
        throw new AlreadyExistsStorageException(this.path);
      }

      fos = fs.create(path);
      
      // set delimiter.
      this.delimiter = this.meta.getOption(DELIMITER, DELIMITER_DEFAULT);
      
      this.statsEnabled = statsEnabled;
      if (statsEnabled) {
        this.statSet = new StatSet();
        this.numRowStat = new Stat(StatType.TABLE_NUM_ROWS);
        this.statSet.putStat(this.numRowStat);
        this.outputBytesStat = new Stat(StatType.TABLE_NUM_BYTES);
        this.statSet.putStat(this.outputBytesStat);
      }
    }

    @Override
    public void addTuple(Tuple tuple) throws IOException {
      StringBuilder sb = new StringBuilder();
      Column col = null;
      Datum datum = null;
      for (int i = 0; i < schema.getColumnNum(); i++) {
        datum = tuple.get(i);
        if (datum.type() == DatumType.NULL) {
        } else {
          col = schema.getColumn(i);
          switch (col.getDataType()) {
          case BYTE:
            sb.append(new String(Base64.encodeBase64(tuple.getByte(i)
                .asByteArray(), false)));
            break;
          case BYTES:
            sb.append(new String(Base64.encodeBase64(tuple.getBytes(i)
                .asByteArray(), false)));
            break;
          case STRING:
            sb.append(tuple.getString(i));
            break;
          case SHORT:
            sb.append(tuple.getShort(i));
            break;
          case INT:
            sb.append(tuple.getInt(i));
            break;
          case LONG:
            sb.append(tuple.getLong(i));
            break;
          case FLOAT:
            sb.append(tuple.getFloat(i));
            break;
          case DOUBLE:
            sb.append(tuple.getDouble(i));
            break;
          case IPv4:
            sb.append(tuple.getIPv4(i));
            break;
          case IPv6:
            sb.append(tuple.getIPv6(i));
            break;
          default:
            break;
          }
        }
        sb.append(delimiter);
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.append('\n');
      fos.writeBytes(sb.toString());
      
      // Statistical section
      if (statsEnabled) {
        numRowStat.increment();
      }
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {
      // Statistical section
      if (statsEnabled) {
        outputBytesStat.setValue(fos.getPos());
      }
      fos.close();
    }

    @Override
    public StatSet getStats() {
      return this.statSet;
    }
  }

  public static class CSVScanner extends FileScanner {
    private FileSystem fs;
    private FSDataInputStream fis;
    private SortedSet<Fragment> tabletSet;
    private Iterator<Fragment> tabletIter;
    private Fragment curTablet;
    private long startOffset, length;
    private long startPos;
    private static final byte LF = '\n';
    private String delimiter;

    private byte[] buffer = null;
    private byte[] piece = null;
    private String[] tupleList;

    private int bufferSize, validIndex, curIndex = 0;
    private final static int DEFAULT_BUFFER_SIZE = 65536;
    
    private long pageStart = -1;
    private long curTupleOffset = -1;
    private HashMap<Long, Integer> offsetCurIndexMap;
    private long[] tupleOffsets;
    
    public CSVScanner(Configuration conf, final Schema schema,
        final Fragment[] tablets) throws IOException {
      super(conf, schema, tablets);
      init(conf, schema, tablets);
    }

    public void init(Configuration conf, final Schema schema, final Fragment[] tablets)
        throws IOException {
      // set default page size.
      this.bufferSize = DEFAULT_BUFFER_SIZE;

      // set delimiter.
      this.delimiter = tablets[0].getMeta().getOption(DELIMITER, DELIMITER_DEFAULT);    
      if (this.delimiter.equals("|")) {
        this.delimiter = "\\|";
      }
      
      // set tablets iterator.
      this.tabletSet = new TreeSet<Fragment>();
      this.offsetCurIndexMap = new HashMap<Long, Integer>();
      for (Fragment t : tablets)
        this.tabletSet.add(t);
      this.tabletIter = tabletSet.iterator();
      openNextTablet();
    }
    
    private boolean openNextTablet() throws IOException {
      if (this.fis != null) {
        this.fis.close();
      }
      
      // set tablet information.
      if (tabletIter.hasNext()) {
        curTablet = tabletIter.next();     
        this.fs = curTablet.getPath().getFileSystem(this.conf);
        this.fis = this.fs.open(curTablet.getPath());
        this.startOffset = curTablet.getStartOffset();
        if (curTablet.getLength() == -1) { // unknown case
          this.length = fs.getFileStatus(curTablet.getPath()).getLen();
        } else {
          this.length = curTablet.getLength();
        }
        long available = tabletable();//(this.startOffset + this.length) - fis.getPos();
        
        // set correct start offset.
        if (startOffset != 0) {
          if (startOffset < available) {  
            fis.seek(startOffset - 1);
            while ( (fis.readByte()) != LF) {
            }
            // fis.seek(fis.getPos());
          } else {
            fis.seek(available);
          }
        }
        startPos = fis.getPos();

        return pageBuffer();
      } else {
        return false;
      }
    }

    private boolean pageBuffer() throws IOException {
      
      this.offsetCurIndexMap.clear();
      if (tabletable() < 1) {
        // initialize.
        this.curIndex = 0;
        this.validIndex = 0;
        this.curTupleOffset = 0;
        this.bufferSize = DEFAULT_BUFFER_SIZE;;
        return false;
      }
      
      // set buffer size.
      if (tabletable() <= bufferSize) {
        bufferSize = (int) tabletable();
      } else {
        bufferSize = DEFAULT_BUFFER_SIZE;
      }
      
      // read.
      if (fis.getPos() == startPos) {
        buffer = new byte[bufferSize];
        this.pageStart = fis.getPos();
        fis.read(buffer);
        piece = new byte[0];
      } else {
        if (tabletable() <= bufferSize) 
          bufferSize = piece.length + (int) tabletable();
        buffer = new byte[bufferSize];
        this.pageStart = fis.getPos() - piece.length;
        System.arraycopy(piece, 0, buffer, 0, piece.length);
        if (tabletable() != 0) {
          fis.read(buffer, piece.length, (buffer.length - piece.length));
        }
      }
      tupleList = new String(buffer).split("\n");
      checkLineFeed();
      tupleOffset();
      
      return true;
    }

    private void checkLineFeed() throws IOException {
      if ((char) buffer[buffer.length - 1] != LF) {
        if (tabletable() < 1) {
          // end of tablet.
          long mark = fis.getPos();
          int i;
          for (i = 1; fis.readByte() != LF; i++) ;
          fis.seek(mark);
          byte[] extra = new byte[i - 1];
          fis.read(extra);
          if (i > 1) { // i=1 case : read line feed.
            tupleList[tupleList.length - 1] = new String(tupleList[tupleList.length - 1] + new String(extra));
          }
          validIndex = tupleList.length;
        } else {
          // keeping incorrect tuple.
          piece = tupleList[tupleList.length -1].getBytes();
          validIndex = tupleList.length - 1;
        }
      } else {
        // correct tuple.
        if (tabletable() < bufferSize) {
          bufferSize = (int) tabletable();
        } else {
          bufferSize = DEFAULT_BUFFER_SIZE;
        }
        if (bufferSize > 0) {
       // piece = new byte[bufferSize - 1];
          // fis.read(piece);
          // fis.seek(fis.getPos()-piece.length);
          piece = new byte[0];
        }
        validIndex = tupleList.length;
      }
    }
    
    private void tupleOffset() throws IOException {
      this.curIndex = 0;
      this.curTupleOffset = 0;
      this.tupleOffsets = new long[tupleList.length];
      for (int i = 0; i < this.tupleList.length; i++) {
        this.tupleOffsets[i] = this.curTupleOffset + this.pageStart;
        this.offsetCurIndexMap.put(this.curTupleOffset + this.pageStart, i);
        this.curTupleOffset += (tupleList[i] + "\n").getBytes().length;
      }
    }
    
    @Override
    public void seek(long offset) throws IOException {
      if (this.offsetCurIndexMap.containsKey(offset)) {
        curIndex = this.offsetCurIndexMap.get(offset);
      } else if (offset >= this.pageStart + this.bufferSize || offset < this.pageStart) {
        fis.seek(offset);
        piece = new byte[0];
        buffer = new byte[DEFAULT_BUFFER_SIZE];
        bufferSize = DEFAULT_BUFFER_SIZE;
        curIndex = 0;
        validIndex = 0;
        // pageBuffer();
      } else {
        throw new IOException("invalid offset");
      }
    }

    @Override
    public long getNextOffset() throws IOException {
      if (curIndex == tupleList.length) {
        pageBuffer();
      }
      return this.tupleOffsets[curIndex];
    }

    @Override
    public long tabletable() throws IOException {
      return (this.startOffset + this.length - fis.getPos());
    }

    @Override
    public Tuple next() throws IOException {
      try {
        if (curIndex == validIndex) {
          if (!pageBuffer()) {
            if (!openNextTablet()) {
              return null;
            }
          }
        }
        
        long nextOffset = getNextOffset();

        VTuple tuple = new VTuple(schema.getColumnNum());
        tuple.setOffset(nextOffset);
        String[] cells = tupleList[curIndex++].split(delimiter);
        Column field;

        for (int i = 0; i < schema.getColumnNum(); i++) {
          field = schema.getColumn(i);
          if (cells.length <= i) {
            tuple.put(i, DatumFactory.createNullDatum());
          } else {
            String cell = cells[i].trim();

            if (cell.equals("")) {
              tuple.put(i, DatumFactory.createNullDatum());
            } else {
              switch (field.getDataType()) {
              case BYTE:
                tuple.put(i,
                    DatumFactory.createByte(Base64.decodeBase64(cell)[0]));
                break;
              case BYTES:
                tuple.put(i,
                    DatumFactory.createBytes(Base64.decodeBase64(cell)));
                break;
              case SHORT:
                tuple.put(i, DatumFactory.createShort(cell));
                break;
              case INT:
                tuple.put(i, DatumFactory.createInt(cell));
                break;
              case LONG:
                tuple.put(i, DatumFactory.createLong(cell));
                break;
              case FLOAT:
                tuple.put(i, DatumFactory.createFloat(cell));
                break;
              case DOUBLE:
                tuple.put(i, DatumFactory.createDouble(cell));
                break;
              case STRING:
                tuple.put(i, DatumFactory.createString(cell));
                break;
              case IPv4:
                if (cells[i].charAt(0) == '/') {
                  tuple.put(
                      i,
                      DatumFactory.createIPv4(cells[i].substring(1,
                          cell.length())));
                }
                break;
              }
            }
          }
        }
        return tuple;
      } catch (Throwable t) {
        LOG.error("tupleList Length: " + tupleList.length, t);
        LOG.error("tupleList Current index: " + curIndex, t);
        LOG.error("tupleList Vaild index: " + validIndex, t);
      }
      return null;
    }

    @Override
    public void reset() throws IOException {
      init(conf, schema, tablets);
    }

    @Override
    public void close() throws IOException {
      fis.close();
    }
  }
}
