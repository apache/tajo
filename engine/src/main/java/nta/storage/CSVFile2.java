package nta.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.datum.DatumFactory;
import nta.datum.NullDatum;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.storage.exception.AlreadyExistsStorageException;

import org.apache.commons.codec.binary.Base64;
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

  // public static final Base64 base64 = new Base64(0, new byte[0]);

  public CSVFile2(Configuration conf) {
    super(conf);
  }

  @Override
  public Appender getAppender(Schema schema, Path path) throws IOException {
    return new CSVAppender(conf, path, schema, false);
  }

  @Override
  public Scanner openScanner(Schema schema, Fragment[] tablets)
      throws IOException {
    return new CSVScanner(conf, schema, tablets);
  }

  public static class CSVAppender implements Appender {

    private final Path path;
    private final Schema schema;
    private final FileSystem fs;
    private FSDataOutputStream fos;

    public CSVAppender(Configuration conf, final Path path,
        final Schema schema, boolean tablewrite) throws IOException {
      this.path = new Path(path, "data");
      this.fs = path.getFileSystem(conf);
      this.schema = schema;

      if (!fs.exists(path.getParent())) {
        throw new FileNotFoundException(path.toString());
      }

      if (fs.exists(path)) {
        throw new AlreadyExistsStorageException(path);
      }

      fos = fs.create(path);
    }

    @Override
    public void addTuple(Tuple tuple) throws IOException {
      StringBuilder sb = new StringBuilder();
      Column col = null;
      for (int i = 0; i < schema.getColumnNum(); i++) {
        if (tuple.contains(i)) {
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
        sb.append(',');
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.append('\n');
      fos.writeBytes(sb.toString());
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {
      fos.close();
    }
  }

  public static class CSVScanner extends FileScanner {
    private FileSystem fs;
    private FSDataInputStream fis;
    private SortedSet<Fragment> tabletSet;
    private Iterator<Fragment> tabletIter;
    private long startOffset;
    private long length;
    private long tabletstart;
    private static final byte LF = '\n';
    private String delimiter;

    private byte[] buffer = null;
    private byte[] piece = null;
    private String[] tupleList;
    private int curIndex = 0;
    private int checkTupleList, bufferSize = 65536;
    private final static int DEFAULT_BUFFER_SIZE = 65536;

    private long pageStart = -1;
    private long currentTupleOffset = -1;
    private HashMap<Long, Integer> offsetCurIndexMap;
    private long[] tupleOffsets;

    public CSVScanner(Configuration conf, final Schema schema,
        final Fragment[] tablets) throws IOException {
      super(conf, schema, tablets);
      init(conf, schema, tablets);
    }

    public void init(Configuration conf, final Schema schema,
        final Fragment[] tablets) throws IOException {
      this.tabletSet = new TreeSet<Fragment>();
      this.offsetCurIndexMap = new HashMap<Long, Integer>();

      for (Fragment t : tablets)
        this.tabletSet.add(t);

      this.tabletIter = tabletSet.iterator();

      if (tabletIter.hasNext())
        openTablet(this.tabletIter.next());
    }

    private void openTablet(Fragment tablet) throws IOException {
      this.delimiter =
          tablet.getMeta().getOptions().get(DELIMITER, DELIMITER_DEFAULT);
      this.startOffset = tablet.getStartOffset();
      this.length = tablet.getLength();
      this.fs = tablet.getPath().getFileSystem(conf);

      fis = fs.open(tablet.getPath());
      long available = (this.startOffset + this.length) - fis.getPos();

      if (startOffset != 0) {
        if (startOffset < available) {
          fis.seek(startOffset - 1);
          if (fis.readByte() == LF) {
            fis.seek(startOffset);
          } else {
            while (fis.readByte() != LF)
              ;
            fis.seek(fis.getPos());
          }
        } else
          fis.seek(available);
      }
      tabletstart = fis.getPos();
      pageBuffer();
    }

    private boolean pageBuffer() throws IOException {
      this.offsetCurIndexMap.clear();
      if (available() < 1) {
        this.curIndex = 0;
        this.checkTupleList = 0;
        this.currentTupleOffset = 0;
        return false;
      }
      if (available() <= bufferSize) {
        bufferSize = (int) available();
      }

      if (fis.getPos() == 0 || fis.getPos() == tabletstart) { // first.
        buffer = new byte[bufferSize];
        this.pageStart = fis.getPos();
        fis.read(buffer);
      } else if (available() <= bufferSize) { // last.
        bufferSize = piece.length + (int) available();
        buffer = new byte[bufferSize];
        this.pageStart = fis.getPos() - piece.length;
        System.arraycopy(piece, 0, buffer, 0, piece.length);
        fis.read(buffer, piece.length, (buffer.length - piece.length));
      } else {
        buffer = new byte[bufferSize];
        this.pageStart = fis.getPos() - piece.length;
        System.arraycopy(piece, 0, buffer, 0, piece.length);
        fis.read(buffer, piece.length, (buffer.length - piece.length));
      }
      tupleList = new String(buffer).split("\n");

      if ((char) buffer[buffer.length - 1] != LF) { // check \n.
        checkTupleList = tupleList.length - 1;
        piece = tupleList[tupleList.length - 1].getBytes();
      } else {
        checkTupleList = tupleList.length;
        piece = new byte[bufferSize];
        fis.read(piece);
      }

      this.curIndex = 0;
      this.currentTupleOffset = 0;
      this.tupleOffsets = new long[tupleList.length];
      for (int i = 0; i < this.tupleList.length; i++) {
        this.tupleOffsets[i] = this.currentTupleOffset + this.pageStart;
        this.offsetCurIndexMap.put(this.currentTupleOffset + this.pageStart, i);
        this.currentTupleOffset += (tupleList[i] + "\n").getBytes().length;
      }
      checkLastPos();
      return true;
    }

    private void checkLastPos() throws IOException {
      if (fis.getPos() >= startOffset + length) {
        int i, pos = (int) fis.getPos();
        for (i = tupleList.length - 1; i >= 0; i--) {
          pos -= (tupleList[i].length() + 1);
          if (pos < startOffset + length)
            break;
        }
        checkTupleList = i + 1;
      }
    }

    @Override
    public void seek(long offset) throws IOException {
      if (this.offsetCurIndexMap.containsKey(offset)) {
        curIndex = this.offsetCurIndexMap.get(offset);
      } else if (offset >= this.pageStart + this.bufferSize
          || offset < this.pageStart) {
        fis.seek(offset);
        piece = new byte[0];
        buffer = new byte[DEFAULT_BUFFER_SIZE];
        bufferSize = DEFAULT_BUFFER_SIZE;
        curIndex = 0;
        checkTupleList = 0;
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
    public long available() throws IOException {
      return (this.startOffset + this.length - fis.getPos());
    }

    @Override
    public Tuple next() throws IOException {

      if (curIndex == checkTupleList) { // tuple list end.
        curIndex = 0;
        if (fis.getPos() >= startOffset + length) {
          return null;
        }
        if (!pageBuffer()) {
          return null;
        }
      }

      VTuple tuple = new VTuple(schema.getColumnNum());
      String[] cells = tupleList[curIndex++].split(delimiter);
      Column field;

      for (int i = 0; i < cells.length; i++) {
        field = schema.getColumn(i);
        String cell = cells[i].trim();
        if (cell.equals("")) {
          tuple.put(i, DatumFactory.createNullDatum());
        } else {
          switch (field.getDataType()) {
          case BYTE:
            tuple.put(i, DatumFactory.createByte(Base64.decodeBase64(cell)[0]));
            break;
          case BYTES:
            tuple.put(i, DatumFactory.createBytes(Base64.decodeBase64(cell)));
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
              tuple
                  .put(
                      i,
                      DatumFactory.createIPv4(cells[i].substring(1,
                          cell.length())));
            }
            break;
          }
        }
      }
      return tuple;
    }

    @Override
    public void reset() throws IOException {

    }

    @Override
    public void close() throws IOException {
      fis.close();
    }
  }
}
