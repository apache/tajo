package nta.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import nta.catalog.Column;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.datum.DatumFactory;
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
  //public static final Base64 base64 = new Base64(0, new byte[0]);
  
  public CSVFile2(Configuration conf) {
    super(conf);
  }

  @Override
  public Appender getAppender(Schema schema, Path path) 
      throws IOException {
    return new CSVAppender(getConf(), path, schema, false);
  }

  @Override
  public Scanner openScanner(Schema schema, Fragment[] tablets) 
      throws IOException {
    return new CSVScanner(conf, schema, tablets);
  }
  
  public static class CSVAppender extends FileAppender {    
    private final FileSystem fs;
    private FSDataOutputStream fos;
    
    public CSVAppender(Configuration conf, final Path path, 
        final Schema schema, boolean tablewrite)    
        throws IOException {
      super(conf, schema, path);      
      this.fs = path.getFileSystem(conf);
      
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
            sb.append(new String(Base64.encodeBase64(tuple.getByte(i).asByteArray(),false)));
            break;
          case BYTES:
            sb.append(new String(Base64.encodeBase64(tuple.getBytes(i).asByteArray(),false)));
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
    
    private long startOffset;
    private long length;
    private String line;
    private byte[] sweep;
    
    private SortedSet<Fragment> tabletSet;
    private Iterator<Fragment> tabletIter;
    
    private static final byte LF = '\n';    
    private String delimiter;
    private Options option;
    

    public CSVScanner(Configuration conf, final Schema schema, final Fragment[] tablets)
        throws IOException {
      super(conf, schema, tablets);
      this.delimiter = DELIMITER_DEFAULT;
      init();      
    }
    
    public CSVScanner(Configuration conf, final Schema schema, 
        final Fragment [] tablets, Options option) throws IOException {
      this(conf, schema, tablets);
      this.option = option;
      this.delimiter = option.get(DELIMITER, DELIMITER_DEFAULT);
      init();
    }

    private void init() throws IOException {
      this.fs = FileSystem.get(conf);
      this.tabletSet = new TreeSet<Fragment>();

      for (Fragment t : tablets)
        this.tabletSet.add(t);

      this.tabletIter = tabletSet.iterator();

      if (tabletIter.hasNext())
        openTablet(this.tabletIter.next());
    }

    private void openTablet(Fragment tablet) throws IOException {
      this.startOffset = tablet.getStartOffset();
      this.length = tablet.getLength();      
      this.fs = tablet.getPath().getFileSystem(conf);

      fis = fs.open(tablet.getPath());
      long available = fis.available();
      if (startOffset != 0) {
        if (startOffset < available) {
          fis.seek(startOffset);
          while (fis.readByte() != LF)
            ;
        } else {
          fis.seek(available);
        }
      }
    }

    @Override
    public Tuple next() throws IOException {
      if (fis.getPos() > startOffset + length) {
        if(tabletIter.hasNext()) {
          openTablet(tabletIter.next());
        } else {
          return null;
        }
      }

      if ((line = fis.readLine()) == null) {
        if(tabletIter.hasNext()) {
          openTablet(tabletIter.next());
        } else {
          return null;
        }
      }

      VTuple tuple = new VTuple(schema.getColumnNum());
      String[] cells = null;
      cells = line.split(this.delimiter);
      Column field;

      for (int i = 0; i < cells.length; i++) {
        field = schema.getColumn(i);
        String cell = cells[i].trim();
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
            tuple.put(i,
                DatumFactory.createIPv4(cells[i].substring(1, cell.length())));
          }
          break;
        }
      }
      return tuple;
    }

    @Override
    public void reset() throws IOException {

    }
    
    @Override
    public Schema getSchema() {     
      return this.schema;
    }

    @Override
    public void close() throws IOException {
      fis.close();
    }

  }
}
