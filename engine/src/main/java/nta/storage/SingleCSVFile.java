package nta.storage;

import java.io.FileNotFoundException;
import java.io.IOException;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.statistics.TableStat;
import nta.catalog.statistics.TableStatistics;
import nta.datum.ArrayDatum;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.DatumType;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.json.GsonCreator;
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
 *
 */
public class SingleCSVFile extends SingleStorge{
  public static final String DELIMITER = "csvfile.delimiter";
  public static final String DELIMITER_DEFAULT = "|";
  private static final Log LOG = LogFactory.getLog(SingleCSVFile.class);

  public SingleCSVFile(Configuration conf) {
    super(conf);
  }

  @Override
  public Appender getAppender(TableMeta meta, Path path) throws IOException {
    return new CSVAppender(conf, meta, path, true);
  }

  @Override
  public Scanner openSingleScanner(Schema schema, Fragment fragment)
      throws IOException {
    return new CSVScanner(conf, schema, fragment);
  }
  
  public static class CSVAppender extends FileAppender {
    private final TableMeta meta;
    private final Schema schema;
    private final FileSystem fs;
    private FSDataOutputStream fos;
    private String delimiter;

    private final boolean statsEnabled;
    private TableStatistics stats = null;

    public CSVAppender(Configuration conf, final TableMeta meta,
        final Path path, boolean statsEnabled) throws IOException {
      super(conf, meta, path);
      this.fs = path.getFileSystem(conf);
      this.meta = meta;
      this.schema = meta.getSchema();

      if (!fs.exists(path.getParent())) {
        throw new FileNotFoundException(path.toString());
      }

      if (fs.exists(path)) {
        throw new AlreadyExistsStorageException(path);
      }

      fos = fs.create(path);

      this.delimiter = this.meta.getOption(DELIMITER, DELIMITER_DEFAULT);

      this.statsEnabled = statsEnabled;
      if (statsEnabled) {
        this.stats = new TableStatistics(this.schema);
      }
    }

    @Override
    public void addTuple(Tuple tuple) throws IOException {
      StringBuilder sb = new StringBuilder();
      Column col;
      Datum datum;
      for (int i = 0; i < schema.getColumnNum(); i++) {
        datum = tuple.get(i);
        if (statsEnabled) {
          stats.analyzeField(i, datum);
        }
        if (datum.type() == DatumType.NULL) {
        } else {
          col = schema.getColumn(i);
          switch (col.getDataType()) {
          case BOOLEAN:
            sb.append(tuple.getBoolean(i));
            break;
          case BYTE:
            sb.append(new String(Base64.encodeBase64(tuple.getByte(i)
                .asByteArray(), false)));
            break;
          case BYTES:
            sb.append(new String(Base64.encodeBase64(tuple.getBytes(i)
                .asByteArray(), false)));
            break;
          case CHAR:
            sb.append(tuple.getChar(i));
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
          case ARRAY:
            /*sb.append("[");
          boolean first = true;
          ArrayDatum array = (ArrayDatum) tuple.get(i);
          for (Datum field : array.toArray()) {
            if (first) {
              first = false;
            } else {
              sb.append(delimiter);
            }
           sb.append(field.asChars());
          }
          sb.append("]");*/
            ArrayDatum array = (ArrayDatum) tuple.get(i);
            sb.append(array.toJSON());
            break;
          default:
            throw new UnsupportedOperationException("Cannot write such field: " + tuple.get(i).type());
          }
        }
        sb.append(delimiter);
      }
      if(sb.length() > 0) {
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append('\n');
      fos.writeBytes(sb.toString());

      // Statistical section
      if (statsEnabled) {
        stats.incrementRow();
      }
    }

    @Override
    public long getOffset() throws IOException {
      return fos.getPos();
    }

    @Override
    public void flush() throws IOException {
      fos.flush();
    }

    @Override
    public void close() throws IOException {
      // Statistical section
      if (statsEnabled) {
        stats.setNumBytes(fos.getPos());
      }
      fos.close();
    }

    @Override
    public TableStat getStats() {
      return this.stats.getTableStat();
    }
  }
  
  public static class CSVScanner extends SingleFileScanner {
    public CSVScanner(Configuration conf, final Schema schema, final Fragment fragment)
      throws IOException {
      super(conf, schema, fragment);
      init(conf, schema, fragment);
    }
    
    private static final byte LF = '\n';
    private final static long DEFAULT_BUFFER_SIZE = 65536;
    private long bufSize;
    private String delimiter;
    private FileSystem fs;
    private FSDataInputStream fis;
    private long startOffset, length, available, currentPos;
    private byte[] buf = null;
    private String[] tuples = null;
    private int currentIdx = 0;
    
    public void init(Configuration conf, final Schema schema, final Fragment fragment) 
      throws IOException {
      
      // Buffer size, Delimiter
      this.bufSize = DEFAULT_BUFFER_SIZE;
      this.delimiter = fragment.getMeta().getOption(DELIMITER, DELIMITER_DEFAULT);
      if (this.delimiter.equals("|")) {
        this.delimiter = "\\|";
      }
      
      // Fragment information
      this.fs = fragment.getPath().getFileSystem(this.conf);
      this.fis = this.fs.open(fragment.getPath());
      this.startOffset = fragment.getStartOffset();
      this.length = fragment.getLength();
      
      // Fragment start position
      if (startOffset != 0) {
        fis.seek(startOffset - 1);
        while ((fis.readByte()) != LF) {
        }
      }
      fragmentable();
      page();
    }
    
    private long fragmentable() throws IOException {
      currentPos = fis.getPos();
      available = startOffset + length - currentPos;
      return available;
    }
    
    private void page() throws IOException {

      // Buffer size
      if (fragmentable() < bufSize) {
        bufSize = fragmentable();
      } else {
        bufSize = DEFAULT_BUFFER_SIZE;
      }
      
      // Read
      buf = new byte[(int) bufSize];
      fis.read(buf);
      check();
    }
    
    private void check() throws IOException {
      tuples = new String(buf).split("\n");
      
      int cnt = 0;
      if (buf[buf.length -1] != LF && fis.available() > 0) {
        
        // Count bytes
        fragmentable();
        for (; fis.readByte() != LF; cnt++) ;
        fis.seek(currentPos);
        
        // Read bytes
        byte temp[] = new byte[cnt];
        fis.read(temp);
        fis.readByte(); // Read line feed
        
        // Replace tuple
        tuples[tuples.length - 1] = new String(tuples[tuples.length - 1] + new String(temp));
      }
    }

    @Override
    public Tuple next() throws IOException {
      try {
        if (currentIdx == tuples.length) {
          if (fragmentable() < 1) {
            return null;
          } else {
            currentIdx = 0; // Index initialization
            page();
          }
        }
        
        String[] cells = tuples[currentIdx++].split(delimiter);
        VTuple tuple = new VTuple(schema.getColumnNum());
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
              case BOOLEAN:
                tuple.put(i, DatumFactory.createBool(cell));
                break;
              case BYTE:
                tuple.put(i, DatumFactory.createByte(Base64.decodeBase64(cell)[0]));
                break;
              case CHAR:
                tuple.put(i, DatumFactory.createChar(cell.charAt(0)));
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
                tuple.put(i,DatumFactory.createIPv4(cell));
                break;
              case ARRAY:
                Datum data = GsonCreator.getInstance().fromJson(cell, Datum.class);
                tuple.put(i, data);
                break;
              }
            }
          }
        }
        return tuple;
      } catch (Throwable t) {
        LOG.error("Tuple list length: " + tuples.length, t);
        LOG.error("Tuple list current index: " + currentIdx, t);
      }
      return null;
    }

    @Override
    public void reset() throws IOException {
      init(conf, schema, fragment);
    }

    @Override
    public void close() throws IOException {
      fis.close();
    }
  }
}
