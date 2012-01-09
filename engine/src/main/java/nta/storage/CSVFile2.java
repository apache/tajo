package nta.storage;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.ipc.protocolrecords.Tablet;
import nta.storage.exception.ReadOnlyException;

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
public class CSVFile2 {
  public static class CSVAppender implements Appender {
    private final Path path;
    private final Schema schema;
    private final FileSystem fs;
    private FSDataOutputStream fos;

    public CSVAppender(Configuration conf, final Path path, final Schema schema)
        throws IOException {
      this.path = new Path(path, "data");
      this.fs = path.getFileSystem(conf);
      this.schema = schema;

      if (!fs.exists(path))
        fs.mkdirs(path);
      if (fs.exists(new Path(path, "table1.csv")))
        throw new ReadOnlyException();

      fos = fs.create(new Path(this.path, "table1.csv"));
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
            sb.append(tuple.getByte(i));
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

  public static class CSVScanner implements FileScanner {
    private Configuration conf;
    private Schema schema;
    private FileSystem fs;
    private FSDataInputStream fis;
    private long startOffset;
    private long length;
    private String line;
    private byte[] sweep;
    private SortedSet<Tablet> tabletSet;
    private Iterator<Tablet> tabletIter;
    private static final byte LF = '\n';

    public CSVScanner(Configuration conf, final Schema schema, final Tablet[] tablets)
        throws IOException {
      init(conf, schema, tablets);
    }

    @Override
    public void init(Configuration conf, final Schema schema, final Tablet[] tablets)
        throws IOException {
      this.conf = conf;
      this.schema = schema;
      this.tabletSet = new TreeSet<Tablet>();

      for (Tablet t : tablets)
        this.tabletSet.add(t);

      this.tabletIter = tabletSet.iterator();

      if (tabletIter.hasNext())
        openTablet(this.tabletIter.next());
    }

    private void openTablet(Tablet tablet) throws IOException {
      this.startOffset = tablet.getStartOffset();
      this.length = tablet.getLength();      
      this.fs = tablet.getFilePath().getFileSystem(conf);

      if (!fs.exists(tablet.getFilePath()))
        fs.mkdirs(tablet.getFilePath());

      fis = fs.open(tablet.getFilePath());
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
      cells = line.split(",");
      Column field;

      for (int i = 0; i < cells.length; i++) {
        field = schema.getColumn(i);
        String cell = cells[i].trim();
        switch (field.getDataType()) {
        case SHORT:
          tuple.put(i, Short.valueOf(cell));
          break;
        case INT:
          tuple.put(i, Integer.valueOf(cell));
          break;
        case LONG:
          tuple.put(i, Long.valueOf(cell));
          break;
        case FLOAT:
          tuple.put(i, Float.valueOf(cell));
          break;
        case DOUBLE:
          tuple.put(i, Double.valueOf(cell));
          break;
        case STRING:
          tuple.put(i, cell);
          break;
        case IPv4:
          if (cells[i].charAt(0) == '/') {
            tuple.put(i,
                Inet4Address.getByName(cells[i].substring(1, cell.length())));
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
