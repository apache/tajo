/**
 * 
 */
package tajo.engine.query;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import tajo.catalog.TableMeta;
import tajo.catalog.TableMetaImpl;
import tajo.catalog.proto.CatalogProtos.TableProto;
import tajo.datum.Datum;
import tajo.datum.DatumType;
import tajo.exception.UnsupportedException;
import tajo.storage.Fragment;
import tajo.storage.MergeScanner;
import tajo.storage.Scanner;
import tajo.storage.Tuple;
import tajo.util.FileUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class ResultSetImpl implements ResultSet {
  private final String cursorName = "tajo";
  private Configuration conf;
  private FileSystem fs;
  private Scanner scanner;
  private TableMeta meta;
  private Tuple cur;
  private int curRow;
  private long totalRow;
  private boolean wasNull;

  public ResultSetImpl(Configuration conf, String path) throws IOException {
    this(conf, new Path(path));
  }

  public ResultSetImpl(Configuration conf, Path path) throws IOException {
    this.conf = conf;
    this.fs = path.getFileSystem(this.conf);
    // TODO - to be improved. It can be solved to get the query finish status
    // from master.
    try {
      this.meta = getMeta(this.conf, path);
    } catch (FileNotFoundException fnf) {
      this.totalRow = 0;
      init();
      return;
    }
    this.totalRow = meta.getStat().getNumRows();
    Collection<Fragment> frags = getFragmentsNG(meta, path);
    scanner = new MergeScanner(conf, meta, frags);
    init();
  }

  private void init() {
    cur = null;
    curRow = 0;
  }

  private TableMeta getMeta(Configuration conf, Path tablePath)
      throws IOException {
    Path tableMetaPath = new Path(tablePath, ".meta");
    if (!fs.exists(tableMetaPath)) {
      throw new FileNotFoundException(".meta file not found in "
          + tablePath.toString());
    }
    FSDataInputStream in = fs.open(tableMetaPath);
    TableProto tableProto = (TableProto) FileUtil.loadProto(in,
        TableProto.getDefaultInstance());
    return new TableMetaImpl(tableProto);
  }

  class FileNameComparator implements Comparator<FileStatus> {

    @Override
    public int compare(FileStatus f1, FileStatus f2) {
      return f2.getPath().getName().compareTo(f1.getPath().getName());
    }
  }

  private Collection<Fragment> getFragmentsNG(TableMeta meta, Path tablePath)
      throws IOException {
    List<Fragment> fraglist = Lists.newArrayList();
    FileStatus[] files = fs.listStatus(tablePath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().charAt(0) != '.';
      }
    });
    Arrays.sort(files, new FileNameComparator());

    String tbname = tablePath.getName();
    for (int i = 0; i < files.length; i++) {
      if (files[i].getLen() == 0) {
        continue;
      }
      fraglist.add(new Fragment(tbname + "_" + i, files[i].getPath(), meta, 0l,
          files[i].getLen(), null));
    }
    return fraglist;
  }

  private Fragment[] getFragments(TableMeta meta, Path tablePath)
      throws IOException {
    List<Fragment> fraglist = Lists.newArrayList();
    FileStatus[] files = fs.listStatus(new Path(tablePath, "data"));
    Arrays.sort(files, new FileNameComparator());

    String tbname = tablePath.getName();
    for (int i = 0; i < files.length; i++) {
      if (files[i].getLen() == 0) {
        continue;
      }
      fraglist.add(new Fragment(tbname + "_" + i, files[i].getPath(), meta, 0l,
          files[i].getLen(), null));
    }
    return fraglist.toArray(new Fragment[fraglist.size()]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */
  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#absolute(int)
   */
  @Override
  public boolean absolute(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#afterLast()
   */
  @Override
  public void afterLast() throws SQLException {
    while (this.next())
      ;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#beforeFirst()
   */
  @Override
  public void beforeFirst() throws SQLException {
    try {
      scanner.reset();
      init();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#cancelRowUpdates()
   */
  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#clearWarnings()
   */
  @Override
  public void clearWarnings() throws SQLException {
    // TODO
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#close()
   */
  @Override
  public void close() throws SQLException {
    try {
      this.scanner.close();
      cur = null;
      curRow = -1;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#deleteRow()
   */
  @Override
  public void deleteRow() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#findColumn(java.lang.String)
   */
  @Override
  public int findColumn(String colName) throws SQLException {
    return this.meta.getSchema().getColumnIdByName(colName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#first()
   */
  @Override
  public boolean first() throws SQLException {
    this.beforeFirst();
    return this.next();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getArray(int)
   */
  @Override
  public Array getArray(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getArray(java.lang.String)
   */
  @Override
  public Array getArray(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getAsciiStream(int)
   */
  @Override
  public InputStream getAsciiStream(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
   */
  @Override
  public InputStream getAsciiStream(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBigDecimal(int)
   */
  @Override
  public BigDecimal getBigDecimal(int fieldId) throws SQLException {
    // TODO
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
   */
  @Override
  public BigDecimal getBigDecimal(String fieldName) throws SQLException {
    // TODO
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBigDecimal(int, int)
   */
  @Override
  public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
   */
  @Override
  public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBinaryStream(int)
   */
  @Override
  public InputStream getBinaryStream(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
   */
  @Override
  public InputStream getBinaryStream(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBlob(int)
   */
  @Override
  public Blob getBlob(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBlob(java.lang.String)
   */
  @Override
  public Blob getBlob(String arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBoolean(int)
   */
  @Override
  public boolean getBoolean(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    return datum.asBool();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBoolean(java.lang.String)
   */
  @Override
  public boolean getBoolean(String colName) throws SQLException {
    Datum datum = cur.get(findColumn(colName));
    handleNull(datum);
    return datum.asBool();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getByte(int)
   */
  @Override
  public byte getByte(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    return datum.asByte();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getByte(java.lang.String)
   */
  @Override
  public byte getByte(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    return datum.asByte();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBytes(int)
   */
  @Override
  public byte[] getBytes(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    return datum.asByteArray();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getBytes(java.lang.String)
   */
  @Override
  public byte[] getBytes(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    return datum.asByteArray();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getCharacterStream(int)
   */
  @Override
  public Reader getCharacterStream(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
   */
  @Override
  public Reader getCharacterStream(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getClob(int)
   */
  @Override
  public Clob getClob(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getClob(java.lang.String)
   */
  @Override
  public Clob getClob(String arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getConcurrency()
   */
  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getCursorName()
   */
  @Override
  public String getCursorName() throws SQLException {
    return cursorName;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDate(int)
   */
  @Override
  public Date getDate(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDate(java.lang.String)
   */
  @Override
  public Date getDate(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
   */
  @Override
  public Date getDate(int arg0, Calendar arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
   */
  @Override
  public Date getDate(String arg0, Calendar arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDouble(int)
   */
  @Override
  public double getDouble(int fieldId) throws SQLException {
    Datum datum = cur.getDouble(fieldId - 1);
    handleNull(datum);
    return datum.asDouble();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getDouble(java.lang.String)
   */
  @Override
  public double getDouble(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    return datum.asDouble();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getFetchDirection()
   */
  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getFetchSize()
   */
  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getFloat(int)
   */
  @Override
  public float getFloat(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    return datum.asFloat();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getFloat(java.lang.String)
   */
  @Override
  public float getFloat(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    return datum.asFloat();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getHoldability()
   */
  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getInt(int)
   */
  @Override
  public int getInt(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    return datum.asInt();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getInt(java.lang.String)
   */
  @Override
  public int getInt(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    return datum.asInt();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getLong(int)
   */
  @Override
  public long getLong(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    return datum.asLong();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getLong(java.lang.String)
   */
  @Override
  public long getLong(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    return datum.asLong();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getMetaData()
   */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new ResultSetMetaDataImpl(meta);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNCharacterStream(int)
   */
  @Override
  public Reader getNCharacterStream(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNCharacterStream(java.lang.String)
   */
  @Override
  public Reader getNCharacterStream(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNClob(int)
   */
  @Override
  public NClob getNClob(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNClob(java.lang.String)
   */
  @Override
  public NClob getNClob(String arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNString(int)
   */
  @Override
  public String getNString(int fieldId) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getNString(java.lang.String)
   */
  @Override
  public String getNString(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getObject(int)
   */
  @Override
  public Object getObject(int fieldId) throws SQLException {
    Datum d = cur.get(fieldId - 1);
    wasNull = (d == null);

    // TODO - to be changed to return Object type
    return d;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getObject(java.lang.String)
   */
  @Override
  public Object getObject(String arg0) throws SQLException {
    Datum d = cur.get(findColumn(arg0));
    wasNull = (d == null);
    return d;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getObject(int, java.util.Map)
   */
  @Override
  public Object getObject(int arg0, Map<String, Class<?>> arg1)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
   */
  @Override
  public Object getObject(String arg0, Map<String, Class<?>> arg1)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRef(int)
   */
  @Override
  public Ref getRef(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRef(java.lang.String)
   */
  @Override
  public Ref getRef(String arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRow()
   */
  @Override
  public int getRow() throws SQLException {
    return curRow;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRowId(int)
   */
  @Override
  public RowId getRowId(int fieldId) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getRowId(java.lang.String)
   */
  @Override
  public RowId getRowId(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getSQLXML(int)
   */
  @Override
  public SQLXML getSQLXML(int fieldId) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getSQLXML(java.lang.String)
   */
  @Override
  public SQLXML getSQLXML(String arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getShort(int)
   */
  @Override
  public short getShort(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    return datum.asShort();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getShort(java.lang.String)
   */
  @Override
  public short getShort(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    return datum.asShort();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getStatement()
   */
  @Override
  public Statement getStatement() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getString(int)
   */
  @Override
  public String getString(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    return datum.asChars();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getString(java.lang.String)
   */
  @Override
  public String getString(String arg0) throws SQLException {
    Datum datum = cur.get(findColumn(arg0));
    handleNull(datum);
    return datum.asChars();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTime(int)
   */
  @Override
  public Time getTime(int fieldId) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTime(java.lang.String)
   */
  @Override
  public Time getTime(String name) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
   */
  @Override
  public Time getTime(int fieldId, Calendar arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
   */
  @Override
  public Time getTime(String name, Calendar arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTimestamp(int)
   */
  @Override
  public Timestamp getTimestamp(int fieldId) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTimestamp(java.lang.String)
   */
  @Override
  public Timestamp getTimestamp(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
   */
  @Override
  public Timestamp getTimestamp(int fieldId, Calendar arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
   */
  @Override
  public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getType()
   */
  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getURL(int)
   */
  @Override
  public URL getURL(int fieldId) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getURL(java.lang.String)
   */
  @Override
  public URL getURL(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getUnicodeStream(int)
   */
  @Override
  public InputStream getUnicodeStream(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
   */
  @Override
  public InputStream getUnicodeStream(String arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#getWarnings()
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#insertRow()
   */
  @Override
  public void insertRow() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#isAfterLast()
   */
  @Override
  public boolean isAfterLast() throws SQLException {
    return this.curRow > this.totalRow;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#isBeforeFirst()
   */
  @Override
  public boolean isBeforeFirst() throws SQLException {
    return this.curRow == 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#isClosed()
   */
  @Override
  public boolean isClosed() throws SQLException {
    return this.curRow == -1;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#isFirst()
   */
  @Override
  public boolean isFirst() throws SQLException {
    return this.curRow == 1;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#isLast()
   */
  @Override
  public boolean isLast() throws SQLException {
    return this.curRow == this.totalRow;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#last()
   */
  @Override
  public boolean last() throws SQLException {
    Tuple last = null;
    while (this.next()) {
      last = cur;
    }
    cur = last;
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#moveToCurrentRow()
   */
  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#moveToInsertRow()
   */
  @Override
  public void moveToInsertRow() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#next()
   */
  @Override
  public boolean next() throws SQLException {
    try {
      if (totalRow <= 0)
        return false;

      cur = scanner.next();
      curRow++;
      if (cur != null) {
        return true;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#previous()
   */
  @Override
  public boolean previous() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#refreshRow()
   */
  @Override
  public void refreshRow() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#relative(int)
   */
  @Override
  public boolean relative(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#rowDeleted()
   */
  @Override
  public boolean rowDeleted() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#rowInserted()
   */
  @Override
  public boolean rowInserted() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#rowUpdated()
   */
  @Override
  public boolean rowUpdated() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#setFetchDirection(int)
   */
  @Override
  public void setFetchDirection(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#setFetchSize(int)
   */
  @Override
  public void setFetchSize(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
   */
  @Override
  public void updateArray(int arg0, Array arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
   */
  @Override
  public void updateArray(String arg0, Array arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream)
   */
  @Override
  public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String,
   * java.io.InputStream)
   */
  @Override
  public void updateAsciiStream(String arg0, InputStream arg1)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
   */
  @Override
  public void updateAsciiStream(int arg0, InputStream arg1, int arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String,
   * java.io.InputStream, int)
   */
  @Override
  public void updateAsciiStream(String arg0, InputStream arg1, int arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, long)
   */
  @Override
  public void updateAsciiStream(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String,
   * java.io.InputStream, long)
   */
  @Override
  public void updateAsciiStream(String arg0, InputStream arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
   */
  @Override
  public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBigDecimal(java.lang.String,
   * java.math.BigDecimal)
   */
  @Override
  public void updateBigDecimal(String arg0, BigDecimal arg1)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream)
   */
  @Override
  public void updateBinaryStream(int arg0, InputStream arg1)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String,
   * java.io.InputStream)
   */
  @Override
  public void updateBinaryStream(String arg0, InputStream arg1)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
   */
  @Override
  public void updateBinaryStream(int arg0, InputStream arg1, int arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String,
   * java.io.InputStream, int)
   */
  @Override
  public void updateBinaryStream(String arg0, InputStream arg1, int arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, long)
   */
  @Override
  public void updateBinaryStream(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String,
   * java.io.InputStream, long)
   */
  @Override
  public void updateBinaryStream(String arg0, InputStream arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
   */
  @Override
  public void updateBlob(int arg0, Blob arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
   */
  @Override
  public void updateBlob(String arg0, Blob arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream)
   */
  @Override
  public void updateBlob(int arg0, InputStream arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream)
   */
  @Override
  public void updateBlob(String arg0, InputStream arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream, long)
   */
  @Override
  public void updateBlob(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream,
   * long)
   */
  @Override
  public void updateBlob(String arg0, InputStream arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBoolean(int, boolean)
   */
  @Override
  public void updateBoolean(int arg0, boolean arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
   */
  @Override
  public void updateBoolean(String arg0, boolean arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateByte(int, byte)
   */
  @Override
  public void updateByte(int arg0, byte arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
   */
  @Override
  public void updateByte(String arg0, byte arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBytes(int, byte[])
   */
  @Override
  public void updateBytes(int arg0, byte[] arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
   */
  @Override
  public void updateBytes(String arg0, byte[] arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader)
   */
  @Override
  public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String,
   * java.io.Reader)
   */
  @Override
  public void updateCharacterStream(String arg0, Reader arg1)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
   */
  @Override
  public void updateCharacterStream(int arg0, Reader arg1, int arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String,
   * java.io.Reader, int)
   */
  @Override
  public void updateCharacterStream(String arg0, Reader arg1, int arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, long)
   */
  @Override
  public void updateCharacterStream(int arg0, Reader arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String,
   * java.io.Reader, long)
   */
  @Override
  public void updateCharacterStream(String arg0, Reader arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
   */
  @Override
  public void updateClob(int arg0, Clob arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
   */
  @Override
  public void updateClob(String arg0, Clob arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(int, java.io.Reader)
   */
  @Override
  public void updateClob(int arg0, Reader arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader)
   */
  @Override
  public void updateClob(String arg0, Reader arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(int, java.io.Reader, long)
   */
  @Override
  public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader, long)
   */
  @Override
  public void updateClob(String arg0, Reader arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
   */
  @Override
  public void updateDate(int arg0, Date arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
   */
  @Override
  public void updateDate(String arg0, Date arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateDouble(int, double)
   */
  @Override
  public void updateDouble(int arg0, double arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
   */
  @Override
  public void updateDouble(String arg0, double arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateFloat(int, float)
   */
  @Override
  public void updateFloat(int arg0, float arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
   */
  @Override
  public void updateFloat(String arg0, float arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateInt(int, int)
   */
  @Override
  public void updateInt(int arg0, int arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateInt(java.lang.String, int)
   */
  @Override
  public void updateInt(String arg0, int arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateLong(int, long)
   */
  @Override
  public void updateLong(int arg0, long arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateLong(java.lang.String, long)
   */
  @Override
  public void updateLong(String arg0, long arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader)
   */
  @Override
  public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String,
   * java.io.Reader)
   */
  @Override
  public void updateNCharacterStream(String arg0, Reader arg1)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader, long)
   */
  @Override
  public void updateNCharacterStream(int arg0, Reader arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String,
   * java.io.Reader, long)
   */
  @Override
  public void updateNCharacterStream(String arg0, Reader arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(int, java.sql.NClob)
   */
  @Override
  public void updateNClob(int arg0, NClob arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.sql.NClob)
   */
  @Override
  public void updateNClob(String arg0, NClob arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(int, java.io.Reader)
   */
  @Override
  public void updateNClob(int arg0, Reader arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader)
   */
  @Override
  public void updateNClob(String arg0, Reader arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(int, java.io.Reader, long)
   */
  @Override
  public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader, long)
   */
  @Override
  public void updateNClob(String arg0, Reader arg1, long arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNString(int, java.lang.String)
   */
  @Override
  public void updateNString(int arg0, String arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNString(java.lang.String, java.lang.String)
   */
  @Override
  public void updateNString(String arg0, String arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNull(int)
   */
  @Override
  public void updateNull(int arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateNull(java.lang.String)
   */
  @Override
  public void updateNull(String arg0) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
   */
  @Override
  public void updateObject(int arg0, Object arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
   */
  @Override
  public void updateObject(String arg0, Object arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
   */
  @Override
  public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object,
   * int)
   */
  @Override
  public void updateObject(String arg0, Object arg1, int arg2)
      throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
   */
  @Override
  public void updateRef(int arg0, Ref arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
   */
  @Override
  public void updateRef(String arg0, Ref arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRow()
   */
  @Override
  public void updateRow() throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRowId(int, java.sql.RowId)
   */
  @Override
  public void updateRowId(int arg0, RowId arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateRowId(java.lang.String, java.sql.RowId)
   */
  @Override
  public void updateRowId(String arg0, RowId arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateSQLXML(int, java.sql.SQLXML)
   */
  @Override
  public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
    throw new UnsupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateSQLXML(java.lang.String, java.sql.SQLXML)
   */
  @Override
  public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateShort(int, short)
   */
  @Override
  public void updateShort(int arg0, short arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateShort(java.lang.String, short)
   */
  @Override
  public void updateShort(String arg0, short arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateString(int, java.lang.String)
   */
  @Override
  public void updateString(int arg0, String arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
   */
  @Override
  public void updateString(String arg0, String arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
   */
  @Override
  public void updateTime(int arg0, Time arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
   */
  @Override
  public void updateTime(String arg0, Time arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
   */
  @Override
  public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#updateTimestamp(java.lang.String,
   * java.sql.Timestamp)
   */
  @Override
  public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
    throw new UnsupportedException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSet#wasNull()
   */
  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  private void handleNull(Datum d) {
    wasNull = (d.type() == DatumType.NULL);
  }
}
