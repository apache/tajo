/**
 * 
 */
package nta.engine.query;

import java.nio.channels.UnsupportedAddressTypeException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.engine.exception.UnsupportedException;

/**
 * @author jihoon
 *
 */
public class ResultSetMetaDataImpl implements ResultSetMetaData {
  private TableMeta meta;
  
  public ResultSetMetaDataImpl(TableMeta meta) {
    this.meta = meta;
  }

  /* (non-Javadoc)
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */
  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getCatalogName(int)
   */
  @Override
  public String getCatalogName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnClassName(int)
   */
  @Override
  public String getColumnClassName(int column) throws SQLException {
    return meta.getSchema().getColumn(column).getClass().getName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnCount()
   */
  @Override
  public int getColumnCount() throws SQLException {
    return meta.getSchema().getColumnNum();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
   */
  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnLabel(int)
   */
  @Override
  public String getColumnLabel(int column) throws SQLException {
    return meta.getSchema().getColumn(column).getQualifiedName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnName(int)
   */
  @Override
  public String getColumnName(int column) throws SQLException {
    return meta.getSchema().getColumn(column).getColumnName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnType(int)
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    // TODO
    DataType type = meta.getSchema().getColumn(column).getDataType();
    switch (type) {
    case BOOLEAN:
      return Types.BOOLEAN;
    case BIGDECIMAL:
      return Types.DECIMAL;
    case BIGINT:
      return Types.BIGINT;
    case BYTE:
      return Types.TINYINT;
    case BYTES:
      return Types.VARBINARY;
    case CHAR:
      return Types.CHAR;
    case DATE:
      return Types.DATE;
    case DOUBLE:
      return Types.DOUBLE;
    case FLOAT:
      return Types.FLOAT;
    case INT:
      return Types.INTEGER;
    case LONG:
      return Types.BIGINT;
    case SHORT:
      return Types.SMALLINT;
    case STRING:
      return Types.VARCHAR;
    default:
      throw new UnsupportedException();
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
   */
  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return meta.getSchema().getColumn(column).
        getDataType().getClass().getCanonicalName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getPrecision(int)
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getScale(int)
   */
  @Override
  public int getScale(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getSchemaName(int)
   */
  @Override
  public String getSchemaName(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getTableName(int)
   */
  @Override
  public String getTableName(int column) throws SQLException {
    return meta.getSchema().getColumn(column).getTableName();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
   */
  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isCurrency(int)
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
   */
  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isNullable(int)
   */
  @Override
  public int isNullable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isReadOnly(int)
   */
  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isSearchable(int)
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    // TODO
    return true;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isSigned(int)
   */
  @Override
  public boolean isSigned(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedAddressTypeException();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isWritable(int)
   */
  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

}
