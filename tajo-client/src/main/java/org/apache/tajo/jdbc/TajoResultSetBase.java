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

package org.apache.tajo.jdbc;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.*;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public abstract class TajoResultSetBase implements ResultSet {
  protected int curRow;
  protected long totalRow;
  protected boolean wasNull;
  protected Schema schema;
  protected Tuple cur;

  protected void init() {
    cur = null;
    curRow = 0;
    totalRow = 0;
    wasNull = false;
  }

  private void handleNull(Datum d) {
    wasNull = (d instanceof NullDatum);
  }

  public Tuple getCurrentTuple() {
    return cur;
  }

  @Override
  public void beforeFirst() throws SQLException {
    init();
  }

  @Override
  public boolean getBoolean(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    if (wasNull) {
      return false;
    }
    return datum.asBool();
  }

  @Override
  public boolean getBoolean(String colName) throws SQLException {
    Datum datum = cur.get(findColumn(colName));
    handleNull(datum);
    if (wasNull) {
      return false;
    }
    return datum.asBool();
  }

  @Override
  public byte getByte(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    if (wasNull) {
      return 0;
    }
    return datum.asByte();
  }

  @Override
  public byte getByte(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    if (wasNull) {
      return 0;
    }
    return datum.asByte();
  }

  @Override
  public byte[] getBytes(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    if (wasNull) {
      return null;
    }
    return datum.asByteArray();
  }

  @Override
  public byte[] getBytes(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    if (wasNull) {
      return null;
    }
    return datum.asByteArray();
  }

  @Override
  public double getDouble(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    if (wasNull) {
      return 0.0;
    }
    return datum.asFloat8();
  }

  @Override
  public double getDouble(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    if (wasNull) {
      return 0.0;
    }
    return datum.asFloat8();
  }

  @Override
  public float getFloat(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    if (wasNull) {
      return 0.0f;
    }
    return datum.asFloat4();
  }

  @Override
  public float getFloat(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    if (wasNull) {
      return 0.0f;
    }
    return datum.asFloat4();
  }

  @Override
  public int getInt(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    if (wasNull) {
      return 0;
    }
    return datum.asInt4();
  }

  @Override
  public int getInt(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    if (wasNull) {
      return 0;
    }
    return datum.asInt4();
  }

  @Override
  public long getLong(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    if (wasNull) {
      return 0;
    }
    return datum.asInt8();
  }

  @Override
  public long getLong(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    if (wasNull) {
      return 0;
    }
    return datum.asInt8();
  }

  @Override
  public Object getObject(int fieldId) throws SQLException {
    Datum d = cur.get(fieldId - 1);
    handleNull(d);

    if (wasNull) {
      return null;
    }
    TajoDataTypes.Type dataType = schema.getColumn(fieldId - 1).getDataType().getType();

    switch(dataType) {
      case BOOLEAN:  return d.asBool();
      case INT1:
      case INT2: return d.asInt2();
      case INT4: return d.asInt4();
      case INT8: return d.asInt8();
      case TEXT:
      case CHAR:
      case DATE:
      case VARCHAR:  return d.asChars();
      case FLOAT4:  return d.asFloat4();
      case FLOAT8:  return d.asFloat8();
      case NUMERIC:  return d.asFloat8();
      case TIME: {
        return ((TimeDatum)d).asChars(TajoConf.getCurrentTimeZone(), false);
      }
      case TIMESTAMP: {
        return ((TimestampDatum)d).asChars(TajoConf.getCurrentTimeZone(), false);
      }
      default: return d.asChars();
    }
  }

  @Override
  public Object getObject(String name) throws SQLException {
    return getObject(findColumn(name));
  }

  @Override
  public short getShort(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    handleNull(datum);
    if (wasNull) {
      return 0;
    }
    return datum.asInt2();
  }

  @Override
  public short getShort(String name) throws SQLException {
    Datum datum = cur.get(findColumn(name));
    handleNull(datum);
    if (wasNull) {
      return 0;
    }
    return datum.asInt2();
  }

  @Override
  public String getString(int fieldId) throws SQLException {
    Datum datum = cur.get(fieldId - 1);
    return getString(datum, fieldId);
  }

  @Override
  public String getString(String name) throws SQLException {
    int id = findColumn(name);
    Datum datum = cur.get(id);
    return getString(datum, id + 1);
  }

  private String getString(Datum datum, int fieldId) throws SQLException {
    handleNull(datum);

    if (wasNull) {
      return null;
    }

    TajoDataTypes.Type dataType = datum.type();

    switch(dataType) {
      case BOOLEAN:
        return String.valueOf(((BooleanDatum)datum).asBool());
      case TIME: {
        return ((TimeDatum)datum).asChars(TajoConf.getCurrentTimeZone(), false);
      }
      case TIMESTAMP: {
        return ((TimestampDatum)datum).asChars(TajoConf.getCurrentTimeZone(), false);
      }
      default :
        return datum.asChars();
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> clazz) throws SQLException {
    throw new SQLFeatureNotSupportedException("isWrapperFor not supported");
  }

  @Override
  public <T> T unwrap(Class<T> clazz) throws SQLException {
    throw new SQLFeatureNotSupportedException("unwrap not supported");
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException("absolute not supported");
  }

  @Override
  public void afterLast() throws SQLException {
    while (this.next())
      ;
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException("cancelRowUpdates not supported");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException("clearWarnings not supported");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("deleteRow not supported");
  }

  @Override
  public int findColumn(String colName) throws SQLException {
    return schema.getColumnIdByName(colName);
  }

  @Override
  public boolean first() throws SQLException {
    this.beforeFirst();
    return this.next();
  }

  @Override
  public Array getArray(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getArray not supported");
  }

  @Override
  public Array getArray(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getArray not supported");
  }

  @Override
  public InputStream getAsciiStream(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getAsciiStream not supported");
  }

  @Override
  public InputStream getAsciiStream(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getAsciiStream not supported");
  }

  @Override
  public BigDecimal getBigDecimal(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBigDecimal not supported");
  }

  @Override
  public BigDecimal getBigDecimal(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBigDecimal not supported");
  }

  @Override
  public BigDecimal getBigDecimal(int index, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBigDecimal not supported");
  }

  @Override
  public BigDecimal getBigDecimal(String name, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBigDecimal not supported");
  }

  @Override
  public InputStream getBinaryStream(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBinaryStream not supported");
  }

  @Override
  public InputStream getBinaryStream(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBinaryStream not supported");
  }

  @Override
  public Blob getBlob(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBlob not supported");
  }

  @Override
  public Blob getBlob(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getBlob not supported");
  }

  @Override
  public Reader getCharacterStream(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getCharacterStream not supported");
  }

  @Override
  public Reader getCharacterStream(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getCharacterStream not supported");
  }

  @Override
  public Clob getClob(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getClob not supported");
  }

  @Override
  public Clob getClob(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getClob not supported");
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException("getCursorName not supported");
  }

  @Override
  public Date getDate(int index) throws SQLException {
    Object obj = getObject(index);
    if (obj == null) {
      return null;
    }

    try {
      return Date.valueOf((String) obj);
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + index
          + " to date: " + e.toString());
    }
  }

  @Override
  public Date getDate(String name) throws SQLException {
    return getDate(findColumn(name));
  }

  @Override
  public Date getDate(int index, Calendar x) throws SQLException {
    throw new SQLFeatureNotSupportedException("getDate not supported");
  }

  @Override
  public Date getDate(String name, Calendar x) throws SQLException {
    throw new SQLFeatureNotSupportedException("getDate not supported");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("getFetchSize not supported");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("getHoldability not supported");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new TajoResultSetMetaData(schema);
  }

  @Override
  public Reader getNCharacterStream(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNCharacterStream not supported");
  }

  @Override
  public Reader getNCharacterStream(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNCharacterStream not supported");
  }

  @Override
  public NClob getNClob(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNClob not supported");
  }

  @Override
  public NClob getNClob(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNClob not supported");
  }

  @Override
  public String getNString(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNString not supported");
  }

  @Override
  public String getNString(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getNString not supported");
  }

  @Override
  public Object getObject(int index, Map<String, Class<?>> x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("getObject not supported");
  }

  @Override
  public Object getObject(String name, Map<String, Class<?>> x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("getObject not supported");
  }

  public <T> T getObject(String name, Class<T> x)
      throws SQLException {
    //JDK 1.7
    throw new SQLFeatureNotSupportedException("getObject not supported");
  }

  public <T> T getObject(int index, Class<T> x)
      throws SQLException {
    //JDK 1.7
    throw new SQLFeatureNotSupportedException("getObject not supported");
  }

  @Override
  public Ref getRef(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getRef not supported");
  }

  @Override
  public Ref getRef(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getRef not supported");
  }

  @Override
  public int getRow() throws SQLException {
    return curRow;
  }

  @Override
  public RowId getRowId(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getRowId not supported");
  }

  @Override
  public RowId getRowId(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getRowId not supported");
  }

  @Override
  public SQLXML getSQLXML(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getSQLXML not supported");
  }

  @Override
  public SQLXML getSQLXML(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getSQLXML not supported");
  }

  @Override
  public Statement getStatement() throws SQLException {
    throw new SQLFeatureNotSupportedException("getHistoryStatement not supported");
  }

  @Override
  public Time getTime(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTime not supported");
  }

  @Override
  public Time getTime(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTime not supported");
  }

  @Override
  public Time getTime(int index, Calendar x) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTime not supported");
  }

  @Override
  public Time getTime(String name, Calendar x) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTime not supported");
  }

  @Override
  public Timestamp getTimestamp(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTimestamp not supported");
  }

  @Override
  public Timestamp getTimestamp(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTimestamp not supported");
  }

  @Override
  public Timestamp getTimestamp(int index, Calendar x) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTimestamp not supported");
  }

  @Override
  public Timestamp getTimestamp(String name, Calendar x) throws SQLException {
    throw new SQLFeatureNotSupportedException("getTimestamp not supported");
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public URL getURL(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getURL not supported");
  }

  @Override
  public URL getURL(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getURL not supported");
  }

  @Override
  public InputStream getUnicodeStream(int index) throws SQLException {
    throw new SQLFeatureNotSupportedException("getUnicodeStream not supported");
  }

  @Override
  public InputStream getUnicodeStream(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getUnicodeStream not supported");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException("getWarnings not supported");
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("insertRow not supported");
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return this.curRow > this.totalRow;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return this.curRow == 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.curRow == -1;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return this.curRow == 1;
  }

  @Override
  public boolean isLast() throws SQLException {
    return this.curRow == this.totalRow;
  }

  @Override
  public boolean last() throws SQLException {
    Tuple last = null;
    while (this.next()) {
      last = cur;
    }
    cur = last;
    return true;
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("moveToCurrentRow not supported");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("moveToInsertRow not supported");
  }

  @Override
  public boolean next() throws SQLException {
    try {
      if (totalRow <= 0) {
        return false;
      }

      cur = nextTuple();
      curRow++;
      if (cur != null) {
        return true;
      }
    } catch (IOException e) {
      throw new SQLException(e.getMessage());
    }
    return false;
  }

  protected abstract Tuple nextTuple() throws IOException;

  @Override
  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException("previous not supported");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("refreshRow not supported");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("relative not supported");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException("rowDeleted not supported");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException("rowInserted not supported");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException("rowUpdated not supported");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException("setFetchDirection not supported");
  }

  @Override
  public void setFetchSize(int size) throws SQLException {
    throw new SQLFeatureNotSupportedException("setFetchSize not supported");
  }

  @Override
  public void updateArray(int index, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateArray not supported");
  }

  @Override
  public void updateArray(String name, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateArray not supported");
  }

  @Override
  public void updateAsciiStream(int index, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
  }

  @Override
  public void updateAsciiStream(String name, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
  }

  @Override
  public void updateAsciiStream(int index, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
  }

  @Override
  public void updateAsciiStream(String name, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
  }

  @Override
  public void updateAsciiStream(int index, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
  }

  @Override
  public void updateAsciiStream(String name, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
  }

  @Override
  public void updateBigDecimal(int index, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBigDecimal not supported");
  }

  @Override
  public void updateBigDecimal(String name, BigDecimal x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBigDecimal not supported");
  }

  @Override
  public void updateBinaryStream(int index, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
  }

  @Override
  public void updateBinaryStream(String name, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
  }

  @Override
  public void updateBinaryStream(int index, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
  }

  @Override
  public void updateBinaryStream(String name, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
  }

  @Override
  public void updateBinaryStream(int index, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
  }

  @Override
  public void updateBinaryStream(String name, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
  }

  @Override
  public void updateBlob(int index, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBlob not supported");
  }

  @Override
  public void updateBlob(String name, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBlob not supported");
  }

  @Override
  public void updateBlob(int index, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBlob not supported");
  }

  @Override
  public void updateBlob(String name, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBlob not supported");
  }

  @Override
  public void updateBlob(int index, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBlob not supported");
  }

  @Override
  public void updateBlob(String name, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBlob not supported");
  }

  @Override
  public void updateBoolean(int index, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBoolean not supported");
  }

  @Override
  public void updateBoolean(String name, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateBoolean not supported");
  }

  @Override
  public void updateByte(int index, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateByte not supported");
  }

  @Override
  public void updateByte(String name, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateByte not supported");
  }

  @Override
  public void updateBytes(int index, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateByte not supported");
  }

  @Override
  public void updateBytes(String name, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateByte not supported");
  }

  @Override
  public void updateCharacterStream(int index, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
  }

  @Override
  public void updateCharacterStream(String name, Reader x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
  }

  @Override
  public void updateCharacterStream(int index, Reader x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
  }

  @Override
  public void updateCharacterStream(String name, Reader x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
  }

  @Override
  public void updateCharacterStream(int index, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
  }

  @Override
  public void updateCharacterStream(String name, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
  }

  @Override
  public void updateClob(int index, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateClob not supported");
  }

  @Override
  public void updateClob(String name, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateClob not supported");
  }

  @Override
  public void updateClob(int index, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateClob not supported");
  }

  @Override
  public void updateClob(String name, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateClob not supported");
  }

  @Override
  public void updateClob(int index, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateClob not supported");
  }

  @Override
  public void updateClob(String name, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateClob not supported");
  }

  @Override
  public void updateDate(int index, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateDate not supported");
  }

  @Override
  public void updateDate(String name, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateDate not supported");
  }

  @Override
  public void updateDouble(int index, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateDouble not supported");
  }

  @Override
  public void updateDouble(String name, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateDouble not supported");
  }

  @Override
  public void updateFloat(int index, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateFloat not supported");
  }

  @Override
  public void updateFloat(String name, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateFloat not supported");
  }

  @Override
  public void updateInt(int index, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateInt not supported");
  }

  @Override
  public void updateInt(String name, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateInt not supported");
  }

  @Override
  public void updateLong(int index, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateLong not supported");
  }

  @Override
  public void updateLong(String name, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateLong not supported");
  }

  @Override
  public void updateNCharacterStream(int index, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
  }

  @Override
  public void updateNCharacterStream(String name, Reader x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
  }

  @Override
  public void updateNCharacterStream(int index, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
  }

  @Override
  public void updateNCharacterStream(String name, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
  }

  @Override
  public void updateNClob(int index, NClob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNClob not supported");
  }

  @Override
  public void updateNClob(String name, NClob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNClob not supported");
  }

  @Override
  public void updateNClob(int index, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNClob not supported");
  }

  @Override
  public void updateNClob(String name, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNClob not supported");
  }

  @Override
  public void updateNClob(int index, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNClob not supported");
  }

  @Override
  public void updateNClob(String name, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNClob not supported");
  }

  @Override
  public void updateNString(int arg0, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNString not supported");
  }

  @Override
  public void updateNString(String name, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNString not supported");
  }

  @Override
  public void updateNull(int arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNull not supported");
  }

  @Override
  public void updateNull(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateNull not supported");
  }

  @Override
  public void updateObject(int index, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateObject not supported");
  }

  @Override
  public void updateObject(String name, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateObject not supported");
  }

  @Override
  public void updateObject(int index, Object x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateObject not supported");
  }

  @Override
  public void updateObject(String name, Object x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("updateObject not supported");
  }

  @Override
  public void updateRef(int index, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateRef not supported");
  }

  @Override
  public void updateRef(String name, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateRef not supported");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("updateRow not supported");
  }

  @Override
  public void updateRowId(int index, RowId arg1) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateRowId not supported");
  }

  @Override
  public void updateRowId(String name, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateRowId not supported");
  }

  @Override
  public void updateSQLXML(int index, SQLXML arg1) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateSQLXML not supported");
  }

  @Override
  public void updateSQLXML(String name, SQLXML x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateSQLXML not supported");

  }

  @Override
  public void updateShort(int index, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateShort not supported");

  }

  @Override
  public void updateShort(String name, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateShort not supported");

  }

  @Override
  public void updateString(int index, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateString not supported");

  }

  @Override
  public void updateString(String name, String arg1) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateString not supported");

  }

  @Override
  public void updateTime(int index, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateTime not supported");

  }

  @Override
  public void updateTime(String name, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateTime not supported");

  }

  @Override
  public void updateTimestamp(int index, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateTimestamp not supported");

  }

  @Override
  public void updateTimestamp(String name, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("updateTimestamp not supported");

  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }
}
