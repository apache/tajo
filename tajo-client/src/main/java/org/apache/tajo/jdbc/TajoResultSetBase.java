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

import org.apache.tajo.QueryId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

public abstract class TajoResultSetBase implements ResultSet {
  protected final Map<String, String> clientSideSessionVars;
  protected TimeZone timezone;

  protected int curRow;
  protected long totalRow;
  protected boolean wasNull;
  protected Schema schema;
  protected Tuple cur;

  protected final QueryId queryId;

  public TajoResultSetBase(QueryId queryId, Schema schema, @Nullable Map<String, String> clientSideSessionVars) {
    this.queryId = queryId;
    this.schema = schema;
    this.clientSideSessionVars = clientSideSessionVars;

    if (clientSideSessionVars != null) {

      if (clientSideSessionVars.containsKey(SessionVars.TIMEZONE.name())) {
        String timezoneId = clientSideSessionVars.get(SessionVars.TIMEZONE.name());
        this.timezone = TimeZone.getTimeZone(timezoneId);
      } else {
        this.timezone = TimeZone.getDefault();
      }

    }
  }

  protected void init() {
    cur = null;
    curRow = 0;
    totalRow = 0;
    wasNull = false;
  }

  private boolean handleNull(Tuple tuple, int index) {
    return wasNull = tuple.isBlankOrNull(index);
  }

  protected Schema getSchema() throws SQLException {
    return schema;
  }

  public QueryId getQueryId() {
    return queryId;
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
    return getBoolean(cur, fieldId - 1);
  }

  @Override
  public boolean getBoolean(String colName) throws SQLException {
    return getBoolean(cur, findColumn(colName));
  }

  private boolean getBoolean(Tuple tuple, int index) {
    return handleNull(tuple, index) ? false : tuple.getBool(index);
  }

  @Override
  public byte getByte(int fieldId) throws SQLException {
    return getByte(cur, fieldId - 1);
  }

  @Override
  public byte getByte(String name) throws SQLException {
    return getByte(cur, findColumn(name));
  }

  private byte getByte(Tuple tuple, int index) {
    return handleNull(tuple, index) ? 0 : tuple.getByte(index);
  }

  @Override
  public byte[] getBytes(int fieldId) throws SQLException {
    return getBytes(cur, fieldId - 1);
  }

  @Override
  public byte[] getBytes(String name) throws SQLException {
    return getBytes(cur, findColumn(name));
  }

  private byte[] getBytes(Tuple tuple, int index) {
    return handleNull(tuple, index) ? null : tuple.getBytes(index);
  }

  @Override
  public double getDouble(int fieldId) throws SQLException {
    return getDouble(cur, fieldId - 1);
  }

  @Override
  public double getDouble(String name) throws SQLException {
    return getDouble(cur, findColumn(name));
  }

  private double getDouble(Tuple tuple, int index) {
    return handleNull(tuple, index) ? 0.0d : tuple.getFloat8(index);
  }

  @Override
  public float getFloat(int fieldId) throws SQLException {
    return getFloat(cur, fieldId - 1);
  }

  @Override
  public float getFloat(String name) throws SQLException {
    return getFloat(cur, findColumn(name));
  }

  private float getFloat(Tuple tuple, int index) throws SQLException {
    return handleNull(tuple, index) ? 0.0f : tuple.getFloat4(index);
  }

  @Override
  public int getInt(int fieldId) throws SQLException {
    return getInt(cur, fieldId - 1);
  }

  @Override
  public int getInt(String name) throws SQLException {
    return getInt(cur, findColumn(name));
  }

  private int getInt(Tuple tuple, int index) throws SQLException {
    return handleNull(tuple, index) ? 0 : tuple.getInt4(index);
  }

  @Override
  public long getLong(int fieldId) throws SQLException {
    return getLong(cur, fieldId - 1);
  }

  @Override
  public long getLong(String name) throws SQLException {
    return getLong(cur, findColumn(name));
  }

  private long getLong(Tuple tuple, int index) throws SQLException {
    return handleNull(tuple, index) ? 0 : tuple.getInt8(index);
  }

  @Override
  public Object getObject(int fieldId) throws SQLException {
    return getObject(cur, fieldId - 1);
  }

  @Override
  public Object getObject(String name) throws SQLException {
    return getObject(cur, findColumn(name));
  }

  private Object getObject(Tuple tuple, int index) {
    if (handleNull(tuple, index)) {
      return null;
    }

    TajoDataTypes.Type dataType = schema.getColumn(index).getDataType().getType();

    switch(dataType) {
      case BOOLEAN:  return tuple.getBool(index);
      case INT1:
      case INT2: return tuple.getInt2(index);
      case INT4: return tuple.getInt4(index);
      case INT8: return tuple.getInt8(index);
      case TEXT:
      case CHAR:
      case VARCHAR:  return tuple.getText(index);
      case FLOAT4:  return tuple.getFloat4(index);
      case FLOAT8:  return tuple.getFloat8(index);
      case NUMERIC:  return tuple.getFloat8(index);
      case DATE: {
        return toDate(tuple.getTimeDate(index), timezone);
      }
      case TIME: {
        return toTime(tuple.getTimeDate(index), timezone);
      }
      case TIMESTAMP: {
        return toTimestamp(tuple.getTimeDate(index), timezone);
      }
      default:
        return tuple.getText(index);
    }
  }

  @Override
  public short getShort(int fieldId) throws SQLException {
    return getShort(cur, fieldId - 1);
  }

  @Override
  public short getShort(String name) throws SQLException {
    return getShort(cur, findColumn(name));
  }

  private short getShort(Tuple tuple, int index) throws SQLException {
    return handleNull(tuple, index) ? 0 : tuple.getInt2(index);
  }

  @Override
  public String getString(int fieldId) throws SQLException {
    return getString(cur, fieldId - 1);
  }

  @Override
  public String getString(String name) throws SQLException {
    return getString(cur, findColumn(name));
  }

  private String getString(Tuple tuple, int index) throws SQLException {
    if (handleNull(tuple, index)) {
      return null;
    }

    switch(tuple.type(index)) {
      case BOOLEAN:
        return String.valueOf(tuple.getBool(index));
      case TIME:
        return TimeDatum.asChars(tuple.getTimeDate(index), timezone, false);
      case TIMESTAMP:
        return TimestampDatum.asChars(tuple.getTimeDate(index), timezone, false);
      default :
        return tuple.getText(index);
    }
  }

  @Override
  public Date getDate(int fieldId) throws SQLException {
    return getDate(cur, null, fieldId - 1);
  }

  @Override
  public Date getDate(String name) throws SQLException {
    return getDate(cur, null, findColumn(name));
  }

  @Override
  public Date getDate(int fieldId, Calendar x) throws SQLException {
    return getDate(cur, x.getTimeZone(), fieldId - 1);
  }

  @Override
  public Date getDate(String name, Calendar x) throws SQLException {
    return getDate(cur, x.getTimeZone(), findColumn(name));
  }

  private Date getDate(Tuple tuple, TimeZone tz, int index) throws SQLException {
    return handleNull(tuple, index) ? null : toDate(tuple.getTimeDate(index), tz);
  }

  private Date toDate(TimeMeta tm, TimeZone tz) {
    if (tz != null) {
      DateTimeUtil.toUserTimezone(tm, tz);
    }
    return new Date(DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp(tm)));
  }

  @Override
  public Time getTime(int fieldId) throws SQLException {
    return getTime(cur, null, fieldId - 1);
  }

  @Override
  public Time getTime(String name) throws SQLException {
    return getTime(cur, null, findColumn(name));
  }

  @Override
  public Time getTime(int fieldId, Calendar x) throws SQLException {
    return getTime(cur, x.getTimeZone(), fieldId - 1);
  }

  @Override
  public Time getTime(String name, Calendar x) throws SQLException {
    return getTime(cur, x.getTimeZone(), findColumn(name));
  }

  private Time getTime(Tuple tuple, TimeZone tz, int index) throws SQLException {
    return handleNull(tuple, index) ? null : toTime(tuple.getTimeDate(index), tz);
  }

  private Time toTime(TimeMeta tm, TimeZone tz) {
    if (tz != null) {
      DateTimeUtil.toUserTimezone(tm, tz);
    }
    return new Time(DateTimeUtil.toJavaTime(tm.hours, tm.minutes, tm.secs, tm.fsecs));
  }

  @Override
  public Timestamp getTimestamp(int fieldId) throws SQLException {
    return getTimestamp(cur, null, fieldId - 1);
  }

  @Override
  public Timestamp getTimestamp(String name) throws SQLException {
    return getTimestamp(cur, null, findColumn(name));
  }

  @Override
  public Timestamp getTimestamp(int fieldId, Calendar x) throws SQLException {
    return getTimestamp(cur, x.getTimeZone(), fieldId - 1);
  }

  @Override
  public Timestamp getTimestamp(String name, Calendar x) throws SQLException {
    return getTimestamp(cur, x.getTimeZone(), findColumn(name));
  }

  private Timestamp getTimestamp(Tuple tuple, TimeZone tz, int index) throws SQLException {
    return handleNull(tuple, index) ? null : toTimestamp(tuple.getTimeDate(index), tz);
  }

  private Timestamp toTimestamp(TimeMeta tm, TimeZone tz) {
    if (tz != null) {
      DateTimeUtil.toUserTimezone(tm, tz);
    }
    return new Timestamp(DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp(tm)));
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
    return getSchema().getColumnIdByName(colName);
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
    return new TajoResultSetMetaData(getSchema());
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
      throw new SQLException(e.getMessage(), e);
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
