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

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.UnsupportedException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class TajoDriver implements Driver, Closeable {
  public static final int MAJOR_VERSION = 1;
  public static final int MINOR_VERSION = 0;

  public static final int JDBC_VERSION_MAJOR = 4;
  public static final int JDBC_VERSION_MINOR = 0;

  public static final String TAJO_JDBC_URL_PREFIX = "jdbc:tajo://";

  protected static TajoConf jdbcTajoConf;

  static {
    try {
      java.sql.DriverManager.registerDriver(new TajoDriver());
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public TajoDriver() {
    jdbcTajoConf = new TajoConf();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public Connection connect(String url, Properties properties) throws SQLException {
    return new TajoConnection(url, properties);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(TAJO_JDBC_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return MINOR_VERSION;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  public static String toSqlType(TajoDataTypes.DataType type) {
    switch (type.getType()) {
      case BOOLEAN:
        return "boolean";
      case INT1:
        return "tinyint";
      case INT2:
        return "smallint";
      case INT4:
        return "integer";
      case INT8:
        return "bigint";
      case FLOAT4:
        return "float";
      case FLOAT8:
        return "float8";
      case DECIMAL:
        return "numeric";
      case VARBINARY:
        return "bytea";
      case CHAR:
        return "character";
      case DATE:
        return "date";
      case VARCHAR:
        return "varchar";
      case TEXT:
        return "varchar";
      default:
        throw new UnsupportedException("Unrecognized column type:" + type);
    }
  }

  public static int tajoTypeToSqlType(TajoDataTypes.DataType type) throws SQLException {
    switch (type.getType()) {
      case BOOLEAN:
        return Types.BOOLEAN;
      case INT1:
        return Types.TINYINT;
      case INT2:
        return Types.SMALLINT;
      case INT4:
        return Types.INTEGER;
      case INT8:
        return Types.BIGINT;
      case FLOAT4:
        return Types.FLOAT;
      case FLOAT8:
        return Types.DOUBLE;
      case DECIMAL:
        return Types.DECIMAL;
      case DATE:
        return Types.TIMESTAMP;
      case VARCHAR:
        return Types.VARCHAR;
      case TEXT:
        return Types.VARCHAR;
      default:
        throw new SQLException("Unrecognized column type: " + type);
    }
  }

  static int columnDisplaySize(int columnType) throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    switch(columnType) {
      case Types.BOOLEAN:
        return columnPrecision(columnType);
      case Types.VARCHAR:
        return Integer.MAX_VALUE; // hive has no max limit for strings
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
        return columnPrecision(columnType) + 1; // allow +/-
      case Types.TIMESTAMP:
        return columnPrecision(columnType);
      // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
      case Types.FLOAT:
        return 24; // e.g. -(17#).e-###
      // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
      case Types.DOUBLE:
        return 25; // e.g. -(17#).e-####
      case Types.DECIMAL:
        return Integer.MAX_VALUE;
      default:
        throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static int columnPrecision(int columnType) throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    switch(columnType) {
      case Types.BOOLEAN:
        return 1;
      case Types.VARCHAR:
        return Integer.MAX_VALUE; // hive has no max limit for strings
      case Types.TINYINT:
        return 3;
      case Types.SMALLINT:
        return 5;
      case Types.INTEGER:
        return 10;
      case Types.BIGINT:
        return 19;
      case Types.FLOAT:
        return 7;
      case Types.DOUBLE:
        return 15;
      case Types.TIMESTAMP:
        return 29;
      case Types.DECIMAL:
        return Integer.MAX_VALUE;
      default:
        throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static int columnScale(int columnType) throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    switch(columnType) {
      case Types.BOOLEAN:
      case Types.VARCHAR:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
        return 0;
      case Types.FLOAT:
        return 7;
      case Types.DOUBLE:
        return 15;
      case Types.TIMESTAMP:
        return 9;
      case Types.DECIMAL:
        return Integer.MAX_VALUE;
      default:
        throw new SQLException("Invalid column type: " + columnType);
    }
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("getParentLogger not supported");
  }
}
