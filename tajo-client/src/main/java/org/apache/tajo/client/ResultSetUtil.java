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

package org.apache.tajo.client;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.UnsupportedException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class ResultSetUtil {
  public static String prettyFormat(ResultSet res) throws SQLException {
    StringBuilder sb = new StringBuilder();
    ResultSetMetaData rsmd = res.getMetaData();
    int numOfColumns = rsmd.getColumnCount();

    for (int i = 1; i <= numOfColumns; i++) {
      if (i > 1) sb.append(",  ");
      String columnName = rsmd.getColumnName(i);
      sb.append(columnName);
    }
    sb.append("\n-------------------------------\n");

    while (res.next()) {
      for (int i = 1; i <= numOfColumns; i++) {
        if (i > 1) sb.append(",  ");
        String columnValue = res.getObject(i).toString();
        sb.append(columnValue);
      }
      sb.append("\n");
    }

    return sb.toString();
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
    case NUMERIC:
      return "numeric";
    case VARBINARY:
      return "bytea";
    case CHAR:
      return "character";
    case DATE:
      return "date";
    case TIMESTAMP:
      return "timestamp";
    case TIME:
      return "time";
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
    case NUMERIC:
      return Types.NUMERIC;
    case DATE:
      return Types.DATE;
    case TIMESTAMP:
      return Types.TIMESTAMP;
    case TIME:
      return Types.TIME;
    case VARCHAR:
      return Types.VARCHAR;
    case TEXT:
      return Types.VARCHAR;
    default:
      throw new SQLException("Unrecognized column type: " + type);
    }
  }

  public static int columnDisplaySize(int columnType) throws SQLException {
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

  public static int columnPrecision(int columnType) throws SQLException {
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

  public static int columnScale(int columnType) throws SQLException {
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
}
