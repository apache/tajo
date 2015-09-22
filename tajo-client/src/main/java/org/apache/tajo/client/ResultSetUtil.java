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

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.TajoRuntimeException;
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
        sb.append(String.valueOf(res.getObject(i)));
      }
      sb.append("\n");
    }

    return sb.toString();
  }

  public static String toSqlType(DataType type) {
    switch (type.getType()) {
    case BIT:
      return "bit";
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
      return "double";
    case NUMERIC:
      return "numeric";
    case VARBINARY:
      return "bytea";
    case DATE:
      return "date";
    case TIMESTAMP:
      return "timestamp";
    case TIME:
      return "time";
    case CHAR:
    case VARCHAR:
    case TEXT:
      return "varchar";
    case BLOB:
      return "blob";
    case RECORD:
      return "struct";
    case NULL_TYPE:
      return "null";
    default:
      throw new TajoRuntimeException(new UnsupportedException("unknown data type '" + type.getType().name() + "'"));
    }
  }

  public static int tajoTypeToSqlType(DataType type) throws SQLException {
    switch (type.getType()) {
    case BIT:
      return Types.BIT;
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
    case CHAR:
    case VARCHAR:
    case TEXT:
      return Types.VARCHAR;
    case BLOB:
      return Types.BLOB;
    case RECORD:
      return Types.STRUCT;
    case NULL_TYPE:
      return Types.NULL;
    default:
      throw new SQLException("Unrecognized column type: " + type);
    }
  }

  public static int columnDisplaySize(int columnType) throws SQLException {

    switch (columnType) {
    case Types.BIT:
    case Types.BOOLEAN:
    case Types.CHAR:
    case Types.VARCHAR:
      return columnPrecision(columnType);
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      return columnPrecision(columnType) + 1; // allow +/-
    case Types.TIMESTAMP:
    case Types.DATE:
    case Types.TIME:
      return columnPrecision(columnType);
    // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
    case Types.FLOAT:
      return 24; // e.g. -(17#).e-###
    // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
    case Types.DOUBLE:
      return 25; // e.g. -(17#).e-####
    case Types.DECIMAL:
      return Integer.MAX_VALUE;
    case Types.NULL:
      return 4;
    case Types.BLOB:
    case Types.BINARY:
    case Types.ARRAY:
    case Types.STRUCT:
    default:
      return 0;  //unknown width
    }
  }

  public static int columnPrecision(int columnType) throws SQLException {

    switch (columnType) {
    case Types.BIT:
    case Types.BOOLEAN:
      return 1;
    case Types.CHAR:
    case Types.VARCHAR:
    case Types.BLOB:
    case Types.BINARY:
    case Types.STRUCT:
    case Types.ARRAY:
      return Integer.MAX_VALUE;
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
    case Types.DATE:
      return 10;
    case Types.TIME:
      return 18;
    case Types.TIMESTAMP:
      return 29;
    case Types.DECIMAL:
      return Integer.MAX_VALUE;
    case Types.NULL:
      return 0;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  public static int columnScale(int columnType) throws SQLException {

    switch (columnType) {
    case Types.BOOLEAN:
    case Types.CHAR:
    case Types.VARCHAR:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
    case Types.DATE:
    case Types.BIT:
    case Types.BLOB:
    case Types.BINARY:
    case Types.ARRAY:
    case Types.STRUCT:
    case Types.NULL:
      return 0;
    case Types.FLOAT:
      return 7;
    case Types.DOUBLE:
      return 15;
    case Types.TIME:
    case Types.TIMESTAMP:
      return 9;
    case Types.DECIMAL:
      return Integer.MAX_VALUE;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }
}
