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

package org.apache.tajo.jdbc.util;

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestResultSetUtil {

  @Test
  public void testDataTypes() throws Exception {
    for (DataType type : getDataTyps()) {
      validate(type);
    }
  }

  private void validate(DataType type) throws SQLException {
    int sqlType = ResultSetUtil.tajoTypeToSqlType(type);

    switch (type.getType()) {

    case BIT:
      assertEquals(Types.BIT, sqlType);
      assertEquals("bit", ResultSetUtil.toSqlType(type));
      assertEquals(1, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(1, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case BOOLEAN:
      assertEquals(Types.BOOLEAN, sqlType);
      assertEquals("boolean", ResultSetUtil.toSqlType(type));
      assertEquals(1, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(1, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case INT1:
      assertEquals(Types.TINYINT, sqlType);
      assertEquals("tinyint", ResultSetUtil.toSqlType(type));
      assertEquals(4, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(3, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case INT2:
      assertEquals(Types.SMALLINT, sqlType);
      assertEquals("smallint", ResultSetUtil.toSqlType(type));
      assertEquals(6, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(5, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case INT4:
      assertEquals(Types.INTEGER, sqlType);
      assertEquals("integer", ResultSetUtil.toSqlType(type));
      assertEquals(11, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(10, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case INT8:
      assertEquals(Types.BIGINT, sqlType);
      assertEquals("bigint", ResultSetUtil.toSqlType(type));
      assertEquals(20, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(19, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case FLOAT4:
      assertEquals(Types.FLOAT, sqlType);
      assertEquals("float", ResultSetUtil.toSqlType(type));
      assertEquals(24, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(7, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(7, ResultSetUtil.columnScale(sqlType));
      break;
    case FLOAT8:
      assertEquals(Types.DOUBLE, sqlType);
      assertEquals("double", ResultSetUtil.toSqlType(type));
      assertEquals(25, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(15, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(15, ResultSetUtil.columnScale(sqlType));
      break;
    case DATE:
      assertEquals(Types.DATE, sqlType);
      assertEquals("date", ResultSetUtil.toSqlType(type));
      assertEquals(10, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(10, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case TIMESTAMP:
      assertEquals(Types.TIMESTAMP, sqlType);
      assertEquals("timestamp", ResultSetUtil.toSqlType(type));
      assertEquals(29, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(29, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(9, ResultSetUtil.columnScale(sqlType));
      break;
    case TIME:
      assertEquals(Types.TIME, sqlType);
      assertEquals("time", ResultSetUtil.toSqlType(type));
      assertEquals(18, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(18, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(9, ResultSetUtil.columnScale(sqlType));
      break;
    case CHAR:
    case VARCHAR:
    case TEXT:
      assertEquals(Types.VARCHAR, sqlType);
      assertEquals("varchar", ResultSetUtil.toSqlType(type));
      assertEquals(Integer.MAX_VALUE, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(Integer.MAX_VALUE, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case BLOB:
      assertEquals(Types.BLOB, sqlType);
      assertEquals("blob", ResultSetUtil.toSqlType(type));
      assertEquals(0, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(Integer.MAX_VALUE, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case RECORD:
      assertEquals(Types.STRUCT, sqlType);
      assertEquals("struct", ResultSetUtil.toSqlType(type));
      assertEquals(0, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(Integer.MAX_VALUE, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    case NULL_TYPE:
      assertEquals(Types.NULL, sqlType);
      assertEquals("null", ResultSetUtil.toSqlType(type));
      assertEquals(4, ResultSetUtil.columnDisplaySize(sqlType));
      assertEquals(0, ResultSetUtil.columnPrecision(sqlType));
      assertEquals(0, ResultSetUtil.columnScale(sqlType));
      break;
    default:
      fail("Unrecognized column type: " + type);
      break;
    }
  }

  private List<DataType> getDataTyps() {
    List<DataType> typeList = Lists.newArrayList();
    for (Type type : getSupportDataTyps()) {
      typeList.add(CatalogUtil.newSimpleDataType(type));
    }
    return typeList;
  }

  private Type[] getSupportDataTyps() {
    return new Type[]{Type.NULL_TYPE, Type.BOOLEAN, Type.BIT,
        Type.INT1, Type.INT2, Type.INT4, Type.INT8,
        Type.FLOAT4, Type.FLOAT8,
        Type.CHAR, Type.VARCHAR, Type.TEXT,
        Type.DATE, Type.TIME, Type.TIMESTAMP,
        Type.BLOB,
        Type.RECORD};
  }
}
