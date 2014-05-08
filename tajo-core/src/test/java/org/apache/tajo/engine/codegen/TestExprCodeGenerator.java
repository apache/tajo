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

package org.apache.tajo.engine.codegen;


import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class TestExprCodeGenerator extends ExprTestBase {
  private static Schema schema;
  static {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));
    schema.addColumn("col8", TajoDataTypes.Type.BOOLEAN);
    schema.addColumn("nullable", TajoDataTypes.Type.NULL_TYPE);
  }

  public TestExprCodeGenerator() {
    super(true);
  }

  @Test
  public void testExplicitCast() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0::int1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1::int1 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2::int1 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3::int1 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4::int1 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5::int1 from table1;", new String [] {"5"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col6::int1 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col7::int1 from table1;", new String [] {"7"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0::int2 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1::int2 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2::int2 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3::int2 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4::int2 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5::int2 from table1;", new String [] {"5"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col6::int2 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col7::int2 from table1;", new String [] {"7"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0::int4 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1::int4 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2::int4 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3::int4 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4::int4 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5::int4 from table1;", new String [] {"5"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col6::int4 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col7::int4 from table1;", new String [] {"7"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0::int8 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1::int8 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2::int8 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3::int8 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4::int8 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5::int8 from table1;", new String [] {"5"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col6::int8 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col7::int8 from table1;", new String [] {"7"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0::float4 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1::float4 from table1;", new String [] {"1.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2::float4 from table1;", new String [] {"2.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3::float4 from table1;", new String [] {"3.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4::float4 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5::float4 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col6::float4 from table1;", new String [] {"6.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col7::float4 from table1;", new String [] {"7.0"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0::float8 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1::float8 from table1;", new String [] {"1.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2::float8 from table1;", new String [] {"2.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3::float8 from table1;", new String [] {"3.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4::float8 from table1;", new String[]
        {Double.valueOf(4.1f).toString()});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5::float8 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col6::float8 from table1;", new String [] {"6.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col7::float8 from table1;", new String [] {"7.0"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0::text from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1::text from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2::text from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3::text from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4::text from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5::text from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col6::text from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col7::text from table1;", new String [] {"7"});
  }

  @Test
  public void testImplicitCastForInt1() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 + col0 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 + col1 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 + col2 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 + col3 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 + col4 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 + col5 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 + col6::int1 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 + col7::int1 from table1;", new String [] {"7"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 - col0 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 - col1 from table1;", new String [] {"-1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 - col2 from table1;", new String [] {"-2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 - col3 from table1;", new String [] {"-3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 - col4 from table1;", new String [] {"-4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 - col5 from table1;", new String [] {"-5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 - col6::int1 from table1;", new String [] {"-6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 - col7::int1 from table1;", new String [] {"-7"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 * col0 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 * col1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 * col2 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 * col3 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 * col4 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 * col5 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 * col6::int1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 * col7::int1 from table1;", new String [] {"0"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 % col1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 % col2 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 % col3 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 % col4 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 % col5 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 % col6::int1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 % col7::int1 from table1;", new String [] {"0"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 = col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 = col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 = col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 = col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 = col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 = col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 = col6::int1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 = col7::int1 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <> col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <> col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <> col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <> col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <> col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <> col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <> col6::int1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <> col7::int1 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 > col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 > col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 > col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 > col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 > col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 > col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 > col6::int1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 > col7::int1 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 >= col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 >= col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 >= col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 >= col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 >= col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 >= col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 >= col6::int1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 >= col7::int1 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 < col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 < col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 < col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 < col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 < col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 < col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 < col6::int1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 < col7::int1 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <= col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <= col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <= col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <= col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <= col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <= col6::int1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col0 <= col7::int1 from table1;", new String [] {"t"});
  }

  @Test
  public void testImplicitCastForInt2() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 + col0 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 + col1 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 + col2 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 + col3 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 + col4 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 + col5 from table1;", new String [] {"6.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 + col6::int2 from table1;", new String [] {"7"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 + col7::int2 from table1;", new String [] {"8"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 - col0 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 - col1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 - col2 from table1;", new String [] {"-1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 - col3 from table1;", new String [] {"-2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 - col4 from table1;", new String [] {"-3.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 - col5 from table1;", new String [] {"-4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 - col6::int2 from table1;", new String [] {"-5"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 - col7::int2 from table1;", new String [] {"-6"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 * col0 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 * col1 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 * col2 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 * col3 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 * col4 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 * col5 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 * col6::int2 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 * col7::int2 from table1;", new String [] {"7"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 % col1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 % col2 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 % col3 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 % col4 from table1;", new String [] {"1.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 % col5 from table1;", new String [] {"1.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 % col6::int2 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 % col7::int2 from table1;", new String [] {"1"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 = col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 = col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 = col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 = col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 = col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 = col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 = col6::int2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 = col7::int2 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <> col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <> col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <> col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <> col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <> col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <> col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <> col6::int2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <> col7::int2 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 > col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 > col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 > col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 > col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 > col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 > col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 > col6::int2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 > col7::int2 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 >= col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 >= col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 >= col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 >= col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 >= col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 >= col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 >= col6::int2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 >= col7::int2 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 < col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 < col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 < col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 < col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 < col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 < col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 < col6::int2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 < col7::int2 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <= col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <= col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <= col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <= col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <= col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <= col6::int2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col1 <= col7::int2 from table1;", new String [] {"t"});
  }

  @Test
  public void testImplicitCastForInt4() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 + col0 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 + col1 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 + col2 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 + col3 from table1;", new String [] {"5"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 + col4 from table1;", new String [] {"6.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 + col5 from table1;", new String [] {"7.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 + col6::int4 from table1;", new String [] {"8"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 + col7::int4 from table1;", new String [] {"9"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 - col0 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 - col1 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 - col2 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 - col3 from table1;", new String [] {"-1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 - col4 from table1;", new String [] {"-2.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 - col5 from table1;", new String [] {
        (new Integer(2) - new Double(5.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 - col6::int4 from table1;", new String [] {"-4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 - col7::int4 from table1;", new String [] {"-5"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 * col0 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 * col1 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 * col2 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 * col3 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 * col4 from table1;", new String [] {"8.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 * col5 from table1;", new String [] {"10.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 * col6::int4 from table1;", new String [] {"12"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 * col7::int4 from table1;", new String [] {"14"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 % col1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 % col2 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 % col3 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 % col4 from table1;", new String [] {"2.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 % col5 from table1;", new String [] {"2.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 % col6::int4 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 % col7::int4 from table1;", new String [] {"2"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 = col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 = col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 = col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 = col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 = col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 = col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 = col6::int4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 = col7::int4 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <> col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <> col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <> col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <> col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <> col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <> col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <> col6::int4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <> col7::int4 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 > col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 > col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 > col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 > col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 > col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 > col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 > col6::int4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 > col7::int4 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 >= col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 >= col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 >= col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 >= col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 >= col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 >= col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 >= col6::int4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 >= col7::int4 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 < col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 < col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 < col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 < col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 < col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 < col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 < col6::int4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 < col7::int4 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <= col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <= col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <= col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <= col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <= col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <= col6::int4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col2 <= col7::int4 from table1;", new String [] {"t"});
  }

  @Test
  public void testImplicitCastForInt8() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 + col0 from table1;", new String[]{"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 + col1 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 + col2 from table1;", new String [] {"5"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 + col3 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 + col4 from table1;", new String [] {
        (new Long(3) + new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 + col5 from table1;", new String [] {"8.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 + col6::int8 from table1;", new String [] {"9"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 + col7::int8 from table1;", new String [] {"10"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 - col0 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 - col1 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 - col2 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 - col3 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 - col4 from table1;", new String [] {
        (new Long(3) - new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 - col5 from table1;", new String [] {
        (new Long(3) - new Double(5.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 - col6::int8 from table1;", new String [] {"-3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 - col7::int8 from table1;", new String [] {"-4"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 * col0 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 * col1 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 * col2 from table1;", new String [] {"6"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 * col3 from table1;", new String [] {"9"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 * col4 from table1;", new String [] {
        (new Long(3) * new Float("4.1"))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 * col5 from table1;", new String [] {
        (new Long(3) * new Double("5.1"))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 * col6::int8 from table1;", new String [] {"18"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 * col7::int8 from table1;", new String [] {"21"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 % col1 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 % col2 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 % col3 from table1;", new String [] {"0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 % col4 from table1;", new String [] {"3.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 % col5 from table1;", new String [] {"3.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 % col6::int8 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 % col7::int8 from table1;", new String [] {"3"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 = col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 = col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 = col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 = col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 = col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 = col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 = col6::int8 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 = col7::int8 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <> col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <> col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <> col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <> col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <> col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <> col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <> col6::int8 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <> col7::int8 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 > col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 > col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 > col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 > col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 > col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 > col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 > col6::int8 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 > col7::int8 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 >= col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 >= col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 >= col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 >= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 >= col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 >= col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 >= col6::int8 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 >= col7::int8 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 < col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 < col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 < col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 < col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 < col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 < col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 < col6::int8 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 < col7::int8 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <= col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <= col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <= col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <= col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <= col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <= col6::int8 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col3 <= col7::int8 from table1;", new String [] {"t"});
  }

  @Test
  public void testImplicitCastForFloat4() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col0 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col1 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col2 from table1;", new String [] {"6.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col3 from table1;", new String [] {"7.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col4 from table1;", new String [] {"8.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col5 from table1;", new String [] {
        (new Float(4.1) + new Double(5.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col6::float4 from table1;", new String [] {"10.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col7::float4 from table1;", new String [] {"11.1"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col0 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col1 from table1;", new String [] {"3.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col2 from table1;", new String [] {"2.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col3 from table1;", new String [] {
        (new Float(4.1) - new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col4 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col5 from table1;", new String [] {
        (new Float(4.1) - new Double(5.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col6::float4 from table1;", new String [] {
        (new Float(4.1) - new Float(6))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col7::float4 from table1;", new String [] {
        (new Float(4.1) - new Float(7))+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col0 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col1 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col2 from table1;", new String [] {"8.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col3 from table1;", new String [] {
        (new Float(4.1) * new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col4 from table1;", new String [] {
        (new Float(4.1) * new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col5 from table1;", new String [] {
        (new Float(4.1) * new Double(5.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col6::float4 from table1;", new String [] {
        (new Float(4.1) * new Float(6))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col7::float4 from table1;", new String [] {
        (new Float(4.1) * new Float(7))+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col1 from table1;", new String [] {
        (new Float(4.1) % new Integer(1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col2 from table1;", new String [] {
        (new Float(4.1) % new Integer(2))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col3 from table1;", new String [] {
        (new Float(4.1) % new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col4 from table1;", new String [] {
        (new Float(4.1) % new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col5 from table1;", new String [] {
        (new Float(4.1) % new Double(5.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col6::float4 from table1;", new String [] {
        (new Float(4.1) % new Float(6))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col7::int1 from table1;", new String [] {
        (new Float(4.1) % new Float(7))+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 = col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 = col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 = col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 = col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 = col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 = col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 = col6::int1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 = col7::int1 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <> col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <> col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <> col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <> col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <> col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <> col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <> col6::int1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <> col7::int1 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 > col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 > col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 > col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 > col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 > col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 > col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 > col6::int1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 > col7::int1 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 >= col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 >= col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 >= col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 >= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 >= col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 >= col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 >= col6::int1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 >= col7::int1 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 < col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 < col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 < col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 < col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 < col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 < col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 < col6::int1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 < col7::int1 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <= col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <= col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <= col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <= col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <= col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <= col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <= col6::int1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 <= col7::int1 from table1;", new String [] {"t"});
  }

  @Test
  public void testImplicitCastForFloat8() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col0 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col1 from table1;", new String [] {"6.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col2 from table1;", new String [] {"7.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col3 from table1;", new String [] {"8.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col4 from table1;", new String [] {
        (new Double(5.1) + new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col5 from table1;", new String [] {"10.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col6::int1 from table1;", new String [] {"11.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col7::int1 from table1;", new String [] {"12.1"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col0 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col1 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col2 from table1;", new String [] {
        (new Double(5.1) - new Integer(2))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col3 from table1;", new String [] {
        (new Double(5.1) - new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col4 from table1;", new String [] {
        (new Double(5.1) - new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col5 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col6::float8 from table1;", new String [] {
        (new Double(5.1) - new Double(6))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col7::float8 from table1;", new String [] {
        (new Double(5.1) - new Double(7))+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col0 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col1 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col2 from table1;", new String [] {"10.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col3 from table1;", new String [] {
        (new Double(5.1) * new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col4 from table1;", new String [] {
        (new Double(5.1) * new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col5 from table1;", new String [] {
        (new Double(5.1) * new Double(5.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col6::float8 from table1;", new String [] {
        (new Double(5.1) * new Double(6))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col7::float8 from table1;", new String [] {
        (new Double(5.1) * new Double(7))+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col1 from table1;", new String [] {
        (new Double(5.1) % new Integer(1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col2 from table1;", new String [] {
        (new Double(5.1) % new Integer(2))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col3 from table1;", new String [] {
        (new Double(5.1) % new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col4 from table1;", new String [] {
        (new Double(5.1) % new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col5 from table1;", new String [] {
        (new Double(5.1) % new Double(5.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col6::float8 from table1;", new String [] {
        (new Double(5.1) % new Double(6))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col7::float8 from table1;", new String [] {
        (new Double(5.1) % new Double(7))+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 = col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 = col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 = col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 = col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 = col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 = col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 = col6::float8 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 = col7::float8 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <> col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <> col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <> col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <> col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <> col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <> col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <> col6::float8 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <> col7::float8 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 > col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 > col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 > col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 > col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 > col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 > col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 > col6::float8 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 > col7::float8 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 >= col0 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 >= col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 >= col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 >= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 >= col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 >= col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 >= col6::float8 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 >= col7::float8 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 < col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 < col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 < col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 < col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 < col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 < col5 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 < col6::float8 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 < col7::float8 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <= col0 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <= col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <= col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <= col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <= col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <= col5 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <= col6::float8 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 <= col7::float8 from table1;", new String [] {"t"});
  }

  @Test
  public void testArithmetic() throws IOException {
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1+1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5", "select col1 + col2 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5", "select col1 + col3 from table1;", new String [] {"4"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5", "select col1 + col4 from table1;", new String [] {"5.5"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5", "select col1 + col5 from table1;", new String [] {"6.5"});
  }

  @Test
  public void testGetField() throws IOException {
    testEval(schema, "table1", "0,1,2,3,4.5,5.5,F6", "select col1 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5,F6", "select col2 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5,F6", "select col3 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5,F6", "select col4 from table1;", new String [] {"4.5"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5,F6", "select col5 from table1;", new String [] {"5.5"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5,F6", "select col6 from table1;", new String [] {"F6"});
    testEval(schema, "table1", "0,1,2,3,4.5,5.5,F6,abc,t", "select col8 from table1;", new String [] {"t"});
  }

  @Test
  public void testNullHandling() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 1));
    schema.addColumn("col8", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));
    schema.addColumn("col9", TajoDataTypes.Type.BOOLEAN);
    schema.addColumn("nullable", TajoDataTypes.Type.NULL_TYPE);

    testEval(schema, "table1", ",1,2,3,4.5,6.5,F6,abc,abc,t", "select col0 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,,2,3,4.5,6.5,F6,abc,abc,t,", "select col1 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,,3,4.5,6.5,F6,abc,abc,t,", "select col2 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,,4.5,6.5,F6,abc,abc,t,", "select col3 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,,6.5,F6,abc,abc,t,", "select col4 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,,F6,abc,abc,t,", "select col5 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5,,abc,abc,t,", "select col6 is null from table1;", new String [] {"t"});
//    testEval(schema, "table1", "0,1,2,3,4.5,6.5,F6,,abc,t,", "select col7 is null from table1;", new String [] {"t"}); TODO
//    testEval(schema, "table1", "0,1,2,3,4.5,6.5,F6,abc,,t,", "select col8 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5,F6,abc,abc,,", "select col9 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5,F6,abc,abc,t,", "select nullable is null from table1;", new String [] {"t"});

    testEval(schema, "table1", ",1,2,3,4.5,6.5,F6,abc,abc,t", "select col0 is not null from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,,2,3,4.5,6.5,F6,abc,abc,t,", "select col1 is not null from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,,3,4.5,6.5,F6,abc,abc,t,", "select col2 is not null from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,,4.5,6.5,F6,abc,abc,t,", "select col3 is not null from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,,6.5,F6,abc,abc,t,", "select col4 is not null from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,,F6,abc,abc,t,", "select col5 is not null from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5,,abc,abc,t,", "select col6 is not null from table1;", new String [] {"f"});
//    testEval(schema, "table1", "0,1,2,3,4.5,6.5,F6,,abc,t,", "select col7 is not null from table1;", new String [] {"f"}); TODO
//    testEval(schema, "table1", "0,1,2,3,4.5,6.5,F6,abc,,t,", "select col8 is not null from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5,F6,abc,abc,,", "select col9 is not null from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5,F6,abc,abc,t,", "select nullable is not null from table1;", new String [] {"f"});
  }

  @Test
  public void testComparison() throws IOException {
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 = col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 = col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 = col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 = col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 = col5 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <> col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <> col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <> col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <> col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <> col5 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 < col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 < col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 < col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 < col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 1 < col5 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <= col1 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <= col2 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <= col4 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 <= col5 from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 > col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 > col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 > col3 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 > col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 > col5 from table1;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 >= col1 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 >= col2 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 >= col3 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 >= col4 from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select 3 >= col5 from table1;", new String [] {"f"});
  }

  @Test
  public void testBetweenAsymmetric() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT4);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    testEval(schema, "table1", "0,", "select col1 between 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "1,", "select col1 between 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "2,", "select col1 between 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "3,", "select col1 between 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "4,", "select col1 between 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "5,", "select (col2 between 1 and 3) is null from table1", new String[]{"t"});

    testEval(schema, "table1", "0,", "select col1 between 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "1,", "select col1 between 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "2,", "select col1 between 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "3,", "select col1 between 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "4,", "select col1 between 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "5,", "select (col2 between 3 and 1) is null from table1", new String[]{"t"});

    testEval(schema, "table1", "0,", "select col1 not between 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "1,", "select col1 not between 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "2,", "select col1 not between 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "3,", "select col1 not between 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "4,", "select col1 not between 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "5,", "select (col2 not between 1 and 3) is null from table1", new String[]{"t"});

    testEval(schema, "table1", "0,", "select col1 not between 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "1,", "select col1 not between 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "2,", "select col1 not between 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "3,", "select col1 not between 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "4,", "select col1 not between 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "5,", "select (col2 not between 3 and 1) is null from table1", new String[]{"t"});
  }

  @Test
  public void testBetweenSymmetric() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT4);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    testEval(schema, "table1", "0,", "select col1 between symmetric 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "1,", "select col1 between symmetric 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "2,", "select col1 between symmetric 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "3,", "select col1 between symmetric 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "4,", "select col1 between symmetric 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "5,", "select (col2 between symmetric 1 and 3) is null from table1", new String[]{"t"});

    testEval(schema, "table1", "0,", "select col1 not between symmetric 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "1,", "select col1 not between symmetric 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "2,", "select col1 not between symmetric 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "3,", "select col1 not between symmetric 1 and 3 from table1", new String[]{"f"});
    testEval(schema, "table1", "4,", "select col1 not between symmetric 1 and 3 from table1", new String[]{"t"});
    testEval(schema, "table1", "5,", "select (col2 not between symmetric 1 and 3) is null from table1", new String[]{"t"});

    testEval(schema, "table1", "0,", "select col1 between symmetric 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "1,", "select col1 between symmetric 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "2,", "select col1 between symmetric 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "3,", "select col1 between symmetric 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "4,", "select col1 between symmetric 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "5,", "select (col2 between symmetric 3 and 1) is null from table1", new String[]{"t"});

    testEval(schema, "table1", "0,", "select col1 not between symmetric 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "1,", "select col1 not between symmetric 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "2,", "select col1 not between symmetric 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "3,", "select col1 not between symmetric 3 and 1 from table1", new String[]{"f"});
    testEval(schema, "table1", "4,", "select col1 not between symmetric 3 and 1 from table1", new String[]{"t"});
    testEval(schema, "table1", "5,", "select (col2 not between symmetric 3 and 1) is null from table1",
        new String[]{"t"});
  }

  @Test
  public void testUnary() throws IOException {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));
    schema.addColumn("col8", TajoDataTypes.Type.BOOLEAN);


    // sign test
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select +col1 from table1;", new String [] {"1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select +col2 from table1;", new String [] {"2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select +col3 from table1;", new String [] {"3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select +col4 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select +col5 from table1;", new String [] {"5.1"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select -col1 from table1;", new String [] {"-1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select -col2 from table1;", new String [] {"-2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select -col3 from table1;", new String [] {"-3"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select -col4 from table1;", new String [] {"-4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select -col5 from table1;", new String [] {"-5.1"});

    // not test
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select col8 from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select NOT (col8) from table1;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,t", "select NOT(NOT (col8)) from table1;", new String [] {"t"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,", "select col8 is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,", "select (NOT (col8)) is null from table1;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7,", "select (NOT(NOT (col8))) is null from table1;", new String [] {"t"});
  }

  @Test
  public void testAndOr() throws IOException {
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select true and true;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select true and false;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select false and true;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select false and false;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select true or true;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select true or false;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select false or true;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select false or false;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select (true and true) and false;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select (true and false) and true;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select (false and true) and true;", new String [] {"f"});

    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select (1 < 2) and true;", new String [] {"t"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select (1 < 2) and false;", new String [] {"f"});
    testEval(schema, "table1", "0,1,2,3,4.5,6.5", "select (1 < 2) or false;", new String [] {"t"});
  }

  public static class NewMockUp {
    public Datum eval(int x, int y) {
      return null;
    }
  }

  public static byte[] getBytecodeForObjectReturn() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test3", null, "org/apache/tajo/engine/planner/TestExprCodeGenerator$NewMockUp", null);
    cw.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
        null, null).visitEnd();

    MethodVisitor methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$NewMockUp", "<init>", "()V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(1, 1);
    methodVisitor.visitEnd();

    methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "eval", "(II)Lorg/apache/tajo/datum/Datum;", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);

    methodVisitor.visitTypeInsn(Opcodes.NEW, "org/apache/tajo/datum/Int4Datum");
    methodVisitor.visitInsn(Opcodes.DUP);

    methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
    methodVisitor.visitVarInsn(Opcodes.ILOAD, 2);
    methodVisitor.visitInsn(Opcodes.IADD);

    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/datum/Int4Datum", "<init>", "(I)V");
    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "org/apache/tajo/datum/Datum");
    methodVisitor.visitInsn(Opcodes.ARETURN);
    methodVisitor.visitMaxs(0, 0);
    methodVisitor.visitEnd();
    cw.visitEnd();
    return cw.toByteArray();
  }

  public void testGenerateCodePlus() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
      InstantiationException {
    MyClassLoader myClassLoader = new MyClassLoader();
    Class aClass = myClassLoader.defineClass("org.Test2", getBytecodeForPlus());
    Constructor constructor = aClass.getConstructor();
    PlusExpr r = (PlusExpr) constructor.newInstance();
    System.out.println(r.eval(1, 3));
  }

  public static class PlusExpr {
    public int eval(int x, int y) {
      return x + y;
    }
  }

  public static byte[] getBytecodeForPlus() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test2", null, "org/apache/tajo/engine/planner/TestExprCodeGenerator$PlusExpr", null);
    cw.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
        null, null).visitEnd();

    System.out.println(Opcodes.ACC_PUBLIC);
    MethodVisitor methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$PlusExpr", "<init>", "()V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(1, 1);
    methodVisitor.visitEnd();

    methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "eval", "(II)I", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
    methodVisitor.visitVarInsn(Opcodes.ILOAD, 2);
    methodVisitor.visitInsn(Opcodes.IADD);
    methodVisitor.visitInsn(Opcodes.IRETURN);
    methodVisitor.visitMaxs(0, 0);
    methodVisitor.visitEnd();
    cw.visitEnd();
    return cw.toByteArray();
  }

  public static byte[] getBytecodeForClass() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test2", null, "org/apache/tajo/engine/planner/TestExprCodeGenerator$Example", null);
    cw.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
        null, null).visitEnd();

    System.out.println(Opcodes.ACC_PUBLIC);
    MethodVisitor methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$Example", "<init>", "()V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(1, 1);
    methodVisitor.visitEnd();


    methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "run", "(Ljava/lang/String;)V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/System", "currentTimeMillis", "()J");
    methodVisitor.visitVarInsn(Opcodes.LSTORE, 2);
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$Example", "run", "(Ljava/lang/String;)V");
    methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/System", "currentTimeMillis", "()J");
    methodVisitor.visitVarInsn(Opcodes.LLOAD, 2);
    methodVisitor.visitInsn(Opcodes.LSUB);
    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(J)V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(5, 4);
    methodVisitor.visitEnd();

    cw.visitEnd();
    return cw.toByteArray();
  }

  static class MyClassLoader extends ClassLoader {
    public Class defineClass(String name, byte[] b) {
      return defineClass(name, b, 0, b.length);
    }
  }
}
