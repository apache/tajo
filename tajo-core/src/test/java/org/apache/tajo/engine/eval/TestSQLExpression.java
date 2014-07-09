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

package org.apache.tajo.engine.eval;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.TimeZone;

import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;
import static org.junit.Assert.fail;

public class TestSQLExpression extends ExprTestBase {

  @Test
  public void testQuotedIdentifiers() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("컬럼1", TEXT);
    schema.addColumn("컬럼2", TEXT);
    testEval(schema, "테이블1", "123,234", "select \"컬럼1\"::float, cast (\"컬럼2\" as float4) as a from \"테이블1\"",
        new String[]{"123.0", "234.0"});
    testEval(schema,
        "테이블1", "123,234", "select char_length(\"컬럼1\"), \"컬럼2\"::float4 as \"별명1\" from \"테이블1\"",
        new String[]{"3", "234.0"});
  }

  @Test
  public void testNoSuchFunction() throws IOException {
    try {
      testSimpleEval("select test123('abc') col1 ", new String[]{"abc"});
      fail("This test should throw NoSuchFunctionException");
    } catch (NoSuchFunctionException e) {
      //success
    } catch (Exception e) {
      fail("This test should throw NoSuchFunctionException: " + e);
    }
  }

  @Test
  public void testSQLStandardCast() throws IOException {
    testSimpleEval("select cast (1 as char)", new String[] {"1"});
    testSimpleEval("select cast (119 as char)", new String[] {"1"});

    testSimpleEval("select cast (1 as int2)", new String[ ]{"1"});
    testSimpleEval("select cast (1 as int4)", new String[] {"1"});
    testSimpleEval("select cast (1 as int8)", new String[] {"1"});
    testSimpleEval("select cast (1 as float)", new String[] {"1.0"});
    testSimpleEval("select cast (1 as double)", new String[] {"1.0"});
    testSimpleEval("select cast (1 as text)", new String[] {"1"});

    testSimpleEval("select cast ('123' as int2)", new String[] {"123"});
    testSimpleEval("select cast ('123' as int4)", new String[] {"123"});
    testSimpleEval("select cast ('123' as int8)", new String[] {"123"});
    testSimpleEval("select cast ('123' as float)", new String[] {"123.0"});
    testSimpleEval("select cast ('123' as double)", new String[] {"123.0"});
    testSimpleEval("select cast ('123' as text)", new String[] {"123"});

    testSimpleEval("select cast (123 as int2)", new String[] {"123"});
    testSimpleEval("select cast (123 as int4)", new String[] {"123"});
    testSimpleEval("select cast (123 as int8)", new String[] {"123"});
    testSimpleEval("select cast (123 as float)", new String[] {"123.0"});
    testSimpleEval("select cast (123 as double)", new String[] {"123.0"});
    testSimpleEval("select cast (123 as text)", new String[] {"123"});

    testSimpleEval("select cast (123.0 as float)", new String[] {"123.0"});
    testSimpleEval("select cast (123.0 as double)", new String[] {"123.0"});
  }

  @Test
  public void testExplicitCast() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.INT1);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", CatalogUtil.newDataType(TajoDataTypes.Type.CHAR, "", 3));

    testSimpleEval("select cast (1 as char)", new String[] {"1"});
    testSimpleEval("select cast (119 as char)", new String[] {"1"});

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
    Schema schema = new Schema();
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
    Schema schema = new Schema();
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
    Schema schema = new Schema();
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
        (new Integer(2) - 5.1d) +""});
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
    Schema schema = new Schema();
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
        (new Long(3) - 5.1d)+""});
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
    Schema schema = new Schema();
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
        (new Float(4.1) + 5.1d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col6::float4 from table1;", new String [] {"10.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 + col7::float4 from table1;", new String [] {"11.1"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col0 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col1 from table1;", new String [] {"3.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col2 from table1;", new String [] {"2.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col3 from table1;", new String [] {
        (new Float(4.1) - new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col4 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col5 from table1;", new String [] {
        (new Float(4.1) - 5.1d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col6::float4 from table1;", new String [] {
        (4.1f - 6f)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 - col7::float4 from table1;", new String [] {
        (4.1f - 7f)+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col0 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col1 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col2 from table1;", new String [] {"8.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col3 from table1;", new String [] {
        (new Float(4.1) * new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col4 from table1;", new String [] {
        (new Float(4.1) * new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col5 from table1;", new String [] {
        (new Float(4.1) * 5.1d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col6::float4 from table1;", new String [] {
        (new Float(4.1) * 6f)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 * col7::float4 from table1;", new String [] {
        (new Float(4.1) * 7f)+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col1 from table1;", new String [] {
        (new Float(4.1) % new Integer(1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col2 from table1;", new String [] {
        (new Float(4.1) % new Integer(2))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col3 from table1;", new String [] {
        (new Float(4.1) % new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col4 from table1;", new String [] {
        (new Float(4.1) % new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col5 from table1;", new String [] {
        (new Float(4.1) % 5.1d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col6::float4 from table1;", new String [] {
        (new Float(4.1) % 6f)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col4 % col7::int1 from table1;", new String [] {
        (new Float(4.1) % 7f)+""});

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
    Schema schema = new Schema();
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
        (5.1d + 4.1f)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col5 from table1;", new String [] {"10.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col6::int1 from table1;", new String [] {"11.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 + col7::int1 from table1;", new String [] {"12.1"});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col0 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col1 from table1;", new String [] {"4.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col2 from table1;", new String [] {
        (5.1d - new Integer(2))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col3 from table1;", new String [] {
        (5.1d - 3l)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col4 from table1;", new String [] {
        (5.1d - 4.1f)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col5 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col6::float8 from table1;", new String [] {
        (5.1d - 6d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 - col7::float8 from table1;", new String [] {
        (5.1d - 7d)+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col0 from table1;", new String [] {"0.0"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col1 from table1;", new String [] {"5.1"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col2 from table1;", new String [] {"10.2"});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col3 from table1;", new String [] {
        (5.1d * new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col4 from table1;", new String [] {
        (5.1d * new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col5 from table1;", new String [] {
        (5.1d * 5.1d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col6::float8 from table1;", new String [] {
        (5.1d * 6d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 * col7::float8 from table1;", new String [] {
        (5.1d * 7d)+""});

    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col1 from table1;", new String [] {
        (5.1d % new Integer(1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col2 from table1;", new String [] {
        (5.1d % new Integer(2))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col3 from table1;", new String [] {
        (5.1d % new Long(3))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col4 from table1;", new String [] {
        (5.1d % new Float(4.1))+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col5 from table1;", new String [] {
        (5.1d % 5.1d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col6::float8 from table1;", new String [] {
        (5.1d % 6d)+""});
    testEval(schema, "table1", "0,1,2,3,4.1,5.1,6,7", "select col5 % col7::float8 from table1;", new String [] {
        (5.1d % 7d)+""});

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
  public void testSigned() throws IOException {
    Schema schema = new Schema();
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
  }

  @Test
  public void testCastWithNestedFunction() throws IOException {
    int unixtime = 1389071574;//(int) (System.currentTimeMillis() / 1000);
    TimestampDatum expected = DatumFactory.createTimestmpDatumWithUnixTime(unixtime);
    testSimpleEval(String.format("select to_timestamp(CAST(split_part('%d.999', '.', 1) as INT8));", unixtime),
        new String[] {expected.asChars(TajoConf.getCurrentTimeZone(), true)});
  }

  @Test
  public void testCastFromTable() throws IOException {
    TimeZone originTimeZone = TajoConf.setCurrentTimeZone(TimeZone.getTimeZone("GMT-6"));
    try {
      Schema schema = new Schema();
      schema.addColumn("col1", TEXT);
      schema.addColumn("col2", TEXT);
      testEval(schema, "table1", "123,234", "select cast(col1 as float) as b, cast(col2 as float) as a from table1",
          new String[]{"123.0", "234.0"});
      testEval(schema, "table1", "123,234", "select col1::float, col2::float from table1",
          new String[]{"123.0", "234.0"});

      TimestampDatum timestamp = DatumFactory.createTimestamp("1980-04-01 01:50:01" +
          DateTimeUtil.getTimeZoneDisplayTime(TajoConf.getCurrentTimeZone()));

      testEval(schema, "table1", "1980-04-01 01:50:01,234", "select col1::timestamp as t1, col2::float from table1 " +
              "where t1 = '1980-04-01 01:50:01'::timestamp",
          new String[]{timestamp.asChars(TajoConf.getCurrentTimeZone(), true), "234.0"}
      );

      testSimpleEval("select '1980-04-01 01:50:01'::timestamp;", new String[]{timestamp.asChars(TajoConf.getCurrentTimeZone(), true)});
      testSimpleEval("select '1980-04-01 01:50:01'::timestamp::text", new String[]{"1980-04-01 01:50:01"});

      testSimpleEval("select (cast ('99999'::int8 as text))::int4 + 1", new String[]{"100000"});
    } finally {
      TajoConf.setCurrentTimeZone(originTimeZone);
    }
  }

  @Test
  public void testBooleanLiteral() throws IOException {
    testSimpleEval("select true", new String[] {"t"});
    testSimpleEval("select false", new String[]{"f"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    testEval(schema, "table1", "123,234", "select col1, col2 from table1 where true", new String[]{"123", "234"});
  }

  @Test
  public void testNullComparisons() throws IOException {
    testSimpleEval("select null is null", new String[] {"t"});
    testSimpleEval("select null is not null", new String[] {"f"});

    testSimpleEval("select (1 = 1)", new String[] {"t"});

    testSimpleEval("select (1::int2 > null) is null", new String[] {"t"});
    testSimpleEval("select (1::int2 < null) is null", new String[] {"t"});
    testSimpleEval("select (1::int2 >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int2 <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int2 <> null) is null", new String[] {"t"});

    testSimpleEval("select (1::int4 > null) is null", new String[] {"t"});
    testSimpleEval("select (1::int4 < null) is null", new String[] {"t"});
    testSimpleEval("select (1::int4 >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int4 <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int4 <> null) is null", new String[] {"t"});

    testSimpleEval("select (1::int8 > null) is null", new String[] {"t"});
    testSimpleEval("select (1::int8 < null) is null", new String[] {"t"});
    testSimpleEval("select (1::int8 >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int8 <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::int8 <> null) is null", new String[] {"t"});

    testSimpleEval("select (1::float > null) is null", new String[] {"t"});
    testSimpleEval("select (1::float < null) is null", new String[] {"t"});
    testSimpleEval("select (1::float >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::float <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::float <> null) is null", new String[] {"t"});

    testSimpleEval("select (1::float8 > null) is null", new String[] {"t"});
    testSimpleEval("select (1::float8 < null) is null", new String[] {"t"});
    testSimpleEval("select (1::float8 >= null) is null", new String[] {"t"});
    testSimpleEval("select (1::float8 <= null) is null", new String[] {"t"});
    testSimpleEval("select (1::float8 <> null) is null", new String[] {"t"});

    testSimpleEval("select ('abc' > null) is null", new String[] {"t"});
    testSimpleEval("select ('abc' < null) is null", new String[] {"t"});
    testSimpleEval("select ('abc' >= null) is null", new String[] {"t"});
    testSimpleEval("select ('abc' <= null) is null", new String[] {"t"});
    testSimpleEval("select ('abc' <> null) is null", new String[] {"t"});

    testSimpleEval("select ('1980-04-01'::date > null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01'::date < null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01'::date >= null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01'::date <= null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01'::date <> null) is null", new String[] {"t"});

    testSimpleEval("select ('09:08:50'::time > null) is null", new String[] {"t"});
    testSimpleEval("select ('09:08:50'::time < null) is null", new String[] {"t"});
    testSimpleEval("select ('09:08:50'::time >= null) is null", new String[] {"t"});
    testSimpleEval("select ('09:08:50'::time <= null) is null", new String[] {"t"});
    testSimpleEval("select ('09:08:50'::time <> null) is null", new String[] {"t"});

    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp > null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp < null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp >= null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp <= null) is null", new String[] {"t"});
    testSimpleEval("select ('1980-04-01 01:50:30'::timestamp <> null) is null", new String[] {"t"});


    // Three Valued Logic - AND
    testSimpleEval("select (true AND true)", new String[] {"t"}); // true - true -> true
    testSimpleEval("select (true AND 1 > null) is null", new String[] {"t"}); // true - unknown -> unknown
    testSimpleEval("select (true AND false)", new String[] {"f"}); // true - false -> true

    testSimpleEval("select (1 > null AND true) is null", new String[] {"t"}); // unknown - true -> true
    testSimpleEval("select (1 > null AND 1 > null) is null", new String[] {"t"}); // unknown - unknown -> unknown
    testSimpleEval("select (1 > null AND false)", new String[] {"f"}); // unknown - false -> false

    testSimpleEval("select (false AND true)", new String[] {"f"}); // false - true -> true
    testSimpleEval("select (false AND 1 > null) is null", new String[] {"f"}); // false - unknown -> unknown
    testSimpleEval("select (false AND false)", new String[] {"f"}); // false - false -> false

    // Three Valued Logic - OR
    testSimpleEval("select (true OR true)", new String[] {"t"}); // true - true -> true
    testSimpleEval("select (true OR 1 > null)", new String[] {"t"}); // true - unknown -> true
    testSimpleEval("select (true OR false)", new String[] {"t"}); // true - false -> true

    testSimpleEval("select (1 > null OR true)", new String[] {"t"}); // unknown - true -> true
    testSimpleEval("select (1 > null OR 1 > null) is null", new String[] {"t"}); // unknown - unknown -> unknown
    testSimpleEval("select (1 > null OR false) is null", new String[] {"t"}); // unknown - false -> false

    testSimpleEval("select (false OR true)", new String[] {"t"}); // false - true -> true
    testSimpleEval("select (false OR 1 > null) is null", new String[] {"t"}); // false - unknown -> unknown
    testSimpleEval("select (false OR false)", new String[] {"f"}); // false - false -> false
  }
}
