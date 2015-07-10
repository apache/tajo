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
import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;

import java.io.IOException;

public class TestEvalCodeGenerator extends ExprTestBase {
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

  @Test
  public void testArithmetic() throws IOException {
    testEval(schema, "testarithmetic", "0,1,2,3,4.5,6.5", "select 1+1;", new String [] {"2"});
    testEval(schema, "testarithmetic", "0,1,2,3,4.5,5.5", "select col1 + col2 from testarithmetic;", new String [] {"3"});
    testEval(schema, "testarithmetic", "0,1,2,3,4.5,5.5", "select col1 + col3 from testarithmetic;", new String [] {"4"});
    testEval(schema, "testarithmetic", "0,1,2,3,4.5,5.5", "select col1 + col4 from testarithmetic;", new String [] {"5.5"});
    testEval(schema, "testarithmetic", "0,1,2,3,4.5,5.5", "select col1 + col5 from testarithmetic;", new String [] {"6.5"});
  }

  @Test
  public void testGetField() throws IOException {
    testEval(schema, "testgetfield", "0,1,2,3,4.5,5.5,F6", "select col1 from testgetfield;", new String [] {"1"});
    testEval(schema, "testgetfield", "0,1,2,3,4.5,5.5,F6", "select col2 from testgetfield;", new String [] {"2"});
    testEval(schema, "testgetfield", "0,1,2,3,4.5,5.5,F6", "select col3 from testgetfield;", new String [] {"3"});
    testEval(schema, "testgetfield", "0,1,2,3,4.5,5.5,F6", "select col4 from testgetfield;", new String [] {"4.5"});
    testEval(schema, "testgetfield", "0,1,2,3,4.5,5.5,F6", "select col5 from testgetfield;", new String [] {"5.5"});
    testEval(schema, "testgetfield", "0,1,2,3,4.5,5.5,F6", "select col6 from testgetfield;", new String [] {"F6"});
    testEval(schema, "testgetfield", "0,1,2,3,4.5,5.5,F6,abc,t", "select col8 from testgetfield;", new String [] {"t"});
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

    testEval(schema, "testnullhandling", ",1,2,3,4.5,6.5,F6,abc,abc,t", "select col0 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,,2,3,4.5,6.5,F6,abc,abc,t,", "select col1 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,1,,3,4.5,6.5,F6,abc,abc,t,", "select col2 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,1,2,,4.5,6.5,F6,abc,abc,t,", "select col3 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,1,2,3,,6.5,F6,abc,abc,t,", "select col4 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,,F6,abc,abc,t,", "select col5 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,,abc,abc,t,", "select col6 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,F6,,abc,t,", "select col7 is null from testnullhandling;", new String[]{"t"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,F6,abc,,t,", "select col8 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,F6,abc,abc,,", "select col9 is null from testnullhandling;", new String [] {"t"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,F6,abc,abc,t,", "select nullable is null from testnullhandling;", new String [] {"t"});

    testEval(schema, "testnullhandling", ",1,2,3,4.5,6.5,F6,abc,abc,t", "select col0 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,,2,3,4.5,6.5,F6,abc,abc,t,", "select col1 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,,3,4.5,6.5,F6,abc,abc,t,", "select col2 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,2,,4.5,6.5,F6,abc,abc,t,", "select col3 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,2,3,,6.5,F6,abc,abc,t,", "select col4 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,,F6,abc,abc,t,", "select col5 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,,abc,abc,t,", "select col6 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,F6,,abc,t,", "select col7 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,F6,abc,,t,", "select col8 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,F6,abc,abc,,", "select col9 is not null from testnullhandling;", new String [] {"f"});
    testEval(schema, "testnullhandling", "0,1,2,3,4.5,6.5,F6,abc,abc,t,", "select nullable is not null from testnullhandling;", new String [] {"f"});
  }

  @Test
  public void testComparison() throws IOException {
    Schema inetSchema = new Schema();
    inetSchema.addColumn("addr1", TajoDataTypes.Type.INET4);
    inetSchema.addColumn("addr2", TajoDataTypes.Type.INET4);

    testSimpleEval("select (1 > null AND false)", new String[] {"f"}); // unknown - false -> false
    testSimpleEval("select (1::int8 > null) is null", new String[] {"t"});

    testSimpleEval("select 1 = null;", new String [] {""});
    testSimpleEval("select 1 <> null;", new String [] {""});
    testSimpleEval("select 1 > null;", new String [] {""});
    testSimpleEval("select 1 >= null;", new String [] {""});
    testSimpleEval("select 1 < null;", new String [] {""});
    testSimpleEval("select 1 <= null;", new String [] {""});

    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 = col1 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 = col2 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 = col3 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 = col4 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 = col5 from testcomparison;", new String [] {"f"});

    testEval(inetSchema, "testcomparison", "192.168.0.1,192.168.0.1", "select addr1 = addr2 from testcomparison;", new String[]{"t"});
    testEval(inetSchema, "testcomparison", "192.168.0.1,192.168.0.2", "select addr1 = addr2 from testcomparison;", new String[]{"f"});

    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <> col1 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <> col2 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <> col3 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <> col4 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <> col5 from testcomparison;", new String [] {"t"});

    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 < col1 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 < col2 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 < col3 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 < col4 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 1 < col5 from testcomparison;", new String [] {"t"});

    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <= col1 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <= col2 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <= col3 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <= col4 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 <= col5 from testcomparison;", new String [] {"t"});

    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 > col1 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 > col2 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 > col3 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 > col4 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 > col5 from testcomparison;", new String [] {"f"});

    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 >= col1 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 >= col2 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 >= col3 from testcomparison;", new String [] {"t"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 >= col4 from testcomparison;", new String [] {"f"});
    testEval(schema, "testcomparison", "0,1,2,3,4.5,6.5", "select 3 >= col5 from testcomparison;", new String [] {"f"});
  }

  @Test
  public void testBetweenAsymmetric() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT4);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    testEval(schema, "testbetweenasymmetric", "0,", "select col1 between 1 and 3 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "1,", "select col1 between 1 and 3 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "2,", "select col1 between 1 and 3 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "3,", "select col1 between 1 and 3 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "4,", "select col1 between 1 and 3 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "5,", "select (col2 between 1 and 3) is null from testbetweenasymmetric", new String[]{"t"});

    testEval(schema, "testbetweenasymmetric", "0,", "select col1 between 3 and 1 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "1,", "select col1 between 3 and 1 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "2,", "select col1 between 3 and 1 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "3,", "select col1 between 3 and 1 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "4,", "select col1 between 3 and 1 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "5,", "select (col2 between 3 and 1) is null from testbetweenasymmetric", new String[]{"t"});

    testEval(schema, "testbetweenasymmetric", "0,", "select col1 not between 1 and 3 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "1,", "select col1 not between 1 and 3 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "2,", "select col1 not between 1 and 3 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "3,", "select col1 not between 1 and 3 from testbetweenasymmetric", new String[]{"f"});
    testEval(schema, "testbetweenasymmetric", "4,", "select col1 not between 1 and 3 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "5,", "select (col2 not between 1 and 3) is null from testbetweenasymmetric", new String[]{"t"});

    testEval(schema, "testbetweenasymmetric", "0,", "select col1 not between 3 and 1 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "1,", "select col1 not between 3 and 1 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "2,", "select col1 not between 3 and 1 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "3,", "select col1 not between 3 and 1 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "4,", "select col1 not between 3 and 1 from testbetweenasymmetric", new String[]{"t"});
    testEval(schema, "testbetweenasymmetric", "5,", "select (col2 not between 3 and 1) is null from testbetweenasymmetric", new String[]{"t"});
  }

  @Test
  public void testBetweenSymmetric() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT4);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    testEval(schema, "testbetweensymmetric", "0,", "select col1 between symmetric 1 and 3 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "1,", "select col1 between symmetric 1 and 3 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "2,", "select col1 between symmetric 1 and 3 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "3,", "select col1 between symmetric 1 and 3 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "4,", "select col1 between symmetric 1 and 3 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "5,", "select (col2 between symmetric 1 and 3) is null from testbetweensymmetric", new String[]{"t"});

    testEval(schema, "testbetweensymmetric", "0,", "select col1 not between symmetric 1 and 3 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "1,", "select col1 not between symmetric 1 and 3 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "2,", "select col1 not between symmetric 1 and 3 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "3,", "select col1 not between symmetric 1 and 3 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "4,", "select col1 not between symmetric 1 and 3 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "5,", "select (col2 not between symmetric 1 and 3) is null from testbetweensymmetric", new String[]{"t"});

    testEval(schema, "testbetweensymmetric", "0,", "select col1 between symmetric 3 and 1 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "1,", "select col1 between symmetric 3 and 1 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "2,", "select col1 between symmetric 3 and 1 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "3,", "select col1 between symmetric 3 and 1 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "4,", "select col1 between symmetric 3 and 1 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "5,", "select (col2 between symmetric 3 and 1) is null from testbetweensymmetric", new String[]{"t"});

    testEval(schema, "testbetweensymmetric", "0,", "select col1 not between symmetric 3 and 1 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "1,", "select col1 not between symmetric 3 and 1 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "2,", "select col1 not between symmetric 3 and 1 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "3,", "select col1 not between symmetric 3 and 1 from testbetweensymmetric", new String[]{"f"});
    testEval(schema, "testbetweensymmetric", "4,", "select col1 not between symmetric 3 and 1 from testbetweensymmetric", new String[]{"t"});
    testEval(schema, "testbetweensymmetric", "5,", "select (col2 not between symmetric 3 and 1) is null from testbetweensymmetric",
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
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select +col1 from testunary;", new String [] {"1"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select +col2 from testunary;", new String [] {"2"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select +col3 from testunary;", new String [] {"3"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select +col4 from testunary;", new String [] {"4.1"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select +col5 from testunary;", new String [] {"5.1"});

    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select -col1 from testunary;", new String [] {"-1"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select -col2 from testunary;", new String [] {"-2"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select -col3 from testunary;", new String [] {"-3"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select -col4 from testunary;", new String [] {"-4.1"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select -col5 from testunary;", new String [] {"-5.1"});

    // not test
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select col8 from testunary;", new String [] {"t"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select NOT (col8) from testunary;", new String [] {"f"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,t", "select NOT(NOT (col8)) from testunary;", new String [] {"t"});

    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,", "select col8 is null from testunary;", new String [] {"t"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,", "select (NOT (col8)) is null from testunary;", new String [] {"t"});
    testEval(schema, "testunary", "0,1,2,3,4.1,5.1,6,7,", "select (NOT(NOT (col8))) is null from testunary;", new String [] {"t"});
  }

  @Test
  public void testAndOr() throws IOException {
    testSimpleEval("select true or (false or false) or false;", new String[] {"t"});

    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select true and true;", new String [] {"t"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select true and false;", new String [] {"f"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select false and true;", new String [] {"f"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select false and false;", new String [] {"f"});

    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select true or true;", new String [] {"t"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select true or false;", new String [] {"t"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select false or true;", new String [] {"t"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select false or false;", new String [] {"f"});

    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select (true and true) and false;", new String [] {"f"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select (true and false) and true;", new String [] {"f"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select (false and true) and true;", new String [] {"f"});

    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select (1 < 2) and true;", new String [] {"t"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select (1 < 2) and false;", new String [] {"f"});
    testEval(schema, "testandor", "0,1,2,3,4.5,6.5", "select (1 < 2) or false;", new String [] {"t"});
  }

  @Test
  public void testFunction() throws IOException {
    testEval(schema, "testfunction", "0,1,2,3,4.5,6.5", "select upper('abc');", new String [] {"ABC"});
    testEval(schema, "testfunction", "0,1,2,3,4.5,6.5", "select upper('bbc');", new String [] {"BBC"});
    testEval(schema, "testfunction", "0,1,2,3,4.5,6.5", "select upper('chs');", new String [] {"CHS"});

    testSimpleEval("select ltrim('xxtrim', 'xx') ", new String[]{"trim"});
  }

  @Test
  public void testStringConcat() throws IOException {
    testSimpleEval("select length('123456') as col1 ", new String[]{"6"});

    testEval(schema, "teststringconcat", "0,1,2,3,4.5,6.5", "select 'abc' || 'bbc'", new String [] {"abcbbc"});
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.TEXT);
    schema.addColumn("col2", TajoDataTypes.Type.TEXT);
    testEval(schema, "teststringconcat", " trim, abc", "select ltrim(col1) || ltrim(col2) from teststringconcat",
        new String[]{"trimabc"});
  }
}
