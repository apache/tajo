/***
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

package org.apache.tajo.engine.function.hiveudf;

import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHiveFunctions extends ExprTestBase {
  @Test
  public void testFindFunction() throws Exception {
    CatalogService catService = getCluster().getCatalogService();

    FunctionDesc desc = catService.getFunction("my_upper", CatalogProtos.FunctionType.UDF,
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT));

    assertEquals(TajoDataTypes.Type.TEXT, desc.getReturnType().getType());
    assertEquals(1, desc.getParamTypes().length);
    assertEquals(TajoDataTypes.Type.TEXT, desc.getParamTypes()[0].getType());
    assertEquals("to uppercase", desc.getDescription());

    TajoDataTypes.DataType int4type = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4);
    desc = catService.getFunction("my_divide", CatalogProtos.FunctionType.UDF, int4type, int4type);

    assertEquals(TajoDataTypes.Type.FLOAT8, desc.getReturnType().getType());
    assertEquals(2, desc.getParamTypes().length);
    assertEquals(TajoDataTypes.Type.INT4, desc.getParamTypes()[0].getType());
    assertEquals(TajoDataTypes.Type.INT4, desc.getParamTypes()[1].getType());

    // synonym
    desc = catService.getFunction("test_upper", CatalogProtos.FunctionType.UDF,
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT));

    assertEquals(TajoDataTypes.Type.TEXT, desc.getReturnType().getType());
    assertEquals(1, desc.getParamTypes().length);
    assertEquals(TajoDataTypes.Type.TEXT, desc.getParamTypes()[0].getType());
    assertEquals("to uppercase", desc.getDescription());

    // Test for UDF without @Description and including multi 'evaluate()'
    desc = catService.getFunction("com_example_hive_udf_MyLower", CatalogProtos.FunctionType.UDF,
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT));

    assertEquals(TajoDataTypes.Type.TEXT, desc.getReturnType().getType());
    assertEquals(1, desc.getParamTypes().length);
    assertEquals(TajoDataTypes.Type.TEXT, desc.getParamTypes()[0].getType());

    // same function for another parameter signature
    desc = catService.getFunction("com_example_hive_udf_MyLower", CatalogProtos.FunctionType.UDF,
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT), CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT));

    assertEquals(TajoDataTypes.Type.TEXT, desc.getReturnType().getType());
    assertEquals(2, desc.getParamTypes().length);
    assertEquals(TajoDataTypes.Type.TEXT, desc.getParamTypes()[0].getType());
    assertEquals(TajoDataTypes.Type.TEXT, desc.getParamTypes()[1].getType());

    // multiple signatures
    // my_substr has two types, (Text, IntWritable) and (Text, IntWritable, IntWritable)
    desc = catService.getFunction("my_substr", CatalogProtos.FunctionType.UDF,
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT), CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4),
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4));
    assertEquals(TajoDataTypes.Type.TEXT, desc.getReturnType().getType());
    assertEquals(3, desc.getParamTypes().length);

    desc = catService.getFunction("my_substr", CatalogProtos.FunctionType.UDF,
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT), CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4));
    assertEquals(TajoDataTypes.Type.TEXT, desc.getReturnType().getType());
    assertEquals(2, desc.getParamTypes().length);
  }

  @Test
  public void testRunFunctions() throws Exception {
    testSimpleEval("select my_upper(null)", new String [] {"NULL"});
    testSimpleEval("select my_upper('abcd')", new String [] {"ABCD"});
    testSimpleEval("select my_divide(1,2)", new String [] {"0.5"});

    // my_substr() uses 1-based index
    testSimpleEval("select my_substr('abcde', 3)", new String [] {"cde"});
    testSimpleEval("select my_substr('abcde', 1, 2)", new String [] {"ab"});
  }
}