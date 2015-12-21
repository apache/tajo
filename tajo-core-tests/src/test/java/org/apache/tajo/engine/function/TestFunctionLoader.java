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

package org.apache.tajo.engine.function;

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.AmbiguousFunctionException;
import org.apache.tajo.function.FunctionInvocation;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.FunctionSupplement;
import org.apache.tajo.util.StringUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.tajo.LocalTajoTestingUtility.getResultText;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestFunctionLoader {

  @Test
  public void testFindScalarFunctions() throws IOException {
    List<FunctionDesc> collections = Lists.newArrayList(FunctionLoader.findScalarFunctions());
    Collections.sort(collections);
    String functionList = StringUtils.join(collections, "\n");

    String result = getResultText(TestFunctionLoader.class, "testFindScalarFunctions.result");
    assertEquals(result.trim(), functionList.trim());
  }

  @Test
  public void testAmbiguousException() {
    FunctionSignature signature = new FunctionSignature(CatalogProtos.FunctionType.GENERAL, "test1",
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT),
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8));

    FunctionInvocation invocation = new FunctionInvocation();
    FunctionSupplement supplement = new FunctionSupplement();

    FunctionDesc desc = new FunctionDesc(signature, invocation, supplement);

    List<FunctionDesc> builtins = new ArrayList<>();
    builtins.add(desc);

    signature = new FunctionSignature(CatalogProtos.FunctionType.GENERAL, "test2",
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8),
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8));

    desc = new FunctionDesc(signature, invocation, supplement);
    builtins.add(desc);

    List<FunctionDesc> udfs = new ArrayList<>();

    signature = new FunctionSignature(CatalogProtos.FunctionType.UDF, "test1",
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT),
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8));

    desc = new FunctionDesc(signature, invocation, supplement);
    udfs.add(desc);

    boolean afexOccurs = false;

    try {
      FunctionLoader.mergeFunctionLists(builtins, udfs);
    } catch (AmbiguousFunctionException e) {
      afexOccurs = true;
    }

    assertTrue(afexOccurs);
  }
}