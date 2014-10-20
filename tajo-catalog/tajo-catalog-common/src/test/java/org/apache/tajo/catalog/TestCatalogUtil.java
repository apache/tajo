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

package org.apache.tajo.catalog;

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.function.FunctionUtil;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TestCatalogUtil {
  @Test
  public final void testGetCanonicalName() {
    String canonical = FunctionUtil.buildSimpleFunctionSignature("sum", CatalogUtil.newSimpleDataTypeArray(Type.INT4,
        Type.INT8));
    assertEquals("sum(int4,int8)", canonical);
  }

  String [] sources = {
      "A",
      "Column_Name",
      "COLUMN_NAME",
      "컬럼"
  };

  String [] normalized = {
      "a",
      "column_name",
      "column_name",
      "컬럼"
  };

  @Test
  public final void testNormalizeIdentifier() {
    for (int i = 0; i < sources.length; i++) {
      assertEquals(normalized[i], CatalogUtil.normalizeIdentifier(sources[i]));
    }
  }

  @Test
  public final void testIsCompatibleType() {
    assertFalse(CatalogUtil.isCompatibleType(Type.INT4, Type.INT8));
    assertTrue(CatalogUtil.isCompatibleType(Type.INT8, Type.INT4));
    assertFalse(CatalogUtil.isCompatibleType(Type.FLOAT4, Type.FLOAT8));
    assertTrue(CatalogUtil.isCompatibleType(Type.FLOAT8, Type.FLOAT4));

    assertTrue(CatalogUtil.isCompatibleType(Type.FLOAT8, Type.INT4));

    assertFalse(CatalogUtil.isCompatibleType(Type.FLOAT8_ARRAY, Type.TEXT_ARRAY));
    assertFalse(CatalogUtil.isCompatibleType(Type.TEXT_ARRAY, Type.FLOAT8_ARRAY));
  }

  @Test
  public final void testCompareDataTypeIncludeVariableLength() {
    assertTrue(CatalogUtil.isMatchedFunction(
        Arrays.asList(CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8, Type.INT4)),
        Arrays.asList(CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4, Type.INT4))
    ));

  assertFalse(CatalogUtil.isMatchedFunction(
      Arrays.asList(CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4, Type.INT4)),
      Arrays.asList(CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8, Type.INT4))
  ));

    assertTrue(CatalogUtil.isMatchedFunction(
        Arrays.asList(CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8, Type.INT8_ARRAY)),
        Arrays.asList(CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4, Type.INT4, Type.INT4))
    ));
  }
}
