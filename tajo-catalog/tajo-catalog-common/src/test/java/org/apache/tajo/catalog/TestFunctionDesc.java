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

import org.apache.tajo.function.Function;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestFunctionDesc {
  private static final String TEST_PATH = "target/test-data/TestFunctionDesc";

  public static class TestSum extends Function {
    private Integer x;
    private Integer y;

    public TestSum() {
      super(new Column[] { new Column("arg1", org.apache.tajo.common.TajoDataTypes.Type.INT4),
          new Column("arg2", org.apache.tajo.common.TajoDataTypes.Type.INT4) });
    }

    public String toJSON() {
      return CatalogGsonHelper.toJson(this, Function.class);
    }

    @Override
    public CatalogProtos.FunctionType getFunctionType() {
      return FunctionType.GENERAL;
    }
  }


  @Test
  public void testGetSignature() throws IOException, ClassNotFoundException {
    FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT8));
    desc.setDescription("desc");
    desc.setExample("example");
    desc.setDetail("detail");

    assertEquals("sum", desc.getFunctionName());
    assertEquals(TestSum.class, desc.getFuncClass());
    assertEquals(FunctionType.GENERAL, desc.getFuncType());
    assertEquals(Type.INT4, desc.getReturnType().getType());
    assertArrayEquals(CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT8),
        desc.getParamTypes());

    CommonTestingUtil.getTestDir(TEST_PATH);
    File save = new File(TEST_PATH + "/save.dat");
    FileUtil.writeProto(save, desc.getProto());

    FunctionDescProto proto = FunctionDescProto.getDefaultInstance();
    proto = (FunctionDescProto) FileUtil.loadProto(save, proto);

    FunctionDesc newDesc = new FunctionDesc(proto);

    assertEquals("sum", newDesc.getFunctionName());
    assertEquals(TestSum.class, newDesc.getFuncClass());
    assertEquals(FunctionType.GENERAL, newDesc.getFuncType());
    assertEquals(Type.INT4, newDesc.getReturnType().getType());

    assertArrayEquals(CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT8),
        newDesc.getParamTypes());

    assertEquals(desc.getProto(), newDesc.getProto());
  }
  
  @Test
  public void testToJson() throws InternalException {
	  FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT8));
	  String json = desc.toJson();
	  FunctionDesc fromJson = CatalogGsonHelper.fromJson(json, FunctionDesc.class);
	  assertEquals(desc, fromJson);
	  assertEquals(desc.getProto(), fromJson.getProto());
  }

  @Test
  public void testGetProto() throws InternalException, ClassNotFoundException {
    FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT8));
    FunctionDescProto proto = desc.getProto();
    FunctionDesc fromProto = new FunctionDesc(proto);
    assertEquals(desc, fromProto);
    assertEquals(desc.toJson(), fromProto.toJson());
  }
  
  @Test
  public void testClone() throws CloneNotSupportedException {
    FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT8));
    FunctionDesc cloned = (FunctionDesc)desc.clone();
    assertTrue("reference chk" , !(desc == cloned));
    assertTrue("getClass() chk", desc.getClass() == cloned.getClass());
    assertTrue("equals() chk", desc.equals(cloned));
  }
}
