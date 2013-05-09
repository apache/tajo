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

package tajo.catalog;

import com.google.gson.Gson;
import org.junit.Test;
import tajo.catalog.function.GeneralFunction;
import tajo.catalog.json.GsonCreator;
import tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.common.TajoDataTypes;
import tajo.common.TajoDataTypes.Type;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.exception.InternalException;
import tajo.storage.Tuple;
import tajo.util.CommonTestingUtil;
import tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestFunctionDesc {
  private static final String TEST_PATH = "target/test-data/TestFunctionDesc";

  public static class TestSum extends GeneralFunction {
    private Integer x;
    private Integer y;

    public TestSum() {
      super(new Column[] { new Column("arg1", TajoDataTypes.Type.INT4),
          new Column("arg2", TajoDataTypes.Type.INT4) });
    }

    @Override
    public Datum eval(Tuple params) {
      x =  params.get(0).asInt4();
      y =  params.get(1).asInt4();
      return DatumFactory.createInt4(x + y);
    }

    public String toJSON() {
      return GsonCreator.getInstance().toJson(this, GeneralFunction.class);
    }
  }


  @Test
  public void testGetSignature() throws IOException {
    FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4, Type.INT8));
    assertEquals("sum", desc.getSignature());
    assertEquals(TestSum.class, desc.getFuncClass());
    assertEquals(FunctionType.GENERAL, desc.getFuncType());
    assertEquals(Type.INT4, desc.getReturnType()[0].getType());
    assertArrayEquals(CatalogUtil.newDataTypesWithoutLen(Type.INT4, Type.INT8),
        desc.getParamTypes());

    CommonTestingUtil.getTestDir(TEST_PATH);
    File save = new File(TEST_PATH + "/save.dat");
    FileUtil.writeProto(save, desc.getProto());

    FunctionDescProto proto = FunctionDescProto.getDefaultInstance();
    proto = (FunctionDescProto) FileUtil.loadProto(save, proto);

    FunctionDesc newDesc = new FunctionDesc(proto);
    assertEquals("sum", newDesc.getSignature());
    assertEquals(TestSum.class, newDesc.getFuncClass());
    assertEquals(FunctionType.GENERAL, newDesc.getFuncType());
    assertEquals(Type.INT4, newDesc.getReturnType()[0].getType());
    assertArrayEquals(CatalogUtil.newDataTypesWithoutLen(Type.INT4, Type.INT8),
        newDesc.getParamTypes());

    assertEquals(desc.getProto(), newDesc.getProto());
  }
  
  @Test
  public void testJson() throws InternalException {
	  FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4, Type.INT8));
	  String json = desc.toJSON();
	  System.out.println(json);
	  Gson gson = GsonCreator.getInstance();
	  FunctionDesc fromJson = gson.fromJson(json, FunctionDesc.class);
	  
	  assertEquals("sum", fromJson.getSignature());
	    assertEquals(TestSum.class, fromJson.getFuncClass());
	    assertEquals(FunctionType.GENERAL, fromJson.getFuncType());
	    assertEquals(Type.INT4, fromJson.getReturnType()[0].getType());
	    assertArrayEquals(CatalogUtil.newDataTypesWithoutLen(Type.INT4, Type.INT8),
	    		fromJson.getParamTypes());

	    assertEquals(desc.getProto(), fromJson.getProto());
  }
  
  @Test
  public void testClone() throws CloneNotSupportedException {
    FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4, Type.INT8));
    FunctionDesc cloned = (FunctionDesc)desc.clone();
    assertTrue("reference chk" , !(desc == cloned));
    assertTrue("getClass() chk", desc.getClass() == cloned.getClass());
    assertTrue("equals() chk", desc.equals(cloned));
  }
}
