/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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
import tajo.WorkerTestingUtil;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.engine.eval.TestEvalTree.TestSum;
import tajo.engine.exception.InternalException;
import tajo.engine.json.GsonCreator;
import tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestFunctionDesc {
  private static final String TEST_PATH = "target/test-data/TestFunctionDesc";

  @Test
  public void testGetSignature() throws IOException {
    FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        new DataType [] {DataType.INT},
        new DataType [] {DataType.INT,DataType.LONG});
    assertEquals("sum", desc.getSignature());
    assertEquals(TestSum.class, desc.getFuncClass());
    assertEquals(FunctionType.GENERAL, desc.getFuncType());
    assertEquals(DataType.INT, desc.getReturnType()[0]);
    assertArrayEquals(new DataType[] { DataType.INT, DataType.LONG },
        desc.getParamTypes());

    WorkerTestingUtil.buildTestDir(TEST_PATH);
    File save = new File(TEST_PATH + "/save.dat");
    FileUtil.writeProto(save, desc.getProto());

    FunctionDescProto proto = FunctionDescProto.getDefaultInstance();
    proto = (FunctionDescProto) FileUtil.loadProto(save, proto);

    FunctionDesc newDesc = new FunctionDesc(proto);
    assertEquals("sum", newDesc.getSignature());
    assertEquals(TestSum.class, newDesc.getFuncClass());
    assertEquals(FunctionType.GENERAL, newDesc.getFuncType());
    assertEquals(DataType.INT, newDesc.getReturnType()[0]);
    assertArrayEquals(new DataType[] { DataType.INT, DataType.LONG },
        newDesc.getParamTypes());

    assertEquals(desc.getProto(), newDesc.getProto());
  }
  
  @Test
  public void testJson() throws InternalException {
	  FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        new DataType [] {DataType.INT},
        new DataType [] {DataType.INT,DataType.LONG});
	  String json = desc.toJSON();
	  System.out.println(json);
	  Gson gson = GsonCreator.getInstance();
	  FunctionDesc fromJson = gson.fromJson(json, FunctionDesc.class);
	  
	  assertEquals("sum", fromJson.getSignature());
	    assertEquals(TestSum.class, fromJson.getFuncClass());
	    assertEquals(FunctionType.GENERAL, fromJson.getFuncType());
	    assertEquals(DataType.INT, fromJson.getReturnType()[0]);
	    assertArrayEquals(new DataType[] { DataType.INT, DataType.LONG },
	    		fromJson.getParamTypes());

	    assertEquals(desc.getProto(), fromJson.getProto());
  }
  
  @Test
  public void testClone() throws CloneNotSupportedException {
    FunctionDesc desc = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        new DataType [] {DataType.INT},
        new DataType [] {DataType.INT,DataType.LONG});
    FunctionDesc cloned = (FunctionDesc)desc.clone();
    assertTrue("reference chk" , !(desc == cloned));
    assertTrue("getClass() chk", desc.getClass() == cloned.getClass());
    assertTrue("equals() chk", desc.equals(cloned));
  }
}
