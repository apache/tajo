package nta.catalog;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.engine.WorkerTestingUtil;
import nta.engine.exception.InternalException;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.json.GsonCreator;
import nta.util.FileUtil;

import org.junit.Test;

import com.google.gson.Gson;

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
