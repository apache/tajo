package nta.catalog;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
    FunctionDesc desc = new FunctionDesc("sum", TestSum.class,
        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT,
            DataType.LONG });
    assertEquals("sum", desc.getSignature());
    assertEquals(TestSum.class, desc.getFuncClass());
    assertEquals(FunctionType.GENERAL, desc.getFuncType());
    assertEquals(DataType.INT, desc.getReturnType());
    assertArrayEquals(new DataType[] { DataType.INT, DataType.LONG },
        desc.getDefinedArgs());

    WorkerTestingUtil.buildTestDir(TEST_PATH);
    File save = new File(TEST_PATH + "/save.dat");
    FileUtil.writeProto(save, desc.getProto());

    FunctionDescProto proto = FunctionDescProto.getDefaultInstance();
    proto = (FunctionDescProto) FileUtil.loadProto(save, proto);

    FunctionDesc newDesc = new FunctionDesc(proto);
    assertEquals("sum", newDesc.getSignature());
    assertEquals(TestSum.class, newDesc.getFuncClass());
    assertEquals(FunctionType.GENERAL, newDesc.getFuncType());
    assertEquals(DataType.INT, newDesc.getReturnType());
    assertArrayEquals(new DataType[] { DataType.INT, DataType.LONG },
        newDesc.getDefinedArgs());

    assertEquals(desc.getProto(), newDesc.getProto());
  }
  
  @Test
  public void testJson() throws InternalException {
	  FunctionDesc desc = new FunctionDesc("sum", TestSum.class,
		        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT,
		            DataType.LONG });
	  String json = desc.toJSON();
	  System.out.println(json);
	  Gson gson = GsonCreator.getInstance();
	  FunctionDesc fromJson = gson.fromJson(json, FunctionDesc.class);
	  
	  assertEquals("sum", fromJson.getSignature());
	    assertEquals(TestSum.class, fromJson.getFuncClass());
	    assertEquals(FunctionType.GENERAL, fromJson.getFuncType());
	    assertEquals(DataType.INT, fromJson.getReturnType());
	    assertArrayEquals(new DataType[] { DataType.INT, DataType.LONG },
	    		fromJson.getDefinedArgs());

	    assertEquals(desc.getProto(), fromJson.getProto());
  }
}
