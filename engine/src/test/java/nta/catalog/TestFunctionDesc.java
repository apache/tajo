package nta.catalog;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.engine.EngineTestingUtils;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.util.FileUtil;

import org.junit.Test;

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

    EngineTestingUtils.buildTestDir(TEST_PATH);
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
}
