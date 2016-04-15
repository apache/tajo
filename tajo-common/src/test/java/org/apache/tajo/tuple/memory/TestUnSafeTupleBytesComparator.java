package org.apache.tajo.tuple.memory;

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.tuple.RowBlockReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestUnSafeTupleBytesComparator {

  @Test
  public void testCompare() {
    MemoryRowBlock rowBlock = null;
    try {
      rowBlock = new MemoryRowBlock(new DataType[]{
          DataType.newBuilder().setType(Type.TEXT).build()
      });
      RowWriter builder = rowBlock.getWriter();
      builder.startRow();
      builder.putText("CÔTE D'IVOIRE");
      builder.endRow();
      builder.startRow();
      builder.putText("CANADA");
      builder.endRow();

      RowBlockReader reader = rowBlock.getReader();

      UnSafeTuple t1 = new UnSafeTuple();
      UnSafeTuple t2 = new UnSafeTuple();
      reader.next(t1);
      reader.next(t2);

      int compare = UnSafeTupleBytesComparator.compare(t1.getFieldAddr(0), t2.getFieldAddr(0));
      assertEquals(130, compare);
    } finally {
      if (rowBlock != null) {
        rowBlock.release();
      }
    }
  }
}
