package org.apache.tajo.storage.newtuple.map;

import org.apache.tajo.storage.newtuple.SizeOf;

public class MapMinusInt4ValFloat8ColOp extends MapBinaryOp {

  public void map(int vecnum, long result, long lhs, long rhs, long nullFlags, long selId) {
    for (int i = 0; i < vecnum; i++) {
      int lval1 = unsafe.getInt(lhs);
      double rval1 = unsafe.getFloat(rhs);
      unsafe.putDouble(result, lval1 - rval1);

      result += SizeOf.SIZE_OF_LONG;
      rhs += SizeOf.SIZE_OF_LONG;
      lhs += SizeOf.SIZE_OF_LONG;
    }
  }
}
