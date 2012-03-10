package nta.engine.utils;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.storage.Tuple;

public class TupleUtil {
  public static int[] getTargetIds(Schema inSchema, Schema outSchema) {
    int[] targetIds = new int[outSchema.getColumnNum()];
    int i = 0;
    for (Column target : outSchema.getColumns()) {
      targetIds[i] = inSchema.getColumnId(target.getQualifiedName());
      i++;
    }

    return targetIds;
  }

  public static Tuple project(Tuple in, Tuple out, int[] targetIds) {
    out.clear();
    for (int idx = 0; idx < targetIds.length; idx++) {
      out.put(idx, in.get(targetIds[idx]));
    }
    return out;
  }
}
