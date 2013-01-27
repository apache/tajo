package tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import tajo.catalog.Schema;
import tajo.catalog.SortSpec;
import tajo.datum.Datum;
import tajo.datum.DatumType;
import tajo.storage.Tuple;

import java.util.Comparator;

/**
 * The Comparator class for Outer and Inner Tuples
 * 
 * @author ByungNam Lim
 * @author Hyunsik Choi
 * 
 * @see tajo.storage.Tuple
 */
public class JoinTupleComparator implements Comparator<Tuple> {
  private int numSortKey;
  private final int[] outerSortKeyIds;
  private final int[] innerSortKeyIds;

  private Datum outer;
  private Datum inner;
  private int compVal;

  public JoinTupleComparator(Schema leftschema, Schema rightschema, SortSpec[][] sortKeys) {
    Preconditions.checkArgument(sortKeys.length == 2,
        "The two of the sortspecs must be given, but " + sortKeys.length + " sortkeys are given.");
    Preconditions.checkArgument(sortKeys[0].length == sortKeys[1].length,
        "The number of both side sortkeys must be equals, but they are different: "
            + sortKeys[0].length + " and " + sortKeys[1].length);

    this.numSortKey = sortKeys[0].length; // because it is guaranteed that the number of sortspecs are equals
    this.outerSortKeyIds = new int[numSortKey];
    this.innerSortKeyIds = new int[numSortKey];

    for (int i = 0; i < numSortKey; i++) {
      this.outerSortKeyIds[i] = leftschema.getColumnId(sortKeys[0][i].getSortKey().getQualifiedName());
      this.innerSortKeyIds[i] = rightschema.getColumnId(sortKeys[1][i].getSortKey().getQualifiedName());
    }
  }

  @Override
  public int compare(Tuple outerTuple, Tuple innerTuple) {
    for (int i = 0; i < numSortKey; i++) {
      outer = outerTuple.get(outerSortKeyIds[i]);
      inner = innerTuple.get(innerSortKeyIds[i]);

      if (outer.type() == DatumType.NULL || inner.type() == DatumType.NULL) {
        if (!outer.equals(inner)) {
          if (outer.type() == DatumType.NULL) {
            compVal = 1;
          } else if (inner.type() == DatumType.NULL) {
            compVal = -1;
          }
        } else {
          compVal = 0;
        }
      } else {
        compVal = outer.compareTo(inner);
      }

      if (compVal < 0 || compVal > 0) {
        return compVal;
      }
    }
    return 0;
  }
}