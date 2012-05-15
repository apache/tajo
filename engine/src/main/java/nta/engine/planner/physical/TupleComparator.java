/**
 * 
 */
package nta.engine.planner.physical;

import java.util.Comparator;

import com.google.common.base.Preconditions;

import nta.catalog.Schema;
import nta.datum.Datum;
import nta.datum.DatumType;
import nta.engine.parser.QueryBlock.SortSpec;
import nta.storage.Tuple;

/**
 * The Comparator class for Tuples
 * 
 * @author Hyunsik Choi
 * 
 * @see Tuple
 */
public class TupleComparator implements Comparator<Tuple> {
  private final int[] sortKeyIds;
  private final boolean[] asc;
  @SuppressWarnings("unused")
  private final boolean[] nullFirsts;  

  private Datum left;
  private Datum right;
  private int compVal;

  public TupleComparator(Schema schema, SortSpec[] sortKeys) {
    Preconditions.checkArgument(sortKeys.length > 0, 
        "At least one sort key must be specified.");
    
    this.sortKeyIds = new int[sortKeys.length];
    this.asc = new boolean[sortKeys.length];
    this.nullFirsts = new boolean[sortKeys.length];
    for (int i = 0; i < sortKeys.length; i++) {
      this.sortKeyIds[i] = schema.getColumnId(sortKeys[i].getSortKey().getQualifiedName());
          
      this.asc[i] = sortKeys[i].isAscending();
      this.nullFirsts[i]= sortKeys[i].isNullFirst();
    }
  }

  @Override
  public int compare(Tuple tuple1, Tuple tuple2) {
    for (int i = 0; i < sortKeyIds.length; i++) {
      left = tuple1.get(sortKeyIds[i]);
      right = tuple2.get(sortKeyIds[i]);

      if (left.type() == DatumType.NULL || right.type() == DatumType.NULL) {
        if (!left.equals(right)) {
          if (left.type() == DatumType.NULL) {
            compVal = 1;
          } else if (right.type() == DatumType.NULL) {
            compVal = -1;
          }
          if (nullFirsts[i]) {
            if (compVal != 0) {
              compVal *= -1;
            }
          }
        } else {
          compVal = 0;
        }
      } else {
        if (asc[i]) {
          compVal = left.compareTo(right);
        } else {
          compVal = right.compareTo(left);
        }
      }

      if (compVal < 0 || compVal > 0) {
        return compVal;
      }
    }
    return 0;
  }
}