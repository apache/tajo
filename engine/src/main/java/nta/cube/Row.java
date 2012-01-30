package nta.cube;

import nta.catalog.Schema;
import nta.datum.Datum;
import nta.storage.VTuple;

public class Row extends VTuple implements Comparable<Row> {
  int count;

  public Row(Schema schema) {
    super(schema.getColumnNum() - 1);
  }

  public Row(int size) {
    super(size);
  }

  public String toString() {
    StringBuilder str = new StringBuilder();
    for (int k = 0; k < values.length; k++) {
      str.append(values[k].toString() + " ");
    }
    str.append(count);
    return str.toString();
  }

  @Override
  public int compareTo(Row o) {
    Datum datum;

    for (int k = 0; k < values.length; k++) {
      datum = this.values[k];
      if (datum.greaterThan(o.values[k]).asBool()) {
        return 1;
      } else if (datum.lessThan(o.values[k]).asBool()) {
        return -1;
      } else {
        continue;
      }

    }
    return 0;
  }

  public boolean equals(Row o) {
    if (o == null)
      return false;

    if (o.compareTo(this) == 0)
      return true;
    else
      return false;
  }
}
