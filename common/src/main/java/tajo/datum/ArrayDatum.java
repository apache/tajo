package tajo.datum;

import com.google.gson.annotations.Expose;
import tajo.datum.json.GsonCreator;

/**
 * @author Hyunsik Choi
 */
public class ArrayDatum extends Datum {
  @Expose private Datum [] data;
  public ArrayDatum(Datum [] data) {
    super(DatumType.ARRAY);
    this.data = data;
  }

  public ArrayDatum(int size) {
    super(DatumType.ARRAY);
    this.data = new Datum[size];
  }

  public Datum get(int idx) {
    return data[idx];
  }

  public Datum [] toArray() {
    return data;
  }

  public void put(int idx, Datum datum) {
    data[idx] = datum;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int compareTo(Datum datum) {
    return 0; // TODO - to be implemented
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = true;
    for (Datum field : data) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(field.asChars());
    }
    sb.append("]");

    return sb.toString();
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, Datum.class);
  }
}
