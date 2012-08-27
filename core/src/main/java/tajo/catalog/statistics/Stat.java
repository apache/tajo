package tajo.catalog.statistics;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import tajo.engine.TCommonProtos.StatProto;
import tajo.engine.TCommonProtos.StatType;

/**
 * @author Hyunsik Choi
 */
public class Stat implements Cloneable {
  @Expose private long val = 0;
  @Expose private StatType type;

  public Stat(StatType type) {
    this.type = type;
    val = 0;
  }

  public Stat(StatProto proto) {
    this.type = proto.getType();
    this.val = proto.getValue();
  }

  public StatType getType() {
    return this.type;
  }

  public long getValue() {
    return this.val;
  }

  public void setValue(long val) {
    this.val = val;
  }

  public void increment() {
    this.val++;
  }

  public void incrementBy(long delta) {
    this.val += delta;
  }

  public void subtract() {
    this.val--;
  }

  public void subtractBy(long delta) {
    val -= delta;
  }

  public boolean equals(Object obj) {
    if (obj instanceof Stat) {
      Stat other = (Stat) obj;
      return this.type == other.type 
          && this.val == other.val;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(type, val);
  }

  public Object clone() throws CloneNotSupportedException {
    Stat stat = (Stat) super.clone();
    stat.type = type;
    stat.val = val;

    return stat;
  }

  /**
   * @return
   */
  public StatProto toProto() {
    // This is not designed for a proto buffer object due to
    // performance problem. However, it needs the method to transform
    // itself to the proto object.
    StatProto.Builder builder = StatProto.newBuilder();
    builder.setType(type);
    builder.setValue(val);

    return builder.build();
  }

  public String toString() {
    return type + ": " + val;
  }
}
