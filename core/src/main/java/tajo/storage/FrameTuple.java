/**
 * 
 */
package tajo.storage;

import com.google.common.base.Preconditions;
import tajo.datum.*;
import tajo.engine.exception.UnimplementedException;
import tajo.engine.exception.UnsupportedException;

import java.net.InetAddress;

/**
 * An instance of FrameTuple is an immutable tuple.
 * It contains two tuples and pretends to be one instance of Tuple for
 * join qual evaluatations.
 * 
 * @author Hyunsik Choi
 *
 */
public class FrameTuple implements Tuple {
  private int size;
  private int leftSize;
  
  private Tuple left;
  private Tuple right;
  
  public FrameTuple() {}
  
  public FrameTuple(Tuple left, Tuple right) {
    set(left, right);
  }
  
  public void set(Tuple left, Tuple right) {
    this.size = left.size() + right.size();
    this.left = left;
    this.leftSize = left.size();
    this.right = right;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean contains(int fieldId) {
    Preconditions.checkArgument(fieldId < size, 
        "Out of field access: " + fieldId);
    
    if (fieldId < leftSize) {
      return left.contains(fieldId);
    } else {
      return right.contains(fieldId - leftSize);
    }
  }

  @Override
  public boolean isNull(int fieldid) {
    return get(fieldid).type() == DatumType.NULL;
  }

  @Override
  public void clear() {
    throw new UnsupportedException();
  }

  @Override
  public void put(int fieldId, Datum value) {
    throw new UnsupportedException();
  }

  @Override
  public void put(int fieldId, Datum[] values) {
    throw new UnsupportedException();
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    throw new UnsupportedException();
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedException();
  }
  
  @Override
  public long getOffset() {
    throw new UnsupportedException();
  }

  @Override
  public void put(Datum [] values) {
    throw new UnsupportedException();
  }

  @Override
  public Datum get(int fieldId) {
    Preconditions.checkArgument(fieldId < size, 
        "Out of field access: " + fieldId);
    
    if (fieldId < leftSize) {
      return left.get(fieldId);
    } else {
      return right.get(fieldId - leftSize);
    }
  }

  @Override
  public BoolDatum getBoolean(int fieldId) {
    return (BoolDatum) get(fieldId);
  }

  @Override
  public ByteDatum getByte(int fieldId) {
    return (ByteDatum) get(fieldId);
  }

  @Override
  public CharDatum getChar(int fieldId) {
    return (CharDatum) get(fieldId);
  }

  @Override
  public BytesDatum getBytes(int fieldId) {
    return (BytesDatum) get(fieldId);
  }

  @Override
  public ShortDatum getShort(int fieldId) {
    return (ShortDatum) get(fieldId);
  }

  @Override
  public IntDatum getInt(int fieldId) {
    return (IntDatum) get(fieldId);
  }

  @Override
  public LongDatum getLong(int fieldId) {
    return (LongDatum) get(fieldId);
  }

  @Override
  public FloatDatum getFloat(int fieldId) {
    return (FloatDatum) get(fieldId);
  }

  @Override
  public DoubleDatum getDouble(int fieldId) {
    return (DoubleDatum) get(fieldId);
  }

  @Override
  public IPv4Datum getIPv4(int fieldId) {
    return (IPv4Datum) get(fieldId);
  }

  @Override
  public byte[] getIPv4Bytes(int fieldId) { 
    return get(fieldId).asByteArray();
  }

  @Override
  public InetAddress getIPv6(int fieldId) {
    throw new UnimplementedException();
  }
  
  @Override
  public byte[] getIPv6Bytes(int fieldId) {
    throw new UnimplementedException();
  }

  @Override
  public StringDatum getString(int fieldId) {
    return (StringDatum) get(fieldId);
  }

  @Override
  public StringDatum2 getString2(int fieldId) {
    return (StringDatum2) get(fieldId);
  }

  public String toString() {
    boolean first = true;
    StringBuilder str = new StringBuilder();
    str.append("(");
    for(int i=0; i < size(); i++) {      
      if(contains(i)) {
        if(first) {
          first = false;
        } else {
          str.append(", ");
        }
        str.append(i)
        .append("=>")
        .append(get(i));
      }
    }
    str.append(")");
    return str.toString();
  }
}
