/**
 * 
 */
package nta.storage;

import java.net.InetAddress;

import com.google.common.base.Preconditions;

import nta.datum.BoolDatum;
import nta.datum.ByteDatum;
import nta.datum.BytesDatum;
import nta.datum.Datum;
import nta.datum.DoubleDatum;
import nta.datum.FloatDatum;
import nta.datum.IPv4Datum;
import nta.datum.IntDatum;
import nta.datum.LongDatum;
import nta.datum.ShortDatum;
import nta.datum.StringDatum;
import nta.engine.exception.UnimplementedException;
import nta.engine.exception.UnsupportedException;

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
  public void clear() {
    throw new UnsupportedException();
  }

  @Override
  public void put(int fieldId, Datum value) {
    throw new UnsupportedException();
  }

  @Override
  public void put(Datum... values) {
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
    return ((IPv4Datum)get(fieldId)).asByteArray();
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
  
  public String toString() {
    boolean first = true;
    StringBuilder str = new StringBuilder();
    str.append("(");
    for(int i=0; i < size(); i++) {      
      if(contains(i) != false) {
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
