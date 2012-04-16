/**
 * 
 */
package nta.datum;

import nta.datum.exception.InvalidCastException;

/**
 * @author hyunsik
 *
 */
public class AllDatum extends Datum {
  private static final AllDatum instance;
  
  static {
    instance = new AllDatum();
  }
  
  private AllDatum() {
    super(DatumType.ALL);
  }
  
  public static AllDatum get() {
    return instance;
  }

  @Override
  public boolean asBool() {
    throw new InvalidCastException("Null cannot be casted to boolean");
  }

  @Override
  public byte asByte() {
    throw new InvalidCastException("Null cannot be casted to byte");
  }

  @Override
  public short asShort() {
    throw new InvalidCastException("Null cannot be casted to short");
  }

  @Override
  public int asInt() {
    throw new InvalidCastException("Null cannot be casted to int");
  }

  @Override
  public long asLong() {
    throw new InvalidCastException("Null cannot be casted to long");
  }

  @Override
  public byte[] asByteArray() {
    throw new InvalidCastException("Null cannot be casted to byte array");
  }

  @Override
  public float asFloat() {
    throw new InvalidCastException("Null cannot be casted to float");
  }

  @Override
  public double asDouble() {
    throw new InvalidCastException("Null cannot be casted to double");
  }

  @Override
  public String asChars() {
    return "ALL";
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public String toJSON() {
    return "";
  }
}
