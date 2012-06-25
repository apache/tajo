package nta.datum;

/**
 * @author Hyunsik Choi
 */
public class NullDatum extends Datum {
  private static final NullDatum instance;
  
  static {
    instance = new NullDatum();
  }
  
  private NullDatum() {
    super(DatumType.NULL);
  }
  
  public static NullDatum get() {
    return instance;
  }
  
  @Override
  public boolean asBool() {
    return false;
  }

  @Override
  public byte asByte() {
    return 0;
  }

  @Override
  public short asShort() {
    return 0;
  }

  @Override
  public int asInt() {
    return 0;
  }

  @Override
  public long asLong() {
    return 0;
  }

  @Override
  public byte[] asByteArray() {
    return null;
  }

  @Override
  public float asFloat() {
    return 0;
  }

  @Override
  public double asDouble() {
    return 0;
  }

  @Override
  public String asChars() {
    return "NULL";
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NullDatum) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(Datum datum) {
    return 0;
  }

  @Override
  public int hashCode() {
    return 23244; // one of the prime number
  }

  @Override
  public String toJSON() {
    return "";
  }
}