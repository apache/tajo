package nta.datum;


public abstract class NumericDatum extends Datum {

  public NumericDatum(DatumType type) {
    super(type);
  }
  
  public abstract Datum plus(Datum datum);
  
  public abstract Datum minus(Datum datum);
  
  public abstract Datum multiply(Datum datum);
  
  public abstract Datum divide(Datum datum);
  
  public abstract void inverseSign();
}
