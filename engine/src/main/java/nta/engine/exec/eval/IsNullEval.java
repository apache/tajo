package nta.engine.exec.eval;

import com.google.gson.annotations.Expose;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.SchemaUtil;
import nta.catalog.proto.CatalogProtos;
import nta.datum.BoolDatum;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class IsNullEval extends BinaryEval {
  private final static ConstEval NULL_EVAL = new ConstEval(DatumFactory.createNullDatum());
  private static final CatalogProtos.DataType[] RES_TYPE = SchemaUtil.newNoNameSchema(CatalogProtos.DataType.BOOLEAN);

  // persistent variables
  @Expose private boolean isNot;
  @Expose private Column columnRef;
  @Expose private Integer fieldId = null;

  // volatile variable
  private BoolDatum result;

  public IsNullEval(boolean not, FieldEval field) {
    super(Type.IS, field, NULL_EVAL);
    this.isNot = not;
    this.columnRef = field.getColumnRef();
  }

  @Override
  public CatalogProtos.DataType[] getValueType() {
    return RES_TYPE;
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    if (fieldId == null) {
      fieldId = schema.getColumnId(columnRef.getQualifiedName());
    }
    result = DatumFactory.createBool(tuple.get(fieldId).isNull());
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    return result;
  }

  public boolean isNot() {
    return isNot;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IsNullEval) {
      IsNullEval other = (IsNullEval) obj;
      return super.equals(other) &&
          this.columnRef.equals(other.columnRef) &&
          this.fieldId == other.fieldId;
    } else {
      return false;
    }
  }

  public Object clone() throws CloneNotSupportedException {
    IsNullEval isNullEval = (IsNullEval) super.clone();
    isNullEval.columnRef = (Column) columnRef.clone();
    isNullEval.fieldId = fieldId;

    return isNullEval;
  }
}
