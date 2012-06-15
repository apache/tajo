package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import nta.datum.DatumType;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class NewCountValue extends NewCountRows {

  public NewCountValue() {
    super(new Column[] {
        new Column("col", CatalogProtos.DataType.ANY)
    });
  }
  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    if (params.get(0).type() != DatumType.NULL) {
      ((CountRowContext) ctx).count++;
    }
  }
}
