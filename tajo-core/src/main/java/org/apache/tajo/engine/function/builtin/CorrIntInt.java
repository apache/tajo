package org.apache.tajo.engine.function.builtin;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "corr",
    example = "> SELECT avg(expr, expr);",
    description = "Returns the Pearson coefficient of correlation between a set of number pairs.\n" +
        "The function takes as arguments any pair of numeric types and returns a double.\n"
        + "Any pair with a NULL is ignored. If the function is applied to an empty set or\n"
        + "a singleton set, NULL will be returned. Otherwise, it computes the following:\n"
        + "   COVAR_POP(x,y)/(STDDEV_POP(x)*STDDEV_POP(y))\n"
        + "where neither x nor y is null,\n"
        + "COVAR_POP is the population covariance,\n"
        + "and STDDEV_POP is the population standard deviation.",
    returnType = Type.FLOAT8,
    paramTypes = {@ParamTypes(paramTypes = {Type.INT4, Type.INT4})}
)
public class CorrIntInt extends CorrLongLong {

  public CorrIntInt() {
    super(new Column[] {
        new Column("expr", Type.INT4),
        new Column("expr", Type.INT4)
    });
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    if (!params.isBlankOrNull(0) && !params.isBlankOrNull(1)) {
      CorrContext corrContext = (CorrContext) ctx;
      long vx = params.getInt4(0);
      long vy = params.getInt4(1);
      double deltaX = vx - corrContext.xavg;
      double deltaY = vy - corrContext.yavg;
      corrContext.count++;
      corrContext.xavg += deltaX / corrContext.count;
      corrContext.yavg += deltaY / corrContext.count;
      if (corrContext.count > 1) {
        corrContext.covar += deltaX * (vy - corrContext.yavg);
        corrContext.xvar += deltaX * (vx - corrContext.xavg);
        corrContext.yvar += deltaY * (vy - corrContext.yavg);
      }
    }
  }
}
