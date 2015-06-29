/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.function.builtin;

import org.apache.tajo.InternalTypes.CorrProto;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;

/**
 * Compute the Pearson correlation coefficient corr(x, y), using the following
 * stable one-pass method, based on:
 * "Formulas for Robust, One-Pass Parallel Computation of Covariances and
 * Arbitrary-Order Statistical Moments", Philippe Pebay, Sandia Labs
 * and "The Art of Computer Programming, volume 2: Seminumerical Algorithms",
 * Donald Knuth.
 *
 *  Incremental:
 *   n : <count>
 *   mx_n = mx_(n-1) + [x_n - mx_(n-1)]/n : <xavg>
 *   my_n = my_(n-1) + [y_n - my_(n-1)]/n : <yavg>
 *   c_n = c_(n-1) + (x_n - mx_(n-1))*(y_n - my_n) : <covariance * n>
 *   vx_n = vx_(n-1) + (x_n - mx_n)(x_n - mx_(n-1)): <variance * n>
 *   vy_n = vy_(n-1) + (y_n - my_n)(y_n - my_(n-1)): <variance * n>
 *
 *  Merge:
 *   c_(A,B) = c_A + c_B + (mx_A - mx_B)*(my_A - my_B)*n_A*n_B/(n_A+n_B)
 *   vx_(A,B) = vx_A + vx_B + (mx_A - mx_B)*(mx_A - mx_B)*n_A*n_B/(n_A+n_B)
 *   vy_(A,B) = vy_A + vy_B + (my_A - my_B)*(my_A - my_B)*n_A*n_B/(n_A+n_B)
 *
 */
@Description(
    functionName = "corr",
    example = "> SELECT corr(expr, expr);",
    description = "Returns the Pearson coefficient of correlation between a set of number pairs.\n" +
        "The function takes as arguments any pair of numeric types and returns a double.\n"
        + "Any pair with a NULL is ignored. If the function is applied to an empty set or\n"
        + "a singleton set, NULL will be returned. Otherwise, it computes the following:\n"
        + "   COVAR_POP(x,y)/(STDDEV_POP(x)*STDDEV_POP(y))\n"
        + "where neither x nor y is null,\n"
        + "COVAR_POP is the population covariance,\n"
        + "and STDDEV_POP is the population standard deviation.",
    returnType = Type.FLOAT8,
    paramTypes = {
        @ParamTypes(paramTypes = {Type.INT8, Type.INT8}),
        @ParamTypes(paramTypes = {Type.INT8, Type.INT4}),
        @ParamTypes(paramTypes = {Type.INT4, Type.INT8}),
        @ParamTypes(paramTypes = {Type.INT4, Type.INT4}),
        @ParamTypes(paramTypes = {Type.INT8, Type.FLOAT8}),
        @ParamTypes(paramTypes = {Type.INT8, Type.FLOAT4}),
        @ParamTypes(paramTypes = {Type.INT4, Type.FLOAT8}),
        @ParamTypes(paramTypes = {Type.INT4, Type.FLOAT4}),
        @ParamTypes(paramTypes = {Type.FLOAT8, Type.INT8}),
        @ParamTypes(paramTypes = {Type.FLOAT8, Type.INT4}),
        @ParamTypes(paramTypes = {Type.FLOAT4, Type.INT8}),
        @ParamTypes(paramTypes = {Type.FLOAT4, Type.INT4}),
        @ParamTypes(paramTypes = {Type.FLOAT8, Type.FLOAT8}),
        @ParamTypes(paramTypes = {Type.FLOAT4, Type.FLOAT8}),
        @ParamTypes(paramTypes = {Type.FLOAT8, Type.FLOAT4}),
        @ParamTypes(paramTypes = {Type.FLOAT4, Type.FLOAT4}),
    }
)
public class Corr extends AggFunction<Datum> {

  /**
   * Evaluate the Pearson correlation coefficient using a stable one-pass
   * algorithm, based on work by Philippe Pébay and Donald Knuth.
   *
   *  Incremental:
   *   n : <count>
   *   mx_n = mx_(n-1) + [x_n - mx_(n-1)]/n : <xavg>
   *   my_n = my_(n-1) + [y_n - my_(n-1)]/n : <yavg>
   *   c_n = c_(n-1) + (x_n - mx_(n-1))*(y_n - my_n) : <covariance * n>
   *   vx_n = vx_(n-1) + (x_n - mx_n)(x_n - mx_(n-1)): <variance * n>
   *   vy_n = vy_(n-1) + (y_n - my_n)(y_n - my_(n-1)): <variance * n>
   *
   *  Merge:
   *   c_X = c_A + c_B + (mx_A - mx_B)*(my_A - my_B)*n_A*n_B/n_X
   *   vx_(A,B) = vx_A + vx_B + (mx_A - mx_B)*(mx_A - mx_B)*n_A*n_B/(n_A+n_B)
   *   vy_(A,B) = vy_A + vy_B + (my_A - my_B)*(my_A - my_B)*n_A*n_B/(n_A+n_B)
   *
   */
  public Corr() {
    super(new Column[] {
        new Column("expr", Type.FLOAT8),
        new Column("expr", Type.FLOAT8)
    });
  }

  public Corr(Column[] definedArgs) {
    super(definedArgs);
  }

  @Override
  public FunctionContext newContext() {
    return new CorrContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    if (!params.isBlankOrNull(0) && !params.isBlankOrNull(1)) {
      CorrContext corrContext = (CorrContext) ctx;
      double vx = params.getFloat8(0);
      double vy = params.getFloat8(1);
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

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    CorrContext corrContext = (CorrContext) ctx;
    if (part.isBlankOrNull(0)) {
      return;
    }
    ProtobufDatum datum = (ProtobufDatum) part.getProtobufDatum(0);
    CorrProto proto = (CorrProto) datum.get();
    long nA = corrContext.count;
    long nB = proto.getCount();

    if (nA == 0) {
      corrContext.count = proto.getCount();
      corrContext.xavg = proto.getXavg();
      corrContext.yavg = proto.getYavg();
      corrContext.xvar = proto.getXvar();
      corrContext.yvar = proto.getYvar();
      corrContext.covar = proto.getCovar();
    } else {
      // Merge the two partials
      double xavgA = corrContext.xavg;
      double yavgA = corrContext.yavg;
      double xavgB = proto.getXavg();
      double yavgB = proto.getYavg();
      double xvarB = proto.getXvar();
      double yvarB = proto.getYvar();
      double covarB = proto.getCovar();

      corrContext.count += nB;
      corrContext.xavg = (xavgA * nA + xavgB * nB) / corrContext.count;
      corrContext.yavg = (yavgA * nA + yavgB * nB) / corrContext.count;
      corrContext.xvar += xvarB + (xavgA - xavgB) * (xavgA - xavgB) * nA * nB / corrContext.count;
      corrContext.yvar += yvarB + (yavgA - yavgB) * (yavgA - yavgB) * nA * nB / corrContext.count;
      corrContext.covar +=
          covarB + (xavgA - xavgB) * (yavgA - yavgB) * ((double) (nA * nB) / corrContext.count);
    }
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    CorrContext corrContext = (CorrContext) ctx;
    if (corrContext.count == 0) {
      return NullDatum.get();
    }
    CorrProto.Builder builder = CorrProto.newBuilder();
    builder.setCount(corrContext.count);
    builder.setXavg(corrContext.xavg);
    builder.setYavg(corrContext.yavg);
    builder.setXvar(corrContext.xvar);
    builder.setYvar(corrContext.yvar);
    builder.setCovar(corrContext.covar);
    return new ProtobufDatum(builder.build());
  }

  @Override
  public DataType getPartialResultType() {
    return CatalogUtil.newDataType(Type.PROTOBUF, CorrProto.class.getName());
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    CorrContext corrContext = (CorrContext) ctx;

    if (corrContext.count < 2) { // SQL standard - return null for zero or one pair
      return NullDatum.get();
    } else {
      return DatumFactory.createFloat8(corrContext.covar
          / java.lang.Math.sqrt(corrContext.xvar)
          / java.lang.Math.sqrt(corrContext.yvar));
    }
  }

  protected static class CorrContext implements FunctionContext {
    long count = 0; // number n of elements
    double xavg = 0; // average of x elements
    double yavg = 0; // average of y elements
    double xvar = 0; // n times the variance of x elements
    double yvar = 0; // n times the variance of y elements
    double covar = 0; // n times the covariance
  }
}
