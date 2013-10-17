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

package org.apache.tajo.engine.eval;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;

public class BetweenPredicateEval extends EvalNode {
  private static final TajoDataTypes.DataType RES_TYPE = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.BOOLEAN);
  @Expose private boolean not;
  @Expose private boolean symmetric;
  @Expose private EvalNode predicand;
  @Expose private EvalNode begin;
  @Expose private EvalNode end;

  private Checker checker;

  public BetweenPredicateEval(boolean not, boolean symmetric, EvalNode predicand, EvalNode begin, EvalNode end) {
    super(EvalType.BETWEEN);
    this.not = not;
    this.symmetric = symmetric;
    this.predicand = predicand;
    this.begin = begin;
    this.end = end;
  }

  private static interface Checker {
    void eval(BetweenContext context, Schema schema, Tuple param);
  }

  private static class ConstantChecker implements Checker {
    EvalNode predicand;
    Datum begin;
    Datum end;
    private boolean not;

    private ConstantChecker(boolean not, EvalNode predicand, Datum begin, Datum end) {
      this.predicand = predicand;
      this.not = not;
      if (begin.compareTo(end) > 0) {
        this.begin = end;
        this.end = begin;
      } else {
        this.begin = begin;
        this.end = end;
      }
    }

    @Override
    public void eval(BetweenContext context, Schema schema, Tuple param) {
      predicand.eval(context.predicandContext, schema, param);
      Datum predicandValue = predicand.terminate(context.predicandContext);

      if (!(predicandValue instanceof NullDatum)) {
        context.result =
            DatumFactory.createBool(not ^ (predicandValue.greaterThanEqual(begin).asBool()
                && predicandValue.lessThanEqual(end).asBool()));
      } else {
        context.result = NullDatum.get();
      }
    }
  }

  private static class AsymmetricChecker implements Checker {
    EvalNode predicand;
    EvalNode begin;
    EvalNode end;
    private boolean not;

    private AsymmetricChecker(boolean not, EvalNode predicand, EvalNode begin, EvalNode end) {
      this.not = not;
      this.predicand = predicand;
      this.begin = begin;
      this.end = end;
    }

    @Override
    public void eval(BetweenContext context, Schema schema, Tuple param) {
      predicand.eval(context.predicandContext, schema, param);
      Datum predicandValue = predicand.terminate(context.predicandContext);
      begin.eval(context.beginContext, schema, param);
      Datum beginValue = begin.terminate(context.beginContext);
      end.eval(context.endContext, schema, param);
      Datum endValue = begin.terminate(context.endContext);

      if (!(predicandValue instanceof NullDatum || beginValue instanceof NullDatum || endValue instanceof NullDatum)) {
        context.result =
            DatumFactory.createBool(not ^ (predicandValue.greaterThanEqual(beginValue).asBool()
                && predicandValue.lessThanEqual(endValue).asBool()));
      } else {
        context.result = NullDatum.get();
      }
    }
  }

  private static class SymmetricChecker implements Checker {
    boolean not;
    EvalNode predicand;
    EvalNode begin;
    EvalNode end;

    SymmetricChecker(boolean not, EvalNode predicand, EvalNode begin, EvalNode end) {
      this.not = not;
      this.predicand = predicand;
      this.begin = begin;
      this.end = end;
    }

    @Override
    public void eval(BetweenContext context, Schema schema, Tuple param) {
      predicand.eval(context.predicandContext, schema, param);
      Datum predicandValue = predicand.terminate(context.predicandContext);
      begin.eval(context.beginContext, schema, param);
      Datum beginValue = begin.terminate(context.beginContext);
      end.eval(context.endContext, schema, param);
      Datum endValue = begin.terminate(context.endContext);

      if (!(predicandValue instanceof NullDatum || beginValue instanceof NullDatum || endValue instanceof NullDatum)) {
        context.result = DatumFactory.createBool( not ^
            (predicandValue.greaterThanEqual(beginValue).asBool() && predicandValue.lessThanEqual(endValue).asBool()) ||
            (predicandValue.lessThanEqual(beginValue).asBool() && predicandValue.greaterThanEqual(endValue).asBool())
        );
      } else {
        context.result = NullDatum.get();
      }
    }
  }

  @Override
  public EvalContext newContext() {
    return new BetweenContext();
  }

  @Override
  public TajoDataTypes.DataType getValueType() {
    return RES_TYPE;
  }

  @Override
  public String getName() {
    return "between";
  }

  @Override
  public String toString() {
    return predicand + " BETWEEN " + (symmetric ? "SYMMETRIC" : "ASYMMETRIC") + " " + begin + " AND " + end;
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    if (checker == null) {
      if (begin.getType() == EvalType.CONST && end.getType() == EvalType.CONST) {
        Datum beginValue = begin.terminate(null);
        Datum endValue = end.terminate(null);

        if (symmetric || beginValue.compareTo(endValue) <= 0) {
          checker = new ConstantChecker(not, predicand, begin.terminate(null), end.terminate(null));
        } else {
          checker = new AsymmetricChecker(not, predicand, begin, end);
        }
      } else {
        if (symmetric) {
          checker = new SymmetricChecker(not, predicand, begin, end);
        } else {
          checker = new AsymmetricChecker(not, predicand, begin, end);
        }
      }
    }

    checker.eval((BetweenContext) ctx, schema, tuple);
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    return ((BetweenContext)ctx).result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BetweenPredicateEval) {
      BetweenPredicateEval another = (BetweenPredicateEval) obj;
      return not == another.not && symmetric == another.symmetric && predicand.equals(another.predicand) &&
          begin.equals(another.begin) && end.equals(another.end);
    }
    return false;
  }

  private class BetweenContext implements EvalContext {
    private EvalContext predicandContext;
    private EvalContext beginContext;
    private EvalContext endContext;
    private Datum result;

    BetweenContext() {
      predicandContext = predicand.newContext();
      beginContext = begin.newContext();
      endContext = end.newContext();
    }
  }

  @Deprecated
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    predicand.preOrder(visitor);
    begin.preOrder(visitor);
    end.preOrder(visitor);
  }

  @Deprecated
  public void postOrder(EvalNodeVisitor visitor) {
    predicand.postOrder(visitor);
    begin.postOrder(visitor);
    end.postOrder(visitor);
    visitor.visit(this);
  }
}