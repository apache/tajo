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

package org.apache.tajo.plan.expr;

import com.google.gson.annotations.Expose;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;

public class BetweenPredicateEval extends EvalNode implements Cloneable {
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

  public boolean isNot() {
    return not;
  }

  public boolean isSymmetric() {
    return symmetric;
  }

  public void setPredicand(EvalNode predicand) {
    this.predicand = predicand;
  }

  public EvalNode getPredicand() {
    return predicand;
  }

  public void setBegin(EvalNode begin) {
    this.begin = begin;
  }

  public EvalNode getBegin() {
    return begin;
  }

  public void setEnd(EvalNode end) {
    this.end = end;
  }

  public EvalNode getEnd() {
    return end;
  }

  private static interface Checker {
    void bind(Schema schema);
    Datum eval(Tuple param);
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
    public void bind(Schema schema) {
      predicand.bind(schema);
    }

    @Override
    public Datum eval(Tuple param) {
      Datum predicandValue = predicand.eval(param);

      if (!predicandValue.isNull()) {
        return DatumFactory.createBool(not ^ (predicandValue.greaterThanEqual(begin).asBool()
                && predicandValue.lessThanEqual(end).asBool()));
      } else {
        return NullDatum.get();
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
    public void bind(Schema schema) {
      predicand.bind(schema);
      begin.bind(schema);
      end.bind(schema);
    }

    @Override
    public Datum eval(Tuple param) {
      Datum predicandValue = predicand.eval(param);
      Datum beginValue = begin.eval(param);
      Datum endValue = end.eval(param);

      if (!(predicandValue.isNull() || beginValue.isNull() || endValue.isNull())) {
        return
            DatumFactory.createBool(not ^ (predicandValue.greaterThanEqual(beginValue).asBool()
                && predicandValue.lessThanEqual(endValue).asBool()));
      } else {
        return NullDatum.get();
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
    public void bind(Schema schema) {
      predicand.bind(schema);
      begin.bind(schema);
      end.bind(schema);
    }

    @Override
    public Datum eval(Tuple param) {
      Datum predicandValue = predicand.eval(param);
      Datum beginValue = begin.eval(param);
      Datum endValue = end.eval(param);

      if (!(predicandValue.isNull()|| beginValue.isNull() || endValue.isNull())) {
        return DatumFactory.createBool( not ^
            (predicandValue.greaterThanEqual(beginValue).asBool() && predicandValue.lessThanEqual(endValue).asBool()) ||
            (predicandValue.lessThanEqual(beginValue).asBool() && predicandValue.greaterThanEqual(endValue).asBool())
        );
      } else {
        return NullDatum.get();
      }
    }
  }

  @Override
  public TajoDataTypes.DataType getValueType() {
    return RES_TYPE;
  }

  @Override
  public int childNum() {
    return 3;
  }

  @Override
  public EvalNode getChild(int idx) {
    if (idx == 0) {
      return predicand;
    } else if (idx == 1) {
      return begin;
    } else if (idx == 2) {
      return end;
    } else {
      throw new ArrayIndexOutOfBoundsException(idx);
    }
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
  public void bind(Schema schema) {
    if (begin.getType() == EvalType.CONST && end.getType() == EvalType.CONST) {
      Datum beginValue = ((ConstEval)begin).getValue();
      Datum endValue = ((ConstEval)end).getValue();

      if (symmetric || beginValue.compareTo(endValue) <= 0) {
        checker = new ConstantChecker(not, predicand, beginValue, endValue);
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
    checker.bind(schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    return checker.eval(tuple);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((begin == null) ? 0 : begin.hashCode());
    result = prime * result + ((checker == null) ? 0 : checker.hashCode());
    result = prime * result + ((end == null) ? 0 : end.hashCode());
    result = prime * result + (not ? 1231 : 1237);
    result = prime * result + ((predicand == null) ? 0 : predicand.hashCode());
    result = prime * result + (symmetric ? 1231 : 1237);
    return result;
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

  @Override
  public Object clone() throws CloneNotSupportedException {
    BetweenPredicateEval newBetween = (BetweenPredicateEval) super.clone();
    newBetween.not = not;
    newBetween.symmetric = symmetric;
    newBetween.predicand = predicand;
    newBetween.begin = begin;
    newBetween.end = end;
    return newBetween;
  }
}