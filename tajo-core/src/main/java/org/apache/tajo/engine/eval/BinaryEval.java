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

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.storage.Tuple;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class BinaryEval extends EvalNode implements Cloneable {
  @Expose protected EvalNode leftExpr;
  @Expose protected EvalNode rightExpr;

  protected BinaryEval(EvalType type) {
    super(type);
  }

  public BinaryEval(EvalType type, EvalNode left, EvalNode right) {
    super(type);
    this.leftExpr = left;
    this.rightExpr = right;

    if(
        type == EvalType.AND ||
            type == EvalType.OR ||
            type == EvalType.EQUAL ||
            type == EvalType.NOT_EQUAL ||
            type == EvalType.LTH ||
            type == EvalType.GTH ||
            type == EvalType.LEQ ||
            type == EvalType.GEQ ) {
      this.returnType = CatalogUtil.newSimpleDataType(Type.BOOLEAN);
    } else if (
        type == EvalType.PLUS ||
            type == EvalType.MINUS ||
            type == EvalType.MULTIPLY ||
            type == EvalType.DIVIDE ||
            type == EvalType.MODULAR ) {
      this.returnType = determineType(left.getValueType(), right.getValueType());

    } else if (type == EvalType.CONCATENATE) {
      this.returnType = CatalogUtil.newSimpleDataType(Type.TEXT);
    }
  }

  public BinaryEval(PartialBinaryExpr expr) {
    this(expr.type, expr.leftExpr, expr.rightExpr);
  }

  public void setLeftExpr(EvalNode expr) {
    this.leftExpr = expr;
  }

  public <T extends EvalNode> T getLeftExpr() {
    return (T) this.leftExpr;
  }

  public void setRightExpr(EvalNode expr) {
    this.rightExpr = expr;
  }

  public <T extends EvalNode> T getRightExpr() {
    return (T) this.rightExpr;
  }

  public EvalNode getExpr(int id) {
    if (id == 0) {
      return this.leftExpr;
    } else if (id == 1) {
      return this.rightExpr;
    } else {
      throw new ArrayIndexOutOfBoundsException("only 0 or 1 is available (" + id + " is not available)");
    }
  }

  /**
   * This is verified by ExprsVerifier.checkArithmeticOperand().
   */
  public static DataType determineType(DataType left, DataType right) throws InvalidEvalException {
    switch (left.getType()) {

    case INT1:
    case INT2:
    case INT4: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4: return CatalogUtil.newSimpleDataType(Type.INT4);
      case INT8: return CatalogUtil.newSimpleDataType(Type.INT8);
      case FLOAT4: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.FLOAT8);
      case DATE: return CatalogUtil.newSimpleDataType(Type.DATE);
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case INT8: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4:
      case INT8: return CatalogUtil.newSimpleDataType(Type.INT8);
      case FLOAT4: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.FLOAT8);
      case DATE: return CatalogUtil.newSimpleDataType(Type.DATE);
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case FLOAT4: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case INT8: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case FLOAT4: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.FLOAT8);
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case FLOAT8: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.FLOAT8);
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case DATE: {
      switch(right.getType()) {
      case INT2:
      case INT4:
      case INT8: return CatalogUtil.newSimpleDataType(Type.DATE);
      case INTERVAL:
      case TIME: return CatalogUtil.newSimpleDataType(Type.TIMESTAMP);
      case DATE: return CatalogUtil.newSimpleDataType(Type.INT4);
      }
    }

    case TIME: {
      switch(right.getType()) {
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.TIME);
      case TIME: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      case DATE: return CatalogUtil.newSimpleDataType(Type.INT4);
      }
    }

    case TIMESTAMP: {
      switch (right.getType()) {
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.TIMESTAMP);
      case TIMESTAMP: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case INTERVAL: {
      switch (right.getType()) {
      case INTERVAL:
      case FLOAT4:
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    default: return left;
    }
  }

  @Override
  public Datum eval(Schema schema, Tuple tuple) {
    Datum lhs = leftExpr.eval(schema, tuple);
    Datum rhs = rightExpr.eval(schema, tuple);

    switch(type) {
    case AND:
      return lhs.and(rhs);
    case OR:
      return lhs.or(rhs);

    case EQUAL:
      return lhs.equalsTo(rhs);
    case NOT_EQUAL:
      return lhs.notEqualsTo(rhs);
    case LTH:
      return lhs.lessThan(rhs);
    case LEQ:
      return lhs.lessThanEqual(rhs);
    case GTH:
      return lhs.greaterThan(rhs);
    case GEQ:
      return lhs.greaterThanEqual(rhs);

    case PLUS:
      return lhs.plus(rhs);
    case MINUS:
      return lhs.minus(rhs);
    case MULTIPLY:
      return lhs.multiply(rhs);
    case DIVIDE:
      return lhs.divide(rhs);
    case MODULAR:
      return lhs.modular(rhs);

    case CONCATENATE:
      if (lhs.type() == Type.NULL_TYPE || rhs.type() == Type.NULL_TYPE) {
        return NullDatum.get();
      }
      return DatumFactory.createText(lhs.asChars() + rhs.asChars());
    default:
      throw new InvalidOperationException("Unknown binary operation: " + type);
    }
  }

  @Override
	public String getName() {
		return type.name();
	}

	@Override
	public DataType getValueType() {
	  return returnType;
	}

  @Deprecated
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    leftExpr.preOrder(visitor);
    rightExpr.preOrder(visitor);
  }

  @Deprecated
  public void postOrder(EvalNodeVisitor visitor) {
    leftExpr.postOrder(visitor);
    rightExpr.postOrder(visitor);
    visitor.visit(this);
  }

	public String toString() {
		return leftExpr +" " + type.getOperatorName() + " "+ rightExpr;
	}

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BinaryEval) {
      BinaryEval other = (BinaryEval) obj;

      boolean b1 = this.type == other.type;
      boolean b2 = leftExpr.equals(other.leftExpr);
      boolean b3 = rightExpr.equals(other.rightExpr);
      return b1 && b2 && b3;
    }
    return false;
  }

  public int hashCode() {
    return Objects.hashCode(this.type, leftExpr, rightExpr);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    BinaryEval node = (BinaryEval) super.clone();
    node.type = type;
    node.leftExpr = leftExpr != null ? (EvalNode) leftExpr.clone() : null;
    node.rightExpr = rightExpr != null ? (EvalNode) rightExpr.clone() : null;

    return node;
  }
}
