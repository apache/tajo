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
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;

public class BinaryEval extends EvalNode implements Cloneable {
	@Expose private DataType returnType = null;

  private class BinaryEvalCtx implements EvalContext {
    EvalContext left;
    EvalContext right;
  }

	/**
	 * @param type
	 */
	public BinaryEval(EvalType type, EvalNode left, EvalNode right) {
		super(type, left, right);		
		Preconditions.checkNotNull(type);
		Preconditions.checkNotNull(left);
		Preconditions.checkNotNull(right);
		
		if(
			type == EvalType.AND ||
			type == EvalType.OR ||
			type == EvalType.EQUAL ||
			type == EvalType.LTH ||
			type == EvalType.GTH ||
			type == EvalType.LEQ ||
			type == EvalType.GEQ ) {
			this.returnType = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.BOOLEAN);
		} else if (
			type == EvalType.PLUS ||
			type == EvalType.MINUS ||
			type == EvalType.MULTIPLY ||
			type == EvalType.DIVIDE ||
      type == EvalType.MODULAR ) {
			this.returnType = determineType(left.getValueType(), right.getValueType());

		} else if (type == EvalType.CONCATENATE) {
      this.returnType = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT);
    }
	}

  public BinaryEval(PartialBinaryExpr expr) {
	  this(expr.type, expr.leftExpr, expr.rightExpr);
	}

  @Override
  public EvalContext newContext() {
    BinaryEvalCtx newCtx =  new BinaryEvalCtx();
    newCtx.left = leftExpr.newContext();
    newCtx.right = rightExpr.newContext();

    return newCtx;
  }

  /**
   * This is verified by ExprsVerifier.checkArithmeticOperand().
   */
  private DataType determineType(DataType left, DataType right) throws InvalidEvalException {
    switch (left.getType()) {
      case INT4: {
        switch(right.getType()) {
          case INT2:
          case INT4: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4);
          case INT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8);
          case FLOAT4: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT4);
          case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
        }
      }

      case INT8: {
        switch(right.getType()) {
          case INT2:
          case INT4:
          case INT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8);
          case FLOAT4:
          case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
        }
      }

      case FLOAT4: {
        switch(right.getType()) {
          case INT2:
          case INT4:
          case INT8:
          case FLOAT4:
          case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
        }
      }

      case FLOAT8: {
        switch(right.getType()) {
          case INT2:
          case INT4:
          case INT8:
          case FLOAT4:
          case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
        }
      }

      default: return left;
    }
  }

	/* (non-Javadoc)
	 * @see nta.query.executor.eval.Expr#evalBool(Tuple)
	 */
	@Override
	public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    BinaryEvalCtx binCtx = (BinaryEvalCtx) ctx;
	  leftExpr.eval(binCtx == null ? null : binCtx.left, schema, tuple);
    rightExpr.eval(binCtx == null ? null : binCtx.right, schema, tuple);
	}

  @Override
  public Datum terminate(EvalContext ctx) {
    BinaryEvalCtx binCtx = (BinaryEvalCtx) ctx;

    switch(type) {
      case AND:
        return DatumFactory.createBool(leftExpr.terminate(binCtx.left).asBool()
            && rightExpr.terminate(binCtx.right).asBool());
      case OR:
        return DatumFactory.createBool(leftExpr.terminate(binCtx.left).asBool()
            || rightExpr.terminate(binCtx.right).asBool());

      case EQUAL:
        return leftExpr.terminate(binCtx.left).equalsTo(rightExpr.terminate(binCtx.right));
      case NOT_EQUAL:
        return DatumFactory.createBool(!leftExpr.terminate(binCtx.left).equalsTo(rightExpr.terminate(binCtx.right)).
            asBool());
      case LTH:
        return leftExpr.terminate(binCtx.left).lessThan(rightExpr.terminate(binCtx.right));
      case LEQ:
        return leftExpr.terminate(binCtx.left).lessThanEqual(rightExpr.terminate(binCtx.right));
      case GTH:
        return leftExpr.terminate(binCtx.left).greaterThan(rightExpr.terminate(binCtx.right));
      case GEQ:
        return leftExpr.terminate(binCtx.left).greaterThanEqual(rightExpr.terminate(binCtx.right));

      case PLUS:
        return leftExpr.terminate(binCtx.left).plus(rightExpr.terminate(binCtx.right));
      case MINUS:
        return leftExpr.terminate(binCtx.left).minus(rightExpr.terminate(binCtx.right));
      case MULTIPLY:
        return leftExpr.terminate(binCtx.left).multiply(rightExpr.terminate(binCtx.right));
      case DIVIDE:
        return leftExpr.terminate(binCtx.left).divide(rightExpr.terminate(binCtx.right));
      case MODULAR:
        return leftExpr.terminate(binCtx.left).modular(rightExpr.terminate(binCtx.right));

      case CONCATENATE:
        return DatumFactory.createText(leftExpr.terminate(binCtx.left).asChars()
            + rightExpr.terminate(binCtx.right).asChars());
      default:
        throw new InvalidEvalException("We does not support " + type + " expression yet");
    }
  }

  @Override
	public String getName() {
		return "?";
	}
	
	@Override
	public DataType getValueType() {
	  return returnType;
	}
	
	public String toString() {
		return leftExpr +" " + type.getOperatorName() + " "+rightExpr;
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
    BinaryEval eval = (BinaryEval) super.clone();
    eval.returnType = returnType;
    
    return eval;
  }
}
