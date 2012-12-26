/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.eval;

import tajo.catalog.Column;

import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public class AlgebraicUtil {
  
  /**
   * Transpose a given comparison expression into the expression 
   * where the variable corresponding to the target is placed 
   * on the left-hand side.
   * 
   * @param expr
   * @param target
   * @return Transposed expression
   */
  public static EvalNode transpose(EvalNode expr, Column target) {
    EvalNode commutated = null;
    // If the variable is in the right term, inverse the expr.
    if (!EvalTreeUtil.containColumnRef(expr.getLeftExpr(), target)) {
      // the commutate method works with a copy of the expr
      commutated = commutate(expr);
    } else {
      try {
        commutated = (EvalNode) expr.clone();
      } catch (CloneNotSupportedException e) {
        throw new AlgebraicException(e);
      }
    }

    return _transpose(commutated, target);
  }
  
  private static EvalNode _transpose(EvalNode _expr, Column target) {
     EvalNode expr = simplify(_expr);
     
     if (isSingleVar(expr.getLeftExpr())) {
       return expr;
     }
     
     EvalNode left = expr.getLeftExpr();     
     EvalNode lTerm = null;
     EvalNode rTerm = null;
     
    if (left.getType() == EvalNode.Type.PLUS
        || left.getType() == EvalNode.Type.MINUS
        || left.getType() == EvalNode.Type.MULTIPLY
        || left.getType() == EvalNode.Type.DIVIDE) {
      
      // If the left-left term is a variable, the left-right term is transposed.
      if(EvalTreeUtil.containColumnRef(left.getLeftExpr(), target)) {
        PartialBinaryExpr tmpTerm = splitRightTerm(left);
        tmpTerm.type = inverseOperator(tmpTerm.type);
        tmpTerm.setLeftExpr(expr.getRightExpr());
        lTerm = left.getLeftExpr();
        rTerm = new BinaryEval(tmpTerm);
      } else { 
        // Otherwise, the left-right term is transposed into the left-left term.
        PartialBinaryExpr tmpTerm = splitLeftTerm(left);
        tmpTerm.type = inverseOperator(tmpTerm.type);
        tmpTerm.setLeftExpr(expr.getRightExpr());        
        lTerm = left.getRightExpr();
        rTerm = new BinaryEval(tmpTerm);    
      }
    }
    
    return _transpose(new BinaryEval(expr.getType(), lTerm, rTerm), target);
  }
  
  /**
   * Inverse a given operator (+, -, *, /)
   * 
   * @param type
   * @return inversed operator type
   */
  public static EvalNode.Type inverseOperator(EvalNode.Type type) {
    switch (type) {
    case PLUS:
      return EvalNode.Type.MINUS;
    case MINUS:
      return EvalNode.Type.PLUS;
    case MULTIPLY:
      return EvalNode.Type.DIVIDE;
    case DIVIDE:
      return EvalNode.Type.MULTIPLY;
    default : throw new AlgebraicException("ERROR: cannot inverse the operator: " 
      + type);
    }
  }
  
  /**
   * Examine if a given expr is a variable.
   * 
   * @param node
   * @return true if a given expr is a variable.
   */
  private static boolean isSingleVar(EvalNode node) {
    if (node.getType() == EvalNode.Type.FIELD) {
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Simplify the given expr. That is, all subexprs consisting of only constants
   * are calculated immediately.
   * 
   * @param expr to be simplified
   * @return the simplified expr
   */
  public static EvalNode simplify(EvalNode expr) {
    EvalNode left = expr.getLeftExpr();
    EvalNode right = expr.getRightExpr();
    
    switch (expr.getType()) {
    case AND:
    case OR:
    case EQUAL:
    case LTH:
    case LEQ:
    case GTH:
    case GEQ:
      left = simplify(left);
      right = simplify(right);      
      return new BinaryEval(expr.getType(), left, right);    
    
    case PLUS:
    case MINUS:
    case MULTIPLY:
    case DIVIDE:
      left = simplify(left);
      right = simplify(right);
      
      // If both are constants, they can be evaluated immediately.
      if (left.getType() == EvalNode.Type.CONST
          && right.getType() == EvalNode.Type.CONST) {
        EvalContext exprCtx = expr.newContext();
        expr.eval(exprCtx, null, null);
        return new ConstEval(expr.terminate(exprCtx));
      } else {
        return new BinaryEval(expr.getType(), left, right);            
      }
      
    case CONST:
      return expr;
      
    default: new AlgebraicException("Wrong expression: " + expr);
    }
    return expr;
  }
  
  /** 
   * @param expr to be evaluated if the expr includes one variable
   * @return true if expr has only one field
   */
  public static boolean containSingleVar(EvalNode expr) {
    Map<EvalNode.Type, Integer> counter = EvalTreeUtil.getExprCounters(expr);
    
    int sum = 0;
    for (Integer cnt : counter.values()) {      
      sum += cnt;
    }
    
    if (sum == 1 && counter.get(EvalNode.Type.FIELD) == 1) {
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Split the left term and transform it into the right deep expression.
   * 
   * @param expr - notice the left term of this expr will be eliminated 
   * after done.
   * @return the separated expression changed into the right deep expression.  
   * For example, the expr 'x * y' is transformed into '* x'.  
   *
   */
  public static PartialBinaryExpr splitLeftTerm(EvalNode expr) {
    
    if (!(expr.getType() == EvalNode.Type.PLUS
        || expr.getType() == EvalNode.Type.MINUS
        || expr.getType() == EvalNode.Type.MULTIPLY
        || expr.getType() == EvalNode.Type.DIVIDE)) {
      throw new AlgebraicException("Invalid algebraic operation: " + expr);
    }
    
    if (expr.getLeftExpr().getType() != EvalNode.Type.CONST) {
      return splitLeftTerm(expr.getLeftExpr());
    }
    
    PartialBinaryExpr splitted = 
        new PartialBinaryExpr(expr.getType(), null, expr.getLeftExpr());
    expr.setLeftExpr(null);
    return splitted;
  }
  
  /**
   * Split the left term and transform it into the right deep expression.
   * 
   * @param expr - to be splited
   * @return the separated expression changed into the right deep expression.
   * For example, the expr 'x * y' is transformed into '* y'. 
   *
   * @throws CloneNotSupportedException
   */
  public static PartialBinaryExpr splitRightTerm(EvalNode expr) {
    
    if (!(expr.getType() == EvalNode.Type.PLUS
        || expr.getType() == EvalNode.Type.MINUS
        || expr.getType() == EvalNode.Type.MULTIPLY
        || expr.getType() == EvalNode.Type.DIVIDE)) {
      throw new AlgebraicException("Invalid algebraic operation: " + expr);
    }
    
    if (expr.getRightExpr().getType() != EvalNode.Type.CONST) {
      return splitRightTerm(expr.getRightExpr());
    }
    
    PartialBinaryExpr splitted = 
        new PartialBinaryExpr(expr.getType(), null, expr.getRightExpr());
    expr.setRightExpr(null);
    return splitted;
  }
  
  /**
   * Commutate two terms which are added, subtracted and multiplied.
   * 
   * @param inputExpr
   * @return
   */
  public static EvalNode commutate(EvalNode inputExpr) {
    EvalNode expr;
    switch (inputExpr.getType()) {
    case AND:
    case OR:
    case EQUAL:
    case PLUS:
    case MINUS:
    case MULTIPLY: // these types can be commutated w/o any change
      expr = EvalTreeFactory.create(inputExpr.getType(),
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
      
    case GTH:
      expr = EvalTreeFactory.create(EvalNode.Type.LTH,
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
    case GEQ:
      expr = EvalTreeFactory.create(EvalNode.Type.LEQ,
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
    case LTH:
      expr = EvalTreeFactory.create(EvalNode.Type.GTH,
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
    case LEQ:
      expr = EvalTreeFactory.create(EvalNode.Type.GEQ,
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
      
    default :
      throw new AlgebraicException("Cannot commutate the expr: " + inputExpr);
    }
    
    return expr;
  }
}
