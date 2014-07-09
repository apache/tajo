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

import org.apache.tajo.catalog.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class AlgebraicUtil {
  
  /**
   * Transpose a given comparison expression into the expression 
   * where the variable corresponding to the target is placed 
   * on the left-hand side.
   * 
   * @param evalNode
   * @param target
   * @return Transposed expression
   */
  public static EvalNode transpose(EvalNode evalNode, Column target) {
    BinaryEval commutated = null;

    if (evalNode instanceof BinaryEval) { // if it is binary
      BinaryEval binaryEval = (BinaryEval) evalNode;
      // If the variable is in the right term, inverse the expr.
      if (!EvalTreeUtil.containColumnRef(binaryEval.getLeftExpr(), target)) {
        // the commutate method works with a copy of the expr
        commutated = commutate(binaryEval);
      } else {
        try {
          commutated = (BinaryEval) evalNode.clone();
        } catch (CloneNotSupportedException e) {
          throw new AlgebraicException(e);
        }
      }

      return _transpose(commutated, target);
    } else {
      return evalNode;
    }
  }
  
  private static EvalNode _transpose(BinaryEval _expr, Column target) {
     EvalNode expr = eliminateConstantExprs(_expr);

    if (expr instanceof BinaryEval) { // only if expr is a binary operator
      BinaryEval binaryEval = (BinaryEval) expr;
      if (isSingleVar(binaryEval.getLeftExpr())) {
        return expr;
      }

      EvalNode left = binaryEval.getLeftExpr();
      EvalNode lTerm = null;
      EvalNode rTerm = null;

      if (EvalType.isArithmeticOperator(left)) { // we can ensure that left is binary.

        // If the left-left term is a variable, the left-right term is transposed.
        if (EvalTreeUtil.containColumnRef(((BinaryEval)left).getLeftExpr(), target)) {
          PartialBinaryExpr tmpTerm = splitRightTerm((BinaryEval) left);
          tmpTerm.type = inverseOperator(tmpTerm.type);
          tmpTerm.setLeftExpr(((BinaryEval)expr).getRightExpr());
          lTerm = ((BinaryEval)left).getLeftExpr();
          rTerm = new BinaryEval(tmpTerm);
        } else {
          // Otherwise, the left-right term is transposed into the left-left term.
          PartialBinaryExpr tmpTerm = splitLeftTerm((BinaryEval) left);
          tmpTerm.type = inverseOperator(tmpTerm.type);
          tmpTerm.setLeftExpr(((BinaryEval)expr).getRightExpr());
          lTerm = ((BinaryEval)left).getRightExpr();
          rTerm = new BinaryEval(tmpTerm);
        }
      }

      return _transpose(new BinaryEval(expr.getType(), lTerm, rTerm), target);
    } else {
      return _expr;
    }
  }
  
  /**
   * Inverse a given operator (+, -, *, /)
   * 
   * @param type
   * @return inversed operator type
   */
  public static EvalType inverseOperator(EvalType type) {
    switch (type) {
    case PLUS:
      return EvalType.MINUS;
    case MINUS:
      return EvalType.PLUS;
    case MULTIPLY:
      return EvalType.DIVIDE;
    case DIVIDE:
      return EvalType.MULTIPLY;
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
    if (node.getType() == EvalType.FIELD) {
      return true;
    } else {
      return false;
    }
  }

  private static class AlgebraicOptimizer extends SimpleEvalNodeVisitor<Object> {

    @Override
    public EvalNode visitBinaryEval(Object context, Stack<EvalNode> stack, BinaryEval binaryEval) {
      stack.push(binaryEval);
      EvalNode lhs = visit(context, binaryEval.getLeftExpr(), stack);
      EvalNode rhs = visit(context, binaryEval.getRightExpr(), stack);
      stack.pop();

      if (!binaryEval.getLeftExpr().equals(lhs)) {
        binaryEval.setLeftExpr(lhs);
      }
      if (!binaryEval.getRightExpr().equals(rhs)) {
        binaryEval.setRightExpr(rhs);
      }

      if (lhs.getType() == EvalType.CONST && rhs.getType() == EvalType.CONST) {
        return new ConstEval(binaryEval.eval(null, null));
      }

      return binaryEval;
    }

    @Override
    public EvalNode visitUnaryEval(Object context, Stack<EvalNode> stack, UnaryEval unaryEval) {
      stack.push(unaryEval);
      EvalNode child = visit(context, unaryEval.getChild(), stack);
      stack.pop();

      if (child.getType() == EvalType.CONST) {
        return new ConstEval(unaryEval.eval(null, null));
      }

      return unaryEval;
    }

    public EvalNode visitFuncCall(Object context, GeneralFunctionEval evalNode, Stack<EvalNode> stack) {
      boolean constant = true;

      if ("sleep".equals(evalNode.funcDesc.getSignature())) {
        constant = false;
      } else {
        for (EvalNode arg : evalNode.getArgs()) {
          arg = visit(context, arg, stack);
          constant &= (arg.getType() == EvalType.CONST);
        }
      }

      if (constant) {
        return new ConstEval(evalNode.eval(null, null));
      } else {
        return evalNode;
      }
    }
  }

  private final static AlgebraicOptimizer algebraicOptimizer = new AlgebraicOptimizer();
  
  /**
   * Simplify the given expr. That is, all subexprs consisting of only constants
   * are calculated immediately.
   * 
   * @param expr to be simplified
   * @return the simplified expr
   */
  public static EvalNode eliminateConstantExprs(EvalNode expr) {
    return algebraicOptimizer.visit(null, expr, new Stack<EvalNode>());
  }
  
  /** 
   * @param expr to be evaluated if the expr includes one variable
   * @return true if expr has only one field
   */
  public static boolean containSingleVar(EvalNode expr) {
    Map<EvalType, Integer> counter = EvalTreeUtil.getExprCounters(expr);
    
    int sum = 0;
    for (Integer cnt : counter.values()) {      
      sum += cnt;
    }
    
    if (sum == 1 && counter.get(EvalType.FIELD) == 1) {
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Split the left term and transform it into the right deep expression.
   * 
   * @param binary - notice the left term of this expr will be eliminated
   * after done.
   * @return the separated expression changed into the right deep expression.  
   * For example, the expr 'x * y' is transformed into '* x'.  
   *
   */
  public static PartialBinaryExpr splitLeftTerm(BinaryEval binary) {
    
    if (!(EvalType.isArithmeticOperator(binary))) {
      throw new AlgebraicException("Invalid algebraic operation: " + binary);
    }
    
    if (binary.getLeftExpr() instanceof BinaryEval) {
      return splitLeftTerm((BinaryEval) binary.getLeftExpr());
    }
    
    PartialBinaryExpr splitted = 
        new PartialBinaryExpr(binary.getType(), null, binary.getLeftExpr());
    binary.setLeftExpr(null);
    return splitted;
  }
  
  /**
   * Split the left term and transform it into the right deep expression.
   * 
   * @param binary - to be splited
   * @return the separated expression changed into the right deep expression.
   * For example, the expr 'x * y' is transformed into '* y'. 
   *
   * @throws CloneNotSupportedException
   */
  public static PartialBinaryExpr splitRightTerm(BinaryEval binary) {
    
    if (!(EvalType.isArithmeticOperator(binary))) {
      throw new AlgebraicException("Invalid algebraic operation: " + binary);
    }
    
    if (binary.getRightExpr() instanceof BinaryEval) {
      return splitRightTerm((BinaryEval) binary.getRightExpr());
    }
    
    PartialBinaryExpr splitted = 
        new PartialBinaryExpr(binary.getType(), null, binary.getRightExpr());
    binary.setRightExpr(null);
    return splitted;
  }
  
  /**
   * Commutate two terms which are added, subtracted and multiplied.
   * 
   * @param inputExpr
   * @return
   */
  public static BinaryEval commutate(BinaryEval inputExpr) {
    BinaryEval rewritten;
    switch (inputExpr.getType()) {
    case AND:
    case OR:
    case EQUAL:
    case PLUS:
    case MINUS:
    case MULTIPLY: // these types can be commutated w/o any change
      rewritten = EvalTreeFactory.create(inputExpr.getType(),
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
      
    case GTH:
      rewritten = EvalTreeFactory.create(EvalType.LTH,
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
    case GEQ:
      rewritten = EvalTreeFactory.create(EvalType.LEQ,
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
    case LTH:
      rewritten = EvalTreeFactory.create(EvalType.GTH,
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
    case LEQ:
      rewritten = EvalTreeFactory.create(EvalType.GEQ,
          inputExpr.getRightExpr(), inputExpr.getLeftExpr());
      break;
      
    default :
      throw new AlgebraicException("Cannot commutate the expr: " + inputExpr);
    }
    
    return rewritten;
  }

  public static boolean isIndexableOperator(EvalNode expr) {
    return expr.getType() == EvalType.EQUAL ||
        expr.getType() == EvalType.LEQ ||
        expr.getType() == EvalType.LTH ||
        expr.getType() == EvalType.GEQ ||
        expr.getType() == EvalType.GTH ||
        expr.getType() == EvalType.BETWEEN ||
        expr.getType() == EvalType.IN ||
        (expr.getType() == EvalType.LIKE && !((LikePredicateEval)expr).isLeadingWildCard());
  }

  /**
   * Convert a list of conjunctive normal forms into a singleton expression.
   *
   * @param cnfExprs
   * @return The EvalNode object that merges all CNF-formed expressions.
   */
  public static EvalNode createSingletonExprFromCNF(EvalNode... cnfExprs) {
    if (cnfExprs.length == 1) {
      return cnfExprs[0];
    }

    return createSingletonExprFromCNFRecursive(cnfExprs, 0);
  }

  private static EvalNode createSingletonExprFromCNFRecursive(EvalNode[] evalNode, int idx) {
    if (idx == evalNode.length - 2) {
      return new BinaryEval(EvalType.AND, evalNode[idx], evalNode[idx + 1]);
    } else {
      return new BinaryEval(EvalType.AND, evalNode[idx], createSingletonExprFromCNFRecursive(evalNode, idx + 1));
    }
  }

  /**
   * Transforms a expression to an array of conjunctive normal formed expressions.
   *
   * @param expr The expression to be transformed to an array of CNF-formed expressions.
   * @return An array of CNF-formed expressions
   */
  public static EvalNode [] toConjunctiveNormalFormArray(EvalNode expr) {
    List<EvalNode> list = new ArrayList<EvalNode>();
    toConjunctiveNormalFormArrayRecursive(expr, list);
    return list.toArray(new EvalNode[list.size()]);
  }

  private static void toConjunctiveNormalFormArrayRecursive(EvalNode node, List<EvalNode> found) {
    if (node.getType() == EvalType.AND) {
      toConjunctiveNormalFormArrayRecursive(((BinaryEval)node).getLeftExpr(), found);
      toConjunctiveNormalFormArrayRecursive(((BinaryEval)node).getRightExpr(), found);
    } else {
      found.add(node);
    }
  }

  /**
   * Convert a list of conjunctive normal forms into a singleton expression.
   *
   * @param cnfExprs
   * @return The EvalNode object that merges all CNF-formed expressions.
   */
  public static EvalNode createSingletonExprFromDNF(EvalNode... cnfExprs) {
    if (cnfExprs.length == 1) {
      return cnfExprs[0];
    }

    return createSingletonExprFromDNFRecursive(cnfExprs, 0);
  }

  private static EvalNode createSingletonExprFromDNFRecursive(EvalNode[] evalNode, int idx) {
    if (idx == evalNode.length - 2) {
      return new BinaryEval(EvalType.OR, evalNode[idx], evalNode[idx + 1]);
    } else {
      return new BinaryEval(EvalType.OR, evalNode[idx], createSingletonExprFromDNFRecursive(evalNode, idx + 1));
    }
  }

  /**
   * Transforms a expression to an array of disjunctive normal formed expressions.
   *
   * @param exprs The expressions to be transformed to an array of CNF-formed expressions.
   * @return An array of CNF-formed expressions
   */
  public static EvalNode [] toDisjunctiveNormalFormArray(EvalNode...exprs) {
    List<EvalNode> list = new ArrayList<EvalNode>();
    for (EvalNode expr : exprs) {
      toDisjunctiveNormalFormArrayRecursive(expr, list);
    }
    return list.toArray(new EvalNode[list.size()]);
  }

  private static void toDisjunctiveNormalFormArrayRecursive(EvalNode node, List<EvalNode> found) {
    if (node.getType() == EvalType.OR) {
      toDisjunctiveNormalFormArrayRecursive(((BinaryEval)node).getLeftExpr(), found);
      toDisjunctiveNormalFormArrayRecursive(((BinaryEval)node).getRightExpr(), found);
    } else {
      found.add(node);
    }
  }
}
