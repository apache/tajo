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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class EvalTreeUtil {

  public static void changeColumnRef(EvalNode node, String oldName, String newName) {
    node.postOrder(new ChangeColumnRefVisitor(oldName, newName));
  }

  public static void replace(EvalNode expr, EvalNode targetExpr, EvalNode tobeReplaced) {
    EvalReplaceVisitor replacer = new EvalReplaceVisitor(targetExpr, tobeReplaced);
    replacer.visitChild(null, expr, new Stack<EvalNode>());
  }

  public static class EvalReplaceVisitor extends BasicEvalNodeVisitor<EvalNode, EvalNode> {
    private EvalNode target;
    private EvalNode tobeReplaced;

    public EvalReplaceVisitor(EvalNode target, EvalNode tobeReplaced) {
      this.target = target;
      this.tobeReplaced = tobeReplaced;
    }

    @Override
    public EvalNode visitChild(EvalNode context, EvalNode evalNode, Stack<EvalNode> stack) {
      super.visitChild(context, evalNode, stack);

      if (evalNode.equals(target)) {
        EvalNode parent = stack.peek();

        if (parent instanceof BetweenPredicateEval) {
          BetweenPredicateEval between = (BetweenPredicateEval) parent;
          if (between.getPredicand().equals(evalNode)) {
            between.setPredicand(tobeReplaced);
          }
          if (between.getBegin().equals(evalNode)) {
            between.setBegin(tobeReplaced);
          }
          if (between.getEnd().equals(evalNode)) {
            between.setEnd(tobeReplaced);
          }

        } else if (parent instanceof CaseWhenEval) {
          CaseWhenEval caseWhen = (CaseWhenEval) parent;

          // Here, we need to only consider only 'Else'
          // because IfElseEval is handled in the below condition.
          if (caseWhen.hasElse() && caseWhen.getElse().equals(evalNode)) {
            caseWhen.setElseResult(tobeReplaced);
          }
        } else if (parent instanceof CaseWhenEval.IfThenEval) {
          CaseWhenEval.IfThenEval ifThen = (CaseWhenEval.IfThenEval) parent;
          if (ifThen.getCondition().equals(evalNode)) {
            ifThen.setCondition(tobeReplaced);
          }
          if (ifThen.getResult().equals(evalNode)) {
            ifThen.setResult(tobeReplaced);
          }
       } else if (parent instanceof FunctionEval) {
          FunctionEval functionEval = (FunctionEval) parent;
          EvalNode [] arguments = functionEval.getArgs();
          for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].equals(evalNode)) {
              arguments[i] = tobeReplaced;
            }
          }
          functionEval.setArgs(arguments);

        } else if (parent instanceof UnaryEval) {
          if (((UnaryEval)parent).getChild().equals(evalNode)) {
            ((UnaryEval)parent).setChild(tobeReplaced);
          }
        } else if (parent instanceof BinaryEval) {
          BinaryEval binary = (BinaryEval) parent;
          if (binary.getLeftExpr() != null && binary.getLeftExpr().equals(evalNode)) {
            binary.setLeftExpr(tobeReplaced);
          }
          if (binary.getRightExpr() != null && binary.getRightExpr().equals(evalNode)) {
            binary.setRightExpr(tobeReplaced);
          }
        }
      }

      return evalNode;
    }
  }

  /**
   * It finds unique columns from a EvalNode.
   */
  public static LinkedHashSet<Column> findUniqueColumns(EvalNode node) {
    UniqueColumnFinder finder = new UniqueColumnFinder();
    node.postOrder(finder);
    return finder.getColumnRefs();
  }
  
  public static List<Column> findAllColumnRefs(EvalNode node) {
    AllColumnRefFinder finder = new AllColumnRefFinder();
    node.postOrder(finder);
    return finder.getColumnRefs();
  }
  
  public static Schema getSchemaByTargets(Schema inputSchema, Target [] targets) 
      throws InternalException {
    Schema schema = new Schema();
    for (Target target : targets) {
      schema.addColumn(
          target.hasAlias() ? target.getAlias() : target.getEvalTree().getName(),
          getDomainByExpr(inputSchema, target.getEvalTree()));
    }
    
    return schema;
  }

  public static String columnsToStr(Collection<Column> columns) {
    StringBuilder sb = new StringBuilder();
    String prefix = "";
    for (Column column: columns) {
      sb.append(prefix).append(column.getQualifiedName());
      prefix = ",";
    }

    return sb.toString();
  }
  
  public static DataType getDomainByExpr(Schema inputSchema, EvalNode expr)
      throws InternalException {
    switch (expr.getType()) {
    case AND:      
    case OR:
    case EQUAL:
    case NOT_EQUAL:
    case LTH:
    case LEQ:
    case GTH:
    case GEQ:
    case PLUS:
    case MINUS:
    case MULTIPLY:
    case DIVIDE:
    case CONST:
    case FUNCTION:
        return expr.getValueType();

    case FIELD:
      FieldEval fieldEval = (FieldEval) expr;
      return inputSchema.getColumn(fieldEval.getName()).getDataType();

      
    default:
      throw new InternalException("Unknown expr type: " 
          + expr.getType().toString());
    }
  }

  /**
   * Return all exprs to refer columns corresponding to the target.
   *
   * @param expr
   * @param target to be found
   * @return a list of exprs
   */
  public static Collection<EvalNode> getContainExpr(EvalNode expr, Column target) {
    Set<EvalNode> exprSet = Sets.newHashSet();
    getContainExpr(expr, target, exprSet);
    return exprSet;
  }
  
  /**
   * Return the counter to count the number of expression types individually.
   *  
   * @param expr
   * @return
   */
  public static Map<EvalType, Integer> getExprCounters(EvalNode expr) {
    VariableCounter counter = new VariableCounter();
    expr.postOrder(counter);
    return counter.getCounter();
  }
  
  private static void getContainExpr(EvalNode expr, Column target, Set<EvalNode> exprSet) {
    switch (expr.getType()) {
    case EQUAL:
    case LTH:
    case LEQ:
    case GTH:
    case GEQ:
    case NOT_EQUAL:
      if (containColumnRef(expr, target)) {          
        exprSet.add(expr);
      }
    }    
  }
  
  /**
   * Examine if the expr contains the column reference corresponding 
   * to the target column
   */
  public static boolean containColumnRef(EvalNode expr, Column target) {
    Set<Column> exprSet = findUniqueColumns(expr);
    return exprSet.contains(target);
  }

  /**
   * If a given expression is join condition, it returns TRUE. Otherwise, it returns FALSE.
   *
   * @param expr EvalNode to be evaluated
   * @param includeThetaJoin If true, it will return equi as well as non-equi join conditions.
   *                         Otherwise, it only returns equi-join conditions.
   * @return True if it is join condition.
   */
  public static boolean isJoinQual(EvalNode expr, boolean includeThetaJoin) {
    if (expr instanceof BinaryEval) {
      boolean joinComparator;
      if (includeThetaJoin) {
        joinComparator = EvalType.isComparisonOperator(expr);
      } else {
        joinComparator = expr.getType() == EvalType.EQUAL;
      }

      BinaryEval binaryEval = (BinaryEval) expr;
      boolean isBothTermFields =
          binaryEval.getLeftExpr().getType() == EvalType.FIELD &&
          binaryEval.getRightExpr().getType() == EvalType.FIELD;
      return joinComparator && isBothTermFields;
    } else {
      return false;
    }
  }
  
  public static class ChangeColumnRefVisitor implements EvalNodeVisitor {    
    private final String findColumn;
    private final String toBeChanged;
    
    public ChangeColumnRefVisitor(String oldName, String newName) {
      this.findColumn = oldName;
      this.toBeChanged = newName;
    }
    
    @Override
    public void visit(EvalNode node) {
      if (node.type == EvalType.FIELD) {
        FieldEval field = (FieldEval) node;
        if (field.getColumnName().equals(findColumn)
            || field.getName().equals(findColumn)) {
          field.replaceColumnRef(toBeChanged);
        }
      }
    }    
  }
  
  public static class AllColumnRefFinder implements EvalNodeVisitor {
    private List<Column> colList = new ArrayList<Column>();
    private FieldEval field = null;
    
    @Override
    public void visit(EvalNode node) {
      if (node.getType() == EvalType.FIELD) {
        field = (FieldEval) node;
        colList.add(field.getColumnRef());
      } 
    }
    
    public List<Column> getColumnRefs() {
      return this.colList;
    }
  }
  
  public static class UniqueColumnFinder implements EvalNodeVisitor {
    private LinkedHashSet<Column> columnSet = Sets.newLinkedHashSet();
    private FieldEval field = null;
    
    @Override
    public void visit(EvalNode node) {
      if (node.getType() == EvalType.FIELD) {
        field = (FieldEval) node;
        columnSet.add(field.getColumnRef());
      }
    }
    
    public LinkedHashSet<Column> getColumnRefs() {
      return this.columnSet;
    }
  }
  
  public static class VariableCounter implements EvalNodeVisitor {
    private final Map<EvalType, Integer> counter;
    
    public VariableCounter() {
      counter = Maps.newHashMap();
      counter.put(EvalType.FUNCTION, 0);
      counter.put(EvalType.FIELD, 0);
    }
    
    @Override
    public void visit(EvalNode node) {
      if (counter.containsKey(node.getType())) {
        int val = counter.get(node.getType());
        val++;
        counter.put(node.getType(), val);
      }
    }
    
    public Map<EvalType, Integer> getCounter() {
      return counter;
    }
  }

  public static Set<AggregationFunctionCallEval> findDistinctAggFunction(EvalNode expr) {
    AllAggFunctionFinder finder = new AllAggFunctionFinder();
    expr.postOrder(finder);
    return finder.getAggregationFunction();
  }

  public static class AllAggFunctionFinder implements EvalNodeVisitor {
    private Set<AggregationFunctionCallEval> aggFucntions = Sets.newHashSet();
    private AggregationFunctionCallEval field = null;

    @Override
    public void visit(EvalNode node) {
      if (node.getType() == EvalType.AGG_FUNCTION) {
        field = (AggregationFunctionCallEval) node;
        aggFucntions.add(field);
      }
    }

    public Set<AggregationFunctionCallEval> getAggregationFunction() {
      return this.aggFucntions;
    }
  }

  public static <T extends EvalNode> Collection<T> findEvalsByType(EvalNode evalNode, EvalType type) {
    EvalFinder finder = new EvalFinder(type);
    finder.visitChild(null, evalNode, new Stack<EvalNode>());
    return (Collection<T>) finder.evalNodes;
  }

  public static <T extends EvalNode> Collection<T> findOuterJoinRelatedEvals(EvalNode evalNode) {
    EvalOuterJoinRelatedFinder finder = new EvalOuterJoinRelatedFinder();
    finder.visitChild(null, evalNode, new Stack<EvalNode>());
    return (Collection<T>) finder.evalNodes;
  }

  public static class EvalFinder extends BasicEvalNodeVisitor<Object, Object> {
    private EvalType targetType;
    List<EvalNode> evalNodes = TUtil.newList();

    public EvalFinder(EvalType targetType) {
      this.targetType = targetType;
    }

    @Override
    public Object visitChild(Object context, EvalNode evalNode, Stack<EvalNode> stack) {
      super.visitChild(context, evalNode, stack);

      if (evalNode.type == targetType) {
        evalNodes.add(evalNode);
      }

      return evalNode;
    }
  }

  public static class EvalOuterJoinRelatedFinder extends BasicEvalNodeVisitor<Object, Object> {
    List<EvalNode> evalNodes = TUtil.newList();

    public EvalOuterJoinRelatedFinder() {
    }

    @Override
    public Object visitChild(Object context, EvalNode evalNode, Stack<EvalNode> stack) {
      super.visitChild(context, evalNode, stack);

      if (evalNode.type == EvalType.CASE) {
        evalNodes.add(evalNode);
      } else if (evalNode.type == EvalType.FUNCTION) {
        FunctionEval functionEval = (FunctionEval)evalNode;
        if ("coalesce".equals(functionEval.getName())) {
          evalNodes.add(evalNode);
        }
      } else if (evalNode.type == EvalType.IS_NULL) {
        evalNodes.add(evalNode);
      }
      return evalNode;
    }
  }

  public static boolean checkIfCanBeConstant(EvalNode evalNode) {
    return findUniqueColumns(evalNode).size() == 0 && findDistinctAggFunction(evalNode).size() == 0;
  }

  public static Datum evaluateImmediately(EvalNode evalNode) {
    return evalNode.eval(null, null);
  }
}