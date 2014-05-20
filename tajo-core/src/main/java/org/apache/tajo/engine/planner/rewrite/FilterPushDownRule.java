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

package org.apache.tajo.engine.planner.rewrite;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.exception.InvalidQueryException;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class FilterPushDownRule extends BasicLogicalPlanVisitor<Set<EvalNode>, LogicalNode> implements RewriteRule {
  private static final String NAME = "FilterPushDown";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      if (block.hasNode(NodeType.SELECTION) || block.hasNode(NodeType.JOIN)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      this.visit(new HashSet<EvalNode>(), plan, block, block.getRoot(), new Stack<LogicalNode>());
    }

    return plan;
  }

  @Override
  public LogicalNode visitFilter(Set<EvalNode> cnf, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode selNode, Stack<LogicalNode> stack) throws PlanningException {
    cnf.addAll(Sets.newHashSet(AlgebraicUtil.toConjunctiveNormalFormArray(selNode.getQual())));

    stack.push(selNode);
    visit(cnf, plan, block, selNode.getChild(), stack);
    stack.pop();

    if(cnf.size() == 0) { // remove the selection operator if there is no search condition after selection push.
      LogicalNode node = stack.peek();
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        unary.setChild(selNode.getChild());
      } else {
        throw new InvalidQueryException("Unexpected Logical Query Plan");
      }
    } else { // if there remain search conditions

      // check if it can be evaluated here
      Set<EvalNode> matched = TUtil.newHashSet();
      for (EvalNode eachEval : cnf) {
        if (LogicalPlanner.checkIfBeEvaluatedAtThis(eachEval, selNode)) {
          matched.add(eachEval);
        }
      }

      // if there are search conditions which can be evaluated here, push down them and remove them from cnf.
      if (matched.size() > 0) {
        selNode.setQual(AlgebraicUtil.createSingletonExprFromCNF(matched.toArray(new EvalNode[matched.size()])));
        cnf.removeAll(matched);
      }
    }

    return selNode;
  }

  private boolean isOuterJoin(JoinType joinType) {
    return joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER || joinType==JoinType.FULL_OUTER;
  }

  @Override
  public LogicalNode visitJoin(Set<EvalNode> cnf, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode joinNode,
                               Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode left = joinNode.getRightChild();
    LogicalNode right = joinNode.getLeftChild();

    // here we should stop selection pushdown on the null supplying side(s) of an outer join
    // get the two operands of the join operation as well as the join type
    JoinType joinType = joinNode.getJoinType();
    EvalNode joinQual = joinNode.getJoinQual();
    if (joinQual != null && isOuterJoin(joinType)) {
      BinaryEval binaryEval = (BinaryEval) joinQual;
      // if both are fields
      if (binaryEval.getLeftExpr().getType() == EvalType.FIELD &&
          binaryEval.getRightExpr().getType() == EvalType.FIELD) {

        String leftTableName = ((FieldEval) binaryEval.getLeftExpr()).getQualifier();
        String rightTableName = ((FieldEval) binaryEval.getRightExpr()).getQualifier();
        List<String> nullSuppliers = Lists.newArrayList();
        Set<String> leftTableSet = Sets.newHashSet(PlannerUtil.getRelationLineageWithinQueryBlock(plan,
            joinNode.getLeftChild()));
        Set<String> rightTableSet = Sets.newHashSet(PlannerUtil.getRelationLineageWithinQueryBlock(plan,
            joinNode.getRightChild()));

        // some verification
        if (joinType == JoinType.FULL_OUTER) {
          nullSuppliers.add(leftTableName);
          nullSuppliers.add(rightTableName);

          // verify that these null suppliers are indeed in the left and right sets
          if (!rightTableSet.contains(nullSuppliers.get(0)) && !leftTableSet.contains(nullSuppliers.get(0))) {
            throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
          }
          if (!rightTableSet.contains(nullSuppliers.get(1)) && !leftTableSet.contains(nullSuppliers.get(1))) {
            throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
          }

        } else if (joinType == JoinType.LEFT_OUTER) {
          nullSuppliers.add(((RelationNode)joinNode.getRightChild()).getCanonicalName());
          //verify that this null supplier is indeed in the right sub-tree
          if (!rightTableSet.contains(nullSuppliers.get(0))) {
            throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
          }
        } else if (joinType == JoinType.RIGHT_OUTER) {
          if (((RelationNode)joinNode.getRightChild()).getCanonicalName().equals(rightTableName)) {
            nullSuppliers.add(leftTableName);
          } else {
            nullSuppliers.add(rightTableName);
          }

          // verify that this null supplier is indeed in the left sub-tree
          if (!leftTableSet.contains(nullSuppliers.get(0))) {
            throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
          }
        }

        // retain in this outer join node's JoinQual those selection predicates
        // related to the outer join's null supplier(s)
        List<EvalNode> matched2 = Lists.newArrayList();
        for (EvalNode eval : cnf) {

          Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(eval);
          Set<String> tableNames = Sets.newHashSet();
          // getting distinct table references
          for (Column col : columnRefs) {
            if (!tableNames.contains(col.getQualifier())) {
              tableNames.add(col.getQualifier());
            }
          }

          //if the predicate involves any of the null suppliers
          boolean shouldKeep=false;
          Iterator<String> it2 = nullSuppliers.iterator();
          while(it2.hasNext()){
            if(tableNames.contains(it2.next()) == true) {
              shouldKeep = true;
            }
          }

          if(shouldKeep == true) {
            matched2.add(eval);
          }

        }

        //merge the retained predicates and establish them in the current outer join node. Then remove them from the cnf
        EvalNode qual2 = null;
        if (matched2.size() > 1) {
          // merged into one eval tree
          qual2 = AlgebraicUtil.createSingletonExprFromCNF(
              matched2.toArray(new EvalNode[matched2.size()]));
        } else if (matched2.size() == 1) {
          // if the number of matched expr is one
          qual2 = matched2.get(0);
        }

        if (qual2 != null) {
          EvalNode conjQual2 = AlgebraicUtil.createSingletonExprFromCNF(joinNode.getJoinQual(), qual2);
          joinNode.setJoinQual(conjQual2);
          cnf.removeAll(matched2);
        } // for the remaining cnf, push it as usual
      }
    }

    if (joinNode.hasJoinQual()) {
      cnf.addAll(Sets.newHashSet(AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual())));
    }

    visit(cnf, plan, block, left, stack);
    visit(cnf, plan, block, right, stack);

    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (LogicalPlanner.checkIfBeEvaluatedAtJoin(block, eval, joinNode, stack.peek().getType() != NodeType.JOIN)) {
        matched.add(eval);
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = AlgebraicUtil.createSingletonExprFromCNF(
          matched.toArray(new EvalNode[matched.size()]));
    } else if (matched.size() == 1) {
      // if the number of matched expr is one
      qual = matched.get(0);
    }

    if (qual != null) {
      joinNode.setJoinQual(qual);

      if (joinNode.getJoinType() == JoinType.CROSS) {
        joinNode.setJoinType(JoinType.INNER);
      }
      cnf.removeAll(matched);
    }

    return joinNode;
  }

  @Override
  public LogicalNode visitTableSubQuery(Set<EvalNode> cnf, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                        TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, eval, node)) {
        matched.add(eval);
      }
    }

    Map<String, String> columnMap = new HashMap<String, String>();
    for (int i = 0; i < node.getInSchema().size(); i++) {
      LogicalNode childNode = node.getSubQuery();
      if (childNode.getOutSchema().getColumn(i).hasQualifier()) {
      columnMap.put(node.getInSchema().getColumn(i).getQualifiedName(),
          childNode.getOutSchema().getColumn(i).getQualifiedName());
      } else {
        NamedExprsManager namedExprsMgr = plan.getBlock(node.getSubQuery()).getNamedExprsManager();
        String originalName = namedExprsMgr.getOriginalName(childNode.getOutSchema().getColumn(i)
            .getQualifiedName());

        // We need to consider aliased columns of sub-query.
        // Because we can't get original column name for a special occasion.
        // For example, if we use an aliased name inside a sub-query and then we use it to where
        // condition outside the sub-query, we can't find its original name.
        if (originalName != null) {
          columnMap.put(node.getInSchema().getColumn(i).getQualifiedName(), originalName);
        } else {
          columnMap.put(node.getInSchema().getColumn(i).getQualifiedName(),
            node.getInSchema().getColumn(i).getQualifiedName());
        }
      }
    }

    Set<EvalNode> transformed = new HashSet<EvalNode>();

    // Rename from upper block's one to lower block's one
    for (EvalNode matchedEval : matched) {
      EvalNode copy;
      try {
        copy = (EvalNode) matchedEval.clone();
      } catch (CloneNotSupportedException e) {
        throw new PlanningException(e);
      }

      Set<Column> columns = EvalTreeUtil.findUniqueColumns(copy);
      for (Column c : columns) {
        if (columnMap.containsKey(c.getQualifiedName())) {
          EvalTreeUtil.changeColumnRef(copy, c.getQualifiedName(), columnMap.get(c.getQualifiedName()));
        } else {
          throw new PlanningException(
              "Invalid Filter PushDown on SubQuery: No such a corresponding column '"
                  + c.getQualifiedName());
        }
      }

      transformed.add(copy);
    }

    visit(transformed, plan, plan.getBlock(node.getSubQuery()));

    cnf.removeAll(matched);

    return node;
  }

  @Override
  public LogicalNode visitScan(Set<EvalNode> cnf, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode scanNode,
                               Stack<LogicalNode> stack) throws PlanningException {
    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, eval, scanNode)) {
        matched.add(eval);
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = AlgebraicUtil.createSingletonExprFromCNF(
          matched.toArray(new EvalNode[matched.size()]));
    } else if (matched.size() == 1) {
      // if the number of matched expr is one
      qual = matched.get(0);
    }

    if (qual != null) { // if a matched qual exists
      scanNode.setQual(qual);
    }

    cnf.removeAll(matched);

    return scanNode;
  }
}
