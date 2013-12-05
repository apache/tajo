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
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.exception.InvalidQueryException;

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
      if (block.hasSelectionNode() || block.hasJoinNode()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      this.visit(new HashSet<EvalNode>(), plan, block.getRoot());
    }

    return plan;
  }

  @Override
  public LogicalNode visitFilter(Set<EvalNode> cnf, LogicalPlan plan, SelectionNode selNode, Stack<LogicalNode> stack)
      throws PlanningException {
    cnf.addAll(Sets.newHashSet(EvalTreeUtil.getConjNormalForm(selNode.getQual())));

    stack.push(selNode);
    visitChild(cnf, plan, selNode.getChild(), stack);
    stack.pop();

    // remove the selection operator if there is no search condition
    // after selection push.
    if(cnf.size() == 0) {
      LogicalNode node = stack.peek();
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        unary.setChild(selNode.getChild());
      } else {
        throw new InvalidQueryException("Unexpected Logical Query Plan");
      }
    }

    return selNode;
  }

  private boolean isOuterJoin(JoinType joinType) {
    return joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER || joinType==JoinType.FULL_OUTER;
  }

  @Override
  public LogicalNode visitJoin(Set<EvalNode> cnf, LogicalPlan plan, JoinNode joinNode, Stack<LogicalNode> stack)
      throws PlanningException {
    LogicalNode left = joinNode.getRightChild();
    LogicalNode right = joinNode.getLeftChild();

    // here we should stop selection pushdown on the null supplying side(s) of an outer join
    // get the two operands of the join operation as well as the join type
    JoinType joinType = joinNode.getJoinType();
    EvalNode joinQual = joinNode.getJoinQual();
    if (joinQual != null && isOuterJoin(joinType)) {

      // if both are fields
       if (joinQual.getLeftExpr().getType() == EvalType.FIELD && joinQual.getRightExpr().getType() == EvalType.FIELD) {

          String leftTableName = ((FieldEval) joinQual.getLeftExpr()).getTableId();
          String rightTableName = ((FieldEval) joinQual.getRightExpr()).getTableId();
          List<String> nullSuppliers = Lists.newArrayList();
          String [] leftLineage = PlannerUtil.getRelationLineage(joinNode.getLeftChild());
          String [] rightLineage = PlannerUtil.getRelationLineage(joinNode.getRightChild());
          Set<String> leftTableSet = Sets.newHashSet(leftLineage);
          Set<String> rightTableSet = Sets.newHashSet(rightLineage);

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
             nullSuppliers.add(((ScanNode)joinNode.getRightChild()).getTableName()); 
             //verify that this null supplier is indeed in the right sub-tree
             if (!rightTableSet.contains(nullSuppliers.get(0))) {
                 throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
             }
          } else if (joinType == JoinType.RIGHT_OUTER) {
            if (((ScanNode)joinNode.getRightChild()).getTableName().equals(rightTableName)) {
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
            
            Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(eval);
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
             qual2 = EvalTreeUtil.transformCNF2Singleton(
                        matched2.toArray(new EvalNode [matched2.size()]));
          } else if (matched2.size() == 1) {
             // if the number of matched expr is one
             qual2 = matched2.get(0);
          }

          if (qual2 != null) {
             EvalNode conjQual2 = EvalTreeUtil.transformCNF2Singleton(joinNode.getJoinQual(), qual2);
             joinNode.setJoinQual(conjQual2);
             cnf.removeAll(matched2);
          } // for the remaining cnf, push it as usual
       }
    }

    if (joinNode.hasJoinQual()) {
      cnf.addAll(Sets.newHashSet(EvalTreeUtil.getConjNormalForm(joinNode.getJoinQual())));
    }

    visitChild(cnf, plan, left, stack);
    visitChild(cnf, plan, right, stack);

    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (PlannerUtil.canBeEvaluated(eval, joinNode)) {
        matched.add(eval);
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = EvalTreeUtil.transformCNF2Singleton(
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
  public LogicalNode visitScan(Set<EvalNode> cnf, LogicalPlan plan, ScanNode scanNode, Stack<LogicalNode> stack)
      throws PlanningException {

    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (PlannerUtil.canBeEvaluated(eval, scanNode)) {
        matched.add(eval);
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = EvalTreeUtil.transformCNF2Singleton(
          matched.toArray(new EvalNode [matched.size()]));
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
