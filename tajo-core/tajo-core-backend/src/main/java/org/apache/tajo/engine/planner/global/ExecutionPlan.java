/*
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

package org.apache.tajo.engine.planner.global;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.global.ExecutionPlanEdge.Tag;
import org.apache.tajo.engine.planner.graph.SimpleDirectedGraph;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.json.GsonObject;

import java.util.*;
import java.util.Map.Entry;

/**
 * ExecutionPlan is a DAG of logical nodes.
 * If there are two input sources, the plan should include Join or Union.
 * The terminalNode is used as the start position of the traversal, because there are multiple output destinations.
 */
public class ExecutionPlan implements GsonObject {
  @Expose private InputContext inputContext;
  @Expose private boolean hasUnionPlan;
  @Expose private boolean hasJoinPlan;
  @Expose private LogicalRootNode terminalNode;
  @Expose private Map<Integer, LogicalNode> vertices = new HashMap<Integer, LogicalNode>();
  @Expose private SimpleDirectedGraph<Integer, ExecutionPlanEdge> graph
      = new SimpleDirectedGraph<Integer, ExecutionPlanEdge>();

  @VisibleForTesting
  public ExecutionPlan() {

  }

  public ExecutionPlan(LogicalRootNode terminalNode) {
    this.terminalNode = PlannerUtil.clone(terminalNode);
  }

  public void setPlan(LogicalNode plan) {
    this.clear();
    this.addPlan(plan);
  }

  private void clear() {
    for (ExecutionPlanEdge edge : graph.getEdgesAll()) {
      graph.removeEdge(edge.getChildId(), edge.getParentId());
    }
    vertices.clear();
    this.inputContext = null;
    this.hasUnionPlan = false;
    this.hasJoinPlan = false;
  }

  public void addPlan(LogicalNode plan) {
    LogicalNode current = PlannerUtil.clone(plan);
    if (current.getType() == NodeType.ROOT) {
      terminalNode = (LogicalRootNode) current;
    } else {
      this.add(current, terminalNode, Tag.SINGLE);
      terminalNode.setChild(current);
    }
    ExecutionPlanBuilder builder = new ExecutionPlanBuilder(this);
    builder.visit(terminalNode);
  }

  public void add(LogicalNode child, LogicalNode parent, Tag tag) {
    vertices.put(child.getPID(), child);
    vertices.put(parent.getPID(), parent);
    graph.addEdge(child.getPID(), parent.getPID(), new ExecutionPlanEdge(child, parent, tag));
  }

  public void setInputContext(InputContext contexts) {
    this.inputContext = contexts;
  }

  public boolean hasJoinPlan() {
    return this.hasJoinPlan;
  }

  public boolean hasUnionPlan() {
    return this.hasUnionPlan;
  }

  public LogicalRootNode getTerminalNode() {
    return terminalNode;
  }

  public InputContext getInputContext() {
    return inputContext;
  }

  public String toString() {
    return graph.toStringGraph(terminalNode.getPID());
  }

  public Tag getTag(LogicalNode child, LogicalNode parent) {
    return graph.getEdge(child.getPID(), parent.getPID()).getTag();
  }

  public LogicalNode getChild(LogicalNode parent, Tag tag) {
    List<ExecutionPlanEdge> incomingEdges = graph.getIncomingEdges(parent.getPID());
    for (ExecutionPlanEdge inEdge : incomingEdges) {
      if (inEdge.getTag() == tag) {
        return vertices.get(inEdge.getChildId());
      }
    }
    return null;
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, ExecutionPlan.class);
  }

  public Schema getOutSchema(int i) {
    return vertices.get(graph.getChild(terminalNode.getPID(), i)).getOutSchema();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ExecutionPlan) {
      ExecutionPlan other = (ExecutionPlan) o;
      boolean eq = this.hasJoinPlan == other.hasJoinPlan;
      eq &= this.hasUnionPlan == other.hasUnionPlan;
      eq &= this.terminalNode.equals(other.terminalNode);
      eq &= this.inputContext.equals(other.inputContext);
      if (!eq) {
        return false;
      }

      ExecutionPlanComparator comparator = new ExecutionPlanComparator(this, other);
      eq &= comparator.compare();
      return eq;
    }
    return false;
  }

  public LogicalNode getRootChild(int pid) {
    for (Integer childId : graph.getChilds(terminalNode.getPID())) {
      if (childId == pid) {
        return vertices.get(childId);
      }
    }
    return null;
  }

  public int getChildCount(LogicalNode node) {
    return graph.getChildCount(node.getPID());
  }

  public LogicalNode getChild(LogicalNode node, int i) {
    return vertices.get(graph.getChild(node.getPID(), i));
  }

  public int getParentCount(LogicalNode node) {
    return graph.getParentCount(node.getPID());
  }

  public LogicalNode getParent(LogicalNode node, int i) {
    return vertices.get(graph.getParent(node.getPID(), i));
  }

  public List<LogicalNode> getChilds(LogicalNode node) {
    List<LogicalNode> childs = new ArrayList<LogicalNode>();
    for (Integer childId : graph.getChilds(node.getPID())) {
      childs.add(vertices.get(childId));
    }
    return childs;
  }

  public void remove(LogicalNode child, LogicalNode parent) {
    this.graph.removeEdge(child.getPID(), parent.getPID());
  }

  private static class LogicalNodeIdAndTag {
    @Expose int id;
    @Expose Tag tag;

    public LogicalNodeIdAndTag(int id, Tag tag) {
      this.id = id;
      this.tag = tag;
    }
  }

  public static class ExecutionPlanJsonHelper implements GsonObject {
    @Expose private final boolean hasJoinPlan;
    @Expose private final boolean hasUnionPlan;
    @Expose private final InputContext inputContext;
    @Expose private final LogicalRootNode terminalNode;
    @Expose Map<Integer, LogicalNode> vertices = new HashMap<Integer, LogicalNode>();
    @Expose Map<Integer, List<LogicalNodeIdAndTag>> adjacentList = new HashMap<Integer, List<LogicalNodeIdAndTag>>();

    public ExecutionPlanJsonHelper(ExecutionPlan plan) {
      this.hasJoinPlan = plan.hasJoinPlan;
      this.hasUnionPlan = plan.hasUnionPlan;
      this.inputContext = plan.getInputContext();
      this.terminalNode = plan.terminalNode;
      this.vertices.putAll(plan.vertices);
      Collection<ExecutionPlanEdge> edges = plan.graph.getEdgesAll();
      int parentId, childId;
      List<LogicalNodeIdAndTag> adjacents;

      // convert the graph to an adjacent list
      for (ExecutionPlanEdge edge : edges) {
        childId = edge.getChildId();
        parentId = edge.getParentId();

        if (adjacentList.containsKey(childId)) {
          adjacents = adjacentList.get(childId);
        } else {
          adjacents = new ArrayList<LogicalNodeIdAndTag>();
          adjacentList.put(childId, adjacents);
        }
        adjacents.add(new LogicalNodeIdAndTag(parentId, edge.getTag()));
      }
    }

    @Override
    public String toJson() {
      return CoreGsonHelper.toJson(this, ExecutionPlanJsonHelper.class);
    }

    public ExecutionPlan toExecutionPlan() {
      ExecutionPlan plan = new ExecutionPlan(this.terminalNode);
      plan.hasJoinPlan = this.hasJoinPlan;
      plan.hasUnionPlan = this.hasUnionPlan;
      plan.setInputContext(this.inputContext);
      plan.vertices.putAll(this.vertices);

      for (Entry<Integer, List<LogicalNodeIdAndTag>> e : this.adjacentList.entrySet()) {
        LogicalNode child = this.vertices.get(e.getKey());
        for (LogicalNodeIdAndTag idAndTag : e.getValue()) {
          plan.add(child, this.vertices.get(idAndTag.id), idAndTag.tag);
        }
      }

      return plan;
    }
  }

  private static class ExecutionPlanComparator {
    ExecutionPlan plan1;
    ExecutionPlan plan2;
    boolean equal = true;

    public ExecutionPlanComparator(ExecutionPlan plan1, ExecutionPlan plan2) {
      this.plan1 = plan1;
      this.plan2 = plan2;
    }

    public boolean compare() {
      Stack<Integer> s1 = new Stack<Integer>();
      Stack<Integer> s2 = new Stack<Integer>();
      s1.push(plan1.terminalNode.getPID());
      s2.push(plan2.terminalNode.getPID());
      return recursiveCompare(s1, s2);
    }

    private boolean recursiveCompare(Stack<Integer> s1, Stack<Integer> s2) {
      Integer l1 = s1.pop();
      Integer l2 = s2.pop();

      if (l1.equals(l2)) {
        if (plan1.graph.getChildCount(l1) == plan2.graph.getChildCount(l2)) {
          if (plan1.graph.getChildCount(l1) > 0
              && plan2.graph.getChildCount(l2) > 0) {
            for (Integer child : plan1.graph.getChilds(l1)) {
              s1.push(child);
            }
            for (Integer child : plan2.graph.getChilds(l2)) {
              s2.push(child);
            }
          } else {
            equal &= true;
            return recursiveCompare(s1, s2);
          }
        } else {
          equal = false;
        }
      } else {
        equal = false;
      }
      return equal;
    }
  }

  private static class ExecutionPlanBuilder implements LogicalNodeVisitor {
    private ExecutionPlan plan;

    public ExecutionPlanBuilder(ExecutionPlan plan) {
      this.plan = plan;
    }

    @Override
    public void visit(LogicalNode current) {
      try {
        Preconditions.checkArgument(current instanceof UnaryNode, "The current node should be an unary node");
        visit(current, Tag.SINGLE);
      } catch (PlanningException e) {
        throw new RuntimeException(e);
      }
    }

    private void visit(LogicalNode current, Tag tag) throws PlanningException {
      if (current instanceof UnaryNode) {
        visitUnary((UnaryNode) current, tag);
      } else if (current instanceof BinaryNode) {
        visitBinary((BinaryNode) current, tag);
      } else if (current instanceof ScanNode) {
        visitScan((ScanNode) current, tag);
      } else if (current instanceof TableSubQueryNode) {
        visitTableSubQuery((TableSubQueryNode) current, tag);
      }
    }

    private void visitScan(ScanNode node, Tag tag) throws PlanningException {
      if (plan.inputContext == null) {
        plan.inputContext = new InputContext();
      }
      plan.inputContext.addScanNode(node);
    }

    private void visitUnary(UnaryNode node, Tag tag) throws PlanningException {
      if (node.getChild() != null) {
        LogicalNode child = PlannerUtil.clone(node.getChild());
        plan.add(child, node, tag);
        node.setChild(null);
        visit(child, tag);
      }
    }

    private void visitBinary(BinaryNode node, Tag tag) throws PlanningException {
      Preconditions.checkArgument(tag == Tag.SINGLE);

      LogicalNode child;
      if (node.getType() == NodeType.JOIN) {
        plan.hasJoinPlan = true;
      } else if (node.getType() == NodeType.UNION) {
        plan.hasUnionPlan = true;
      }
      if (node.getLeftChild() != null) {
        child = PlannerUtil.clone(node.getLeftChild());
        plan.add(child, node, Tag.LEFT);
        node.setLeftChild(null);
        visit(child, Tag.LEFT);
      }
      if (node.getRightChild() != null) {
        child = PlannerUtil.clone(node.getRightChild());
        plan.add(child, node, Tag.RIGHT);
        node.setRightChild(null);
        visit(child, Tag.RIGHT);
      }
    }

    private void visitTableSubQuery(TableSubQueryNode node, Tag tag) throws PlanningException {
      LogicalNode child = PlannerUtil.clone(node.getSubQuery());
      plan.add(child, node, tag);
      visit(child, tag);
    }
  }
}
