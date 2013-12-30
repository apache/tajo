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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.LogicalPlan.PIDFactory;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.global.ExecutionPlanEdge.EdgeType;
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
  @Expose private PIDFactory pidFactory;
  @Expose private InputContext inputContext;
  @Expose private boolean hasUnionPlan;
  @Expose private boolean hasJoinPlan;
  @Expose private LogicalRootNode terminalNode;
  private Map<Integer, LogicalNode> vertices = new HashMap<Integer, LogicalNode>();
  private SimpleDirectedGraph<Integer, ExecutionPlanEdge> graph
      = new SimpleDirectedGraph<Integer, ExecutionPlanEdge>();

  @Expose private NavigableMap<Integer, PlanGroup> planGroups = Maps.newTreeMap();
  private ExecutionPlanBuilder builder = new ExecutionPlanBuilder(this);
  private final ExecutionPlanComparator comparator = new ExecutionPlanComparator();

  public static class PlanGroup {
    @Expose private int rootPID;
    @Expose private List<LogicalNode> nodes = Lists.newArrayList();  // order: root -> leaf

    public PlanGroup(int rootPID) {
      setId(rootPID);
    }

    public void setId(int rootPID) {
      this.rootPID = rootPID;
    }

    public int getId() {
      return rootPID;
    }

    public void addNodeAndDescendants(LogicalNode logicalNode) {
      add(logicalNode);
      if (logicalNode instanceof UnaryNode) {
        addNodeAndDescendants(((UnaryNode) logicalNode).getChild());
      } else if (logicalNode instanceof BinaryNode) {
        addNodeAndDescendants(((BinaryNode) logicalNode).getLeftChild());
        addNodeAndDescendants(((BinaryNode) logicalNode).getRightChild());
      } else if (logicalNode instanceof TableSubQueryNode) {
        addNodeAndDescendants(((TableSubQueryNode) logicalNode).getSubQuery());
      }
    }

    public void add(LogicalNode logicalNode) {
      nodes.add(logicalNode);
    }

    public LogicalNode toLinkedLogicalNode() {
      LogicalNode[] nodes = this.nodes.toArray(new LogicalNode[this.nodes.size()]);

      for (int i = 0; i < nodes.length;) {
        if (nodes[i] instanceof UnaryNode) {
          ((UnaryNode)nodes[i]).setChild(nodes[++i]);
        } else if (nodes[i] instanceof BinaryNode) {
          ((BinaryNode)nodes[i]).setLeftChild(nodes[i+1]);
          ((BinaryNode)nodes[i]).setRightChild(nodes[i+2]);
          i += 2;
        } else if (nodes[i] instanceof TableSubQueryNode) {
          ((TableSubQueryNode)nodes[i]).setSubQuery(nodes[++i]);
        } else {
          i++;
        }
      }
      return nodes[0];
    }

    public LogicalNode getRootNode() {
      return nodes.get(0);
    }

    public LogicalNode getLeafNode() {
      return nodes.get(nodes.size()-1);
    }

    public void clear() {
      nodes.clear();
    }
  }

  private ExecutionPlan(PIDFactory pidFactory, LogicalRootNode terminalNode) {
    this.pidFactory = pidFactory;
    this.terminalNode = terminalNode;
  }

  public ExecutionPlan(PIDFactory pidFactory) {
    this(pidFactory, new LogicalRootNode(pidFactory.newPID()));
  }

  public void setPlan(LogicalNode plan) {
    this.clear();
    this.addPlan(plan);
  }

  public void clear() {
    for (ExecutionPlanEdge edge : graph.getEdgesAll()) {
      graph.removeEdge(edge.getChildId(), edge.getParentId());
    }
    planGroups.clear();
    vertices.clear();
    this.inputContext = null;
    this.hasUnionPlan = false;
    this.hasJoinPlan = false;
  }

  public void addPlan(LogicalNode plan) {
    LogicalNode topNode = plan;
    if (topNode.getType() == NodeType.ROOT) {
      topNode = ((LogicalRootNode)topNode).getChild();
    }

    SameNodeChecker sameNodeChecker = new SameNodeChecker();
    sameNodeChecker.visit(topNode);
    LogicalNode sameNode = getSameNode(topNode);
    if (sameNode != null) {
      connectChild(topNode, sameNode);
      topNode = sameNode;
    }

    // add group
    PlanGroup planGroup = new PlanGroup(topNode.getPID());
    planGroup.addNodeAndDescendants(topNode);
    planGroups.put(planGroup.rootPID, planGroup);

    topNode = planGroup.getRootNode();
    builder.visit(topNode);
    this.add(topNode, terminalNode, EdgeType.SINGLE);
  }

  private LogicalNode getSameNode(LogicalNode node) {
    for (Entry<Integer, LogicalNode> eachEntry : vertices.entrySet()) {
      if (node.equals(eachEntry.getValue())) {
        return eachEntry.getValue();
      }
    }
    return null;
  }

  private class SameNodeChecker implements LogicalNodeVisitor {

    @Override
    public void visit(LogicalNode node) {
      LogicalNode sameNode;
      if (node instanceof UnaryNode) {
        UnaryNode unaryNode = (UnaryNode) node;
        sameNode = getSameNode(unaryNode.getChild());
        if (sameNode != null) {
          connectChild(unaryNode.getChild(), sameNode);
          unaryNode.setChild(sameNode);
        }
      } else if (node instanceof BinaryNode) {
        BinaryNode binaryNode = (BinaryNode) node;
        sameNode = getSameNode(binaryNode.getLeftChild());
        if (sameNode != null) {
          connectChild(binaryNode.getLeftChild(), sameNode);
          binaryNode.setLeftChild(sameNode);
        }
        sameNode = getSameNode(binaryNode.getRightChild());
        if (sameNode != null) {
          connectChild(binaryNode.getRightChild(), sameNode);
          binaryNode.setRightChild(sameNode);
        }
      } else if (node instanceof TableSubQueryNode) {
        TableSubQueryNode tableSubQueryNode = (TableSubQueryNode) node;
        sameNode = getSameNode(tableSubQueryNode.getSubQuery());
        if (sameNode != null) {
          connectChild(tableSubQueryNode.getSubQuery(), sameNode);
          tableSubQueryNode.setSubQuery(sameNode);
        }
      }
    }
  }

  private void connectChild(LogicalNode originNode, LogicalNode replaceNode) {
    if (replaceNode instanceof UnaryNode) {
      ((UnaryNode) replaceNode).setChild(((UnaryNode)originNode).getChild());
    } else if (replaceNode instanceof BinaryNode) {
      ((BinaryNode) replaceNode).setLeftChild(((BinaryNode) originNode).getLeftChild());
      ((BinaryNode) replaceNode).setRightChild(((BinaryNode) originNode).getRightChild());
    } else if (replaceNode instanceof TableSubQueryNode) {
      ((TableSubQueryNode) replaceNode).setSubQuery(((TableSubQueryNode) originNode).getSubQuery());
    }
  }

  public PlanGroup remoteLogicalNodeGroup(int pid) {
    // TODO: improve the code
    PlanGroup removed = planGroups.remove(pid);
    Collection<PlanGroup> remain = planGroups.values();
    clear();
    for (PlanGroup planGroup : remain) {
      addPlan(planGroup.toLinkedLogicalNode());
    }
    return removed;
  }

  public PlanGroup getPlanGroupWithPID(int pid) {
    return planGroups.get(pid);
  }

  public PlanGroup getFirstPlanGroup() {
    return planGroups.firstEntry().getValue();
  }

  public boolean hasPlanGroup() {
    return planGroups.size() > 0;
  }

  public void add(LogicalNode child, LogicalNode parent, EdgeType edgeType) {
    vertices.put(child.getPID(), child);
    vertices.put(parent.getPID(), parent);
    graph.addEdge(child.getPID(), parent.getPID(), new ExecutionPlanEdge(child, parent, edgeType));
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

  public EdgeType getEdgeType(LogicalNode child, LogicalNode parent) {
    return graph.getEdge(child.getPID(), parent.getPID()).getEdgeType();
  }

  public LogicalNode getChild(LogicalNode parent, EdgeType edgeType) {
    List<ExecutionPlanEdge> incomingEdges = graph.getIncomingEdges(parent.getPID());
    for (ExecutionPlanEdge inEdge : incomingEdges) {
      if (inEdge.getEdgeType() == edgeType) {
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

  public Schema getOutSchemaWithPID(int pid) {
    return planGroups.get(pid).getRootNode().getOutSchema();
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

      eq &= comparator.compare(this, other);
      return eq;
    }
    return false;
  }

  public LogicalNode getTopNode(int index) {
    return vertices.get(getTopNodePid(index));
  }

  public int getTopNodePid(int index) {
    return graph.getChild(terminalNode.getPID(), index);
  }

  public LogicalNode getTopNodeFromPID(int pid) {
    if (planGroups.containsKey(pid)) {
      return planGroups.get(pid).getRootNode();
    } else {
      return null;
    }
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

  private static class PIDAndEdgeType {
    @Expose int id;
    @Expose EdgeType edgeType;

    public PIDAndEdgeType(int id, EdgeType edgeType) {
      this.id = id;
      this.edgeType = edgeType;
    }
  }

  public static class ExecutionPlanJsonHelper implements GsonObject {
    @Expose private final PIDFactory pidFactory;
    @Expose private final boolean hasJoinPlan;
    @Expose private final boolean hasUnionPlan;
    @Expose private final InputContext inputContext;
    @Expose private final LogicalRootNode terminalNode;
    @Expose private final Map<Integer, PlanGroup> planGroups;

    public ExecutionPlanJsonHelper(ExecutionPlan plan) {
      this.pidFactory = plan.pidFactory;
      this.hasJoinPlan = plan.hasJoinPlan;
      this.hasUnionPlan = plan.hasUnionPlan;
      this.inputContext = plan.inputContext;
      this.terminalNode = plan.terminalNode;
      this.planGroups = plan.planGroups;
    }

    @Override
    public String toJson() {
      return CoreGsonHelper.toJson(this, ExecutionPlanJsonHelper.class);
    }

    public ExecutionPlan toExecutionPlan() {
      ExecutionPlan plan = new ExecutionPlan(this.pidFactory, this.terminalNode);
      plan.hasJoinPlan = this.hasJoinPlan;
      plan.hasUnionPlan = this.hasUnionPlan;
      plan.setInputContext(this.inputContext);
      for (PlanGroup planGroup : planGroups.values()) {
        plan.addPlan(planGroup.toLinkedLogicalNode());
      }

      return plan;
    }
  }

  private static class ExecutionPlanComparator {
    boolean equal = true;

    private boolean recursiveCompare(ExecutionPlan plan1, ExecutionPlan plan2,
                                     Stack<Integer> s1, Stack<Integer> s2) {
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
            return recursiveCompare(plan1, plan2, s1, s2);
          } else {
            equal &= true;
          }
        } else {
          equal = false;
        }
      } else {
        equal = false;
      }
      return equal;
    }

    public boolean compare(ExecutionPlan plan1, ExecutionPlan plan2) {
      if (plan1.getChildCount(plan1.terminalNode)
          == plan2.getChildCount(plan2.terminalNode)) {
        Stack<Integer> s1 = new Stack<Integer>();
        Stack<Integer> s2 = new Stack<Integer>();
        int childCount = plan1.getChildCount(plan1.terminalNode);
        for (int i = 0; i < childCount; i++) {
          s1.push(plan1.getTopNode(i).getPID());
          s2.push(plan2.getTopNode(i).getPID());
        }
        return recursiveCompare(plan1, plan2, s1, s2);
      } else {
        return false;
      }
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
        visit(current, EdgeType.SINGLE);
      } catch (PlanningException e) {
        throw new RuntimeException(e);
      }
    }

    private void visit(LogicalNode current, EdgeType edgeType) throws PlanningException {
      if (current instanceof UnaryNode) {
        visitUnary((UnaryNode) current, edgeType);
      } else if (current instanceof BinaryNode) {
        visitBinary((BinaryNode) current, edgeType);
      } else if (current instanceof ScanNode) {
        visitScan((ScanNode) current, edgeType);
      } else if (current instanceof TableSubQueryNode) {
        visitTableSubQuery((TableSubQueryNode) current, edgeType);
      }
    }

    private void visitScan(ScanNode node, EdgeType edgeType) throws PlanningException {
      if (plan.inputContext == null) {
        plan.inputContext = new InputContext();
      }
      plan.inputContext.addScanNode(node);
    }

    private void visitUnary(UnaryNode node, EdgeType edgeType) throws PlanningException {
      if (node.getChild() != null) {
        LogicalNode child = node.getChild();
        plan.add(child, node, edgeType);
        node.setChild(null);
        visit(child, edgeType);
      }
    }

    private void visitBinary(BinaryNode node, EdgeType edgeType) throws PlanningException {
      Preconditions.checkArgument(edgeType == EdgeType.SINGLE);

      LogicalNode child;
      if (node.getType() == NodeType.JOIN) {
        plan.hasJoinPlan = true;
      } else if (node.getType() == NodeType.UNION) {
        plan.hasUnionPlan = true;
      }
      if (node.getLeftChild() != null) {
        child = node.getLeftChild();
        plan.add(child, node, EdgeType.LEFT);
        node.setLeftChild(null);
        visit(child, EdgeType.LEFT);
      }
      if (node.getRightChild() != null) {
        child = node.getRightChild();
        plan.add(child, node, EdgeType.RIGHT);
        node.setRightChild(null);
        visit(child, EdgeType.RIGHT);
      }
    }

    private void visitTableSubQuery(TableSubQueryNode node, EdgeType edgeType) throws PlanningException {
      LogicalNode child = node.getSubQuery();
      plan.add(child, node, edgeType);
      node.nullifySubQuery();
      visit(child, edgeType);
    }
  }
}
