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

/**
 * 
 */
package org.apache.tajo.engine.planner.global;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.serder.PlanProto.EnforceProperty;
import org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.graph.DirectedGraphVisitor;
import org.apache.tajo.util.graph.SimpleDirectedGraph;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterPlan {
  private final QueryId queryId;
  private final QueryContext context;
  private final LogicalPlan plan;
  private ExecutionBlock root;
  private AtomicInteger nextId = new AtomicInteger(0);

  private ExecutionBlock terminalBlock;
  private Map<ExecutionBlockId, ExecutionBlock> execBlockMap = new HashMap<>();
  private SimpleDirectedGraph<ExecutionBlockId, DataChannel> execBlockGraph =
          new SimpleDirectedGraph<>();

  private Map<ExecutionBlockId, ShuffleContext> shuffleInfo = new HashMap<>();

  /**
   *
   */
  public class ShuffleContext {
    ExecutionBlockId parentEbId;
    int partitionNum;

    public ShuffleContext(ExecutionBlockId parentEbId, int partitionNum) {
      this.parentEbId = parentEbId;
      this.partitionNum = partitionNum;
    }

    public ExecutionBlockId getParentEbId() {
      return parentEbId;
    }

    public int getPartitionNum() {
      return partitionNum;
    }
  }

  /**
   *
   * @param ebId
   * @param partitionNum
   */
  public void addShuffleInfo(ExecutionBlockId ebId, int partitionNum) {
    ExecutionBlockId parentId = getParent(getExecBlock(ebId)).getId();
    shuffleInfo.put(parentId, new ShuffleContext(ebId, partitionNum));
  }

  public Optional<ShuffleContext> getShuffleInfo(ExecutionBlockId ebId) {
    ExecutionBlockId parentId = getParent(getExecBlock(ebId)).getId();
    return shuffleInfo.containsKey(parentId) ? Optional.of(shuffleInfo.get(parentId)) : Optional.empty();
  }

  public ExecutionBlockId newExecutionBlockId() {
    return new ExecutionBlockId(queryId, nextId.incrementAndGet());
  }

  public boolean isTerminal(ExecutionBlock execBlock) {
    return terminalBlock.getId().equals(execBlock.getId());
  }

  public ExecutionBlock getTerminalBlock() {
    return terminalBlock;
  }

  public ExecutionBlock createTerminalBlock() {
    terminalBlock = newExecutionBlock();
    return terminalBlock;
  }

  public MasterPlan(QueryId queryId, QueryContext context, LogicalPlan plan) {
    this.queryId = queryId;
    this.context = context;
    this.plan = plan;
  }

  public QueryId getQueryId() {
    return this.queryId;
  }

  public QueryContext getContext() {
    return this.context;
  }

  public LogicalPlan getLogicalPlan() {
    return this.plan;
  }
  
  public void setTerminal(ExecutionBlock root) {
    this.root = root;
    this.terminalBlock = root;
  }
  
  public ExecutionBlock getRoot() {
    return this.root;
  }

  public ExecutionBlock newExecutionBlock() {
    ExecutionBlock newExecBlock = new ExecutionBlock(newExecutionBlockId());
    execBlockMap.put(newExecBlock.getId(), newExecBlock);
    return newExecBlock;
  }

  public boolean containsExecBlock(ExecutionBlockId execBlockId) {
    return execBlockMap.containsKey(execBlockId);
  }

  public ExecutionBlock getExecBlock(ExecutionBlockId execBlockId) {
    return execBlockMap.get(execBlockId);
  }

  public void removeExecBlock(ExecutionBlockId execBlockId) throws IllegalStateException {
    List<DataChannel> channels = getIncomingChannels(execBlockId);
    if (channels != null && channels.size() > 0) {
      throw new IllegalStateException("Cannot remove execution blocks because some other execution blocks are connected");
    }

    channels = getOutgoingChannels(execBlockId);
    if (channels != null && channels.size() > 0) {
      throw new IllegalStateException("Cannot remove execution blocks because some other execution blocks are connected");
    }

    execBlockMap.remove(execBlockId);
  }

  public void addConnect(DataChannel dataChannel) {
    execBlockGraph.addEdge(dataChannel.getSrcId(), dataChannel.getTargetId(), dataChannel);
  }

  public void addConnect(ExecutionBlock src, ExecutionBlock target, ShuffleType type) {
    addConnect(src.getId(), target.getId(), type);
  }

  public void addConnect(ExecutionBlockId src, ExecutionBlockId target, ShuffleType type) {
    addConnect(new DataChannel(src, target, type));
  }

  public boolean isConnected(ExecutionBlock src, ExecutionBlock target) {
    return isConnected(src.getId(), target.getId());
  }

  public boolean isConnected(ExecutionBlockId src, ExecutionBlockId target) {
    return execBlockGraph.hasEdge(src, target);
  }

  public boolean isReverseConnected(ExecutionBlock target, ExecutionBlock src) {
    return execBlockGraph.hasReversedEdge(target.getId(), src.getId());
  }

  public boolean isReverseConnected(ExecutionBlockId target, ExecutionBlockId src) {
    return execBlockGraph.hasReversedEdge(target, src);
  }

  public DataChannel getChannel(ExecutionBlock src, ExecutionBlock target) {
    return execBlockGraph.getEdge(src.getId(), target.getId());
  }

  public DataChannel getChannel(ExecutionBlockId src, ExecutionBlockId target) {
    return execBlockGraph.getEdge(src, target);
  }

  public List<DataChannel> getOutgoingChannels(ExecutionBlockId src) {
    return execBlockGraph.getOutgoingEdges(src);
  }

  public boolean isRoot(ExecutionBlock execBlock) {
    if (!execBlock.getId().equals(terminalBlock.getId())) {
      return execBlockGraph.getParent(execBlock.getId(), 0).equals(terminalBlock.getId());
    } else {
      return false;
    }
  }

  public boolean isLeaf(ExecutionBlock execBlock) {
    return execBlockGraph.isLeaf(execBlock.getId());
  }

  public boolean isLeaf(ExecutionBlockId id) {
    return execBlockGraph.isLeaf(id);
  }

  public List<DataChannel> getIncomingChannels(ExecutionBlockId target) {
    return execBlockGraph.getIncomingEdges(target);
  }

  public void disconnect(ExecutionBlock src, ExecutionBlock target) {
    disconnect(src.getId(), target.getId());
  }

  public void disconnect(ExecutionBlockId src, ExecutionBlockId target) {
    execBlockGraph.removeEdge(src, target);
  }

  public ExecutionBlock getParent(ExecutionBlock executionBlock) {
    return execBlockMap.get(execBlockGraph.getParent(executionBlock.getId(), 0));
  }

  public List<ExecutionBlock> getChilds(ExecutionBlock execBlock) {
    return getChilds(execBlock.getId());
  }

  public List<ExecutionBlock> getChilds(ExecutionBlockId id) {
    List<ExecutionBlock> childBlocks = new ArrayList<>();
    for (ExecutionBlockId cid : execBlockGraph.getChilds(id)) {
      childBlocks.add(execBlockMap.get(cid));
    }
    return childBlocks;
  }

  public int getChildCount(ExecutionBlockId blockId) {
    return execBlockGraph.getChildCount(blockId);
  }

  public ExecutionBlock getChild(ExecutionBlockId execBlockId, int idx) {
    return execBlockMap.get(execBlockGraph.getChild(execBlockId, idx));
  }

  public ExecutionBlock getChild(ExecutionBlock executionBlock, int idx) {
    return getChild(executionBlock.getId(), idx);
  }

  public <CONTEXT> void accept(CONTEXT context, ExecutionBlockId v, DirectedGraphVisitor<CONTEXT,
      ExecutionBlockId> visitor) throws TajoException {
    execBlockGraph.accept(context, v, visitor);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("-------------------------------------------------------------------------------\n");
    sb.append("Execution Block Graph (TERMINAL - " + getTerminalBlock() + ")\n");
    sb.append("-------------------------------------------------------------------------------\n");
    sb.append(execBlockGraph.toStringGraph(getRoot().getId()));
    sb.append("-------------------------------------------------------------------------------\n");

    ExecutionBlockCursor executionOrderCursor = new ExecutionBlockCursor(this, true);
    sb.append("Order of Execution\n");
    sb.append("-------------------------------------------------------------------------------");
    int order = 1;
    for (ExecutionBlock currentEB : executionOrderCursor) {
      sb.append("\n").append(order).append(": ").append(currentEB.getId());
      order++;
    }
    sb.append("\n-------------------------------------------------------------------------------\n");

    for (ExecutionBlock block : executionOrderCursor) {

      boolean terminal = false;
      sb.append("\n");
      sb.append("=======================================================\n");
      sb.append("Block Id: " + block.getId());
      if (isTerminal(block)) {
        sb.append(" [TERMINAL]");
        terminal = true;
      } else if (isRoot(block)) {
        sb.append(" [ROOT]");
      } else if (isLeaf(block)) {
        sb.append(" [LEAF]");
      } else {
        sb.append(" [INTERMEDIATE]");
      }
      sb.append("\n");
      sb.append("=======================================================\n");
      if (terminal) {
        continue;
      }

      if (!isLeaf(block)) {
        sb.append("\n[Incoming]\n");
        for (DataChannel channel : getIncomingChannels(block.getId())) {
          sb.append(channel);
          if (block.getUnionScanMap().containsKey(channel.getSrcId())) {
            sb.append(", union delegated scan: ").append(block.getUnionScanMap().get(channel.getSrcId()));
          }
          sb.append("\n");
        }
      }

      if (!isRoot(block)) {
        sb.append("\n[Outgoing]\n");
        for (DataChannel channel : getOutgoingChannels(block.getId())) {
          sb.append(channel);
          sb.append("\n");
        }
      }


      if (block.getEnforcer().getProperties().size() > 0) {
        sb.append("\n[Enforcers]\n");
        int i = 0;
        List<EnforceProperty> enforceProperties = block.getEnforcer().getProperties();
        Collections.sort(enforceProperties, (e1, e2) -> e1.toString().compareTo(e2.toString()));
        for (EnforceProperty enforce : enforceProperties) {
          sb.append(" ").append(i++).append(": ");
          sb.append(Enforcer.toString(enforce));
          sb.append("\n");
        }
      }

      sb.append("\n").append(PlannerUtil.buildExplainString(block.getPlan()));
    }

    return sb.toString();
  }
}
