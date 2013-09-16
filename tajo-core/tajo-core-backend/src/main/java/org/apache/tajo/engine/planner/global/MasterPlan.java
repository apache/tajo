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

import org.apache.tajo.DataChannel;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.graph.SimpleDirectedGraph;
import org.apache.tajo.master.ExecutionBlock;
import org.apache.tajo.master.ExecutionBlockCursor;
import org.apache.tajo.master.QueryContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ipc.TajoWorkerProtocol.PartitionType;

public class MasterPlan {
  private final QueryId queryId;
  private final QueryContext context;
  private final LogicalPlan plan;
  private ExecutionBlock root;
  private AtomicInteger nextId = new AtomicInteger(0);

  private ExecutionBlock terminalBlock;
  private Map<ExecutionBlockId, ExecutionBlock> execBlockMap = new HashMap<ExecutionBlockId, ExecutionBlock>();
  private SimpleDirectedGraph<ExecutionBlockId, DataChannel> execBlockGraph =
      new SimpleDirectedGraph<ExecutionBlockId, DataChannel>();

  public ExecutionBlockId newExecutionBlockId() {
    return new ExecutionBlockId(queryId, nextId.incrementAndGet());
  }

  public boolean isTerminal(ExecutionBlock execBlock) {
    return terminalBlock == execBlock;
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

  public void addConnect(DataChannel dataChannel) {
    execBlockGraph.connect(dataChannel.getSrcId(), dataChannel.getTargetId(), dataChannel);
  }

  public void addConnect(ExecutionBlock src, ExecutionBlock target, PartitionType type) {
    addConnect(src.getId(), target.getId(), type);
  }

  public void addConnect(ExecutionBlockId src, ExecutionBlockId target, PartitionType type) {
    addConnect(new DataChannel(src, target, type));
  }

  public boolean isConnected(ExecutionBlock src, ExecutionBlock target) {
    return isConnected(src.getId(), target.getId());
  }

  public boolean isConnected(ExecutionBlockId src, ExecutionBlockId target) {
    return execBlockGraph.isConnected(src, target);
  }

  public boolean isReverseConnected(ExecutionBlock target, ExecutionBlock src) {
    return execBlockGraph.isReversedConnected(target.getId(), src.getId());
  }

  public boolean isReverseConnected(ExecutionBlockId target, ExecutionBlockId src) {
    return execBlockGraph.isReversedConnected(target, src);
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
    return execBlockGraph.isRoot(execBlock.getId());
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
    execBlockGraph.disconnect(src, target);
  }

  public List<ExecutionBlock> getChilds(ExecutionBlock execBlock) {
    return getChilds(execBlock.getId());
  }

  public List<ExecutionBlock> getChilds(ExecutionBlockId id) {
    List<ExecutionBlock> childBlocks = new ArrayList<ExecutionBlock>();
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    ExecutionBlockCursor cursor = new ExecutionBlockCursor(this);
    sb.append("-------------------------------------------------------------------------------\n");
    sb.append("Execution Block Graph (TERMINAL - " + getTerminalBlock() + ")\n");
    sb.append("-------------------------------------------------------------------------------\n");
    sb.append(execBlockGraph.toStringGraph(getRoot().getId()));
    sb.append("-------------------------------------------------------------------------------\n");

    while(cursor.hasNext()) {
      ExecutionBlock block = cursor.nextBlock();

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
          sb.append(channel).append("\n");
        }
      }
      sb.append("\n[Outgoing]\n");
      if (!isRoot(block)) {
        for (DataChannel channel : getOutgoingChannels(block.getId())) {
          sb.append(channel);
          sb.append("\n");
        }
      }
      sb.append("\n").append(block.getPlan());
    }

    return sb.toString();
  }

}
