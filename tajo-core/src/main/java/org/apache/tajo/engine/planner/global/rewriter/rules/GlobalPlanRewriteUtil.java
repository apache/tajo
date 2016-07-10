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

package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.logical.*;

import java.util.List;

public class GlobalPlanRewriteUtil {
  /**
   * Merge the parent EB with the child EB.
   *
   * @param plan
   * @param child
   * @param parent
   * @return
   */
  public static ExecutionBlock mergeExecutionBlocks(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent) {
    child.getBroadcastRelations().forEach(parent::addBroadcastRelation);

    // connect parent and grand children
    List<ExecutionBlock> grandChilds = plan.getChilds(child);
    for (ExecutionBlock eachGrandChild : grandChilds) {
      DataChannel originalChannel = plan.getChannel(eachGrandChild, child);
      DataChannel newChannel = new DataChannel(eachGrandChild, parent, originalChannel.getShuffleType(),
          originalChannel.getShuffleOutputNum());
      newChannel.setSchema(originalChannel.getSchema());
      newChannel.setShuffleKeys(originalChannel.getShuffleKeys());
      newChannel.setDataFormat(originalChannel.getDataFormat());
      newChannel.setTransmitType(originalChannel.getTransmitType());
      plan.addConnect(newChannel);
      plan.disconnect(eachGrandChild, child);
    }

    plan.disconnect(child, parent);
    List<DataChannel> channels = plan.getIncomingChannels(child.getId());
    if (channels == null || channels.size() == 0) {
      channels = plan.getOutgoingChannels(child.getId());
      if (channels == null || channels.size() == 0) {
        plan.removeExecBlock(child.getId());
      }
    }
    return parent;
  }

  /**
   * Replace a child of the given parent logical node with the new one.
   *
   * @param newChild
   * @param originalChild
   * @param parent
   */
  public static void replaceChild(LogicalNode newChild, ScanNode originalChild, LogicalNode parent) {
    if (parent instanceof UnaryNode) {
      ((UnaryNode) parent).setChild(newChild);
    } else if (parent instanceof BinaryNode) {
      BinaryNode binary = (BinaryNode) parent;
      if (binary.getLeftChild().equals(originalChild)) {
        binary.setLeftChild(newChild);
      } else if (binary.getRightChild().equals(originalChild)) {
        binary.setRightChild(newChild);
      } else {
        throw new TajoInternalError(originalChild.getPID() + " is not a child of " + parent.getPID());
      }
    } else {
      throw new TajoInternalError(parent.getPID() + " seems to not have any children");
    }
  }

  /**
   * Get a volume of a table of a partitioned table
   * @param scanNode ScanNode corresponding to a table
   * @return table volume (bytes)
   */
  public static long getTableVolume(ScanNode scanNode) {
    if (scanNode.getTableDesc().hasStats()) {
      long scanBytes = scanNode.getTableDesc().getStats().getNumBytes();
      if (scanNode.getType() == NodeType.PARTITIONS_SCAN) {
        PartitionedTableScanNode pScanNode = (PartitionedTableScanNode) scanNode;
        if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
          scanBytes = 0L;
        }
      }

      return scanBytes;
    } else {
      return -1;
    }
  }

  /**
   * It calculates the total volume of all descendent relation nodes.
   */
  public static long computeDescendentVolume(LogicalNode node) {

    if (node instanceof RelationNode) {
      switch (node.getType()) {
        case SCAN:
          ScanNode scanNode = (ScanNode) node;
          if (scanNode.getTableDesc().getStats() == null) {
            // TODO - this case means that data is not located in HDFS. So, we need additional
            // broadcast method.
            return Long.MAX_VALUE;
          } else {
            return scanNode.getTableDesc().getStats().getNumBytes();
          }
        case PARTITIONS_SCAN:
          PartitionedTableScanNode pScanNode = (PartitionedTableScanNode) node;
          if (pScanNode.getTableDesc().getStats() == null) {
            // TODO - this case means that data is not located in HDFS. So, we need additional
            // broadcast method.
            return Long.MAX_VALUE;
          } else {
            // if there is no selected partition
            if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
              return 0;
            } else {
              return pScanNode.getTableDesc().getStats().getNumBytes();
            }
          }
        case TABLE_SUBQUERY:
          return computeDescendentVolume(((TableSubQueryNode) node).getSubQuery());
        default:
          throw new IllegalArgumentException("Not RelationNode");
      }
    } else if (node instanceof UnaryNode) {
      return computeDescendentVolume(((UnaryNode) node).getChild());
    } else if (node instanceof BinaryNode) {
      BinaryNode binaryNode = (BinaryNode) node;
      return computeDescendentVolume(binaryNode.getLeftChild()) + computeDescendentVolume(binaryNode.getRightChild());
    }

    throw new TajoInternalError("Invalid State at node " + node.getPID());
  }

  public static class ParentFinder implements LogicalNodeVisitor {
    private LogicalNode target;
    private LogicalNode found;

    public void set(LogicalNode child) {
      this.target = child;
      this.found = null;
    }

    public void find(LogicalNode root) {
      this.visit(root);
    }

    public LogicalNode getFound() {
      if (found == null) {
        throw new TajoInternalError("cannot find the parent of " + target.getPID());
      }
      return this.found;
    }

    @Override
    public void visit(LogicalNode node) {
      for (int i = 0; i < node.childNum(); i++) {
        if (node.getChild(i).equals(target)) {
          found = node;
          break;
        } else {
          if (found == null) {
            visit(node.getChild(i));
          }
        }
      }
    }
  }
}
