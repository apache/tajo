package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.*;

import java.util.List;

public class GlobalPlanRewriteUtil {
  public static ExecutionBlock mergeExecutionBlocks(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent) {
    for (String broadcastable : child.getBroadcastTables()) {
      parent.addBroadcastRelation(broadcastable);
    }

    // connect parent and grand children
    List<ExecutionBlock> grandChilds = plan.getChilds(child);
    for (ExecutionBlock eachGrandChild : grandChilds) {
      plan.addConnect(eachGrandChild, parent, plan.getChannel(eachGrandChild, child).getShuffleType());
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

  public static void replaceChild(LogicalNode newChild, ScanNode originalChild, LogicalNode parent)
      throws PlanningException {
    if (parent instanceof UnaryNode) {
      ((UnaryNode) parent).setChild(newChild);
    } else if (parent instanceof BinaryNode) {
      BinaryNode binary = (BinaryNode) parent;
      if (binary.getLeftChild().equals(originalChild)) {
        binary.setLeftChild(newChild);
      } else if (binary.getRightChild().equals(originalChild)) {
        binary.setRightChild(newChild);
      } else {
        throw new PlanningException(originalChild.getPID() + " is not a child of " + parent.getPID());
      }
    } else {
      throw new PlanningException(parent.getPID() + " seems to not have any children");
    }
  }

  public static ScanNode findScanForChildEb(ExecutionBlock child, ExecutionBlock parent) {
    ScanNode scanForChild = null;
    for (ScanNode scanNode : parent.getScanNodes()) {
      if (scanNode.getTableName().equals(child.getId().toString())) {
        scanForChild = scanNode;
        break;
      }
    }
    return scanForChild;
  }

  /**
   * Get a volume of a table of a partitioned table
   * @param scanNode ScanNode corresponding to a table
   * @return table volume (bytes)
   */
  public static long getTableVolume(ScanNode scanNode) {
    long scanBytes = scanNode.getTableDesc().getStats().getNumBytes();
    if (scanNode.getType() == NodeType.PARTITIONS_SCAN) {
      PartitionedTableScanNode pScanNode = (PartitionedTableScanNode)scanNode;
      if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
        scanBytes = 0L;
      }
    }

    return scanBytes;
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
