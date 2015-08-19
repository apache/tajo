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

package org.apache.tajo.plan.util;

import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.joinorder.GreedyHeuristicJoinOrderAlgorithm;
import org.apache.tajo.plan.logical.*;

public class PlanVerifierUtil {

//  public static long estimateOutputVolume(LogicalNode plan) {
//    // output volume = output row number * output row width
//    return SchemaUtil.estimateRowByteSizeWithSchema(plan.getOutSchema())
//        * estimateOutputRowNum(PlannerUtil.<JoinNode>findTopNode(plan, NodeType.JOIN));
//  }

  public static double estimateOutputRowNumForJoin(JoinSpec joinSpec, double leftInputRowNum, double rightInputRowNum)
      throws TajoInternalError {

    switch (joinSpec.getType()) {
      case CROSS:
        return leftInputRowNum * rightInputRowNum;
      case INNER:
        return (long) (leftInputRowNum * rightInputRowNum *
            Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, joinSpec.getPredicates().size()));
      case LEFT_OUTER:
        return leftInputRowNum;
      case RIGHT_OUTER:
        return rightInputRowNum;
      case FULL_OUTER:
        return leftInputRowNum < rightInputRowNum ? leftInputRowNum : rightInputRowNum;
      case LEFT_ANTI:
      case LEFT_SEMI:
        return (long) (leftInputRowNum *
            Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, joinSpec.getPredicates().size()));
      case RIGHT_ANTI:
      case RIGHT_SEMI:
        return (long) (rightInputRowNum *
            Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, joinSpec.getPredicates().size()));
    }

    throw new TajoInternalError("Invalide join type: " + joinSpec.getType());
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
}
