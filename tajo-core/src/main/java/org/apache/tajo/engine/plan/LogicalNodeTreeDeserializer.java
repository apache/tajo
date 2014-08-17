/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.engine.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.derby.impl.store.access.sort.Scan;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.plan.proto.PlanProto;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.ScanNode;

import java.util.*;

public class LogicalNodeTreeDeserializer {
  private static final LogicalNodeTreeDeserializer instance;

  static {
    instance = new LogicalNodeTreeDeserializer();
  }

  public static LogicalNode deserilize(PlanProto.LogicalNodeTree tree) {
    Map<Integer, LogicalNode> nodeMap = Maps.newHashMap();

    // sort serialized logical nodes in an ascending order of their sids
    List<PlanProto.LogicalNode> nodeList = Lists.newArrayList(tree.getNodesList());
    Collections.sort(nodeList, new Comparator<PlanProto.LogicalNode>() {
      @Override
      public int compare(PlanProto.LogicalNode o1, PlanProto.LogicalNode o2) {
        return o1.getSid() - o2.getSid();
      }
    });

    LogicalNode current = null;

    // The sorted order is the same of a postfix traverse order.
    // So, it sequentially transforms each serialized node into a LogicalNode instance in a postfix order of
    // the original logical node tree.

    Iterator<PlanProto.LogicalNode> it = nodeList.iterator();
    while (it.hasNext()) {
      PlanProto.LogicalNode protoNode = it.next();

      NodeType type = convertType(protoNode.getType());

      switch (type) {

      case SCAN:
        ScanNode scan = new ScanNode(protoNode.getPid());

        PlanProto.ScanNode scanProto = protoNode.getScan();
        if (scanProto.hasAlias()) {
          scan.init(new TableDesc(scanProto.getTable()), scanProto.getAlias());
        } else {
          scan.init(new TableDesc(scanProto.getTable()));
        }

        if (scanProto.getTargetsCount() > 0) {
          Target[] targets = new Target[scanProto.getTargetsCount()];
          for (int i = 0; i < scanProto.getTargetsCount(); i++) {
            targets[i] = convertTarget(scanProto.getTargets(i));
          }
        }

      default:
        throw new RuntimeException("Unknown NodeType: " + type.name());
      }
    }

    return current;
  }

  public static NodeType convertType(PlanProto.NodeType type) {
    return NodeType.valueOf(type.name());
  }

  public static Target convertTarget(PlanProto.Target targetProto) {
    return new Target(EvalTreeProtoDeserializer.deserialize(targetProto.getExpr()), targetProto.getAlias());
  }
}
