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

package org.apache.tajo.plan.logical;

public class PartitionedTableScanNode extends ScanNode {
  public PartitionedTableScanNode(int pid) {
    super(pid, NodeType.PARTITIONS_SCAN);
  }

  public void init(ScanNode scanNode) {
    tableDesc = scanNode.tableDesc;
    setInSchema(scanNode.getInSchema());
    setOutSchema(scanNode.getOutSchema());
    this.qual = scanNode.qual;
    this.targets = scanNode.targets;

    if (scanNode.hasAlias()) {
      alias = scanNode.alias;
    }
  }

}
