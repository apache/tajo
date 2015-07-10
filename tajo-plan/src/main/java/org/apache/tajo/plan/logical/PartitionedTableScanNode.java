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

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.util.TUtil;

public class PartitionedTableScanNode extends ScanNode {
  @Expose Path [] inputPaths;

  public PartitionedTableScanNode(int pid) {
    super(pid, NodeType.PARTITIONS_SCAN);
  }

  public void init(ScanNode scanNode, Path[] inputPaths) {
    tableDesc = scanNode.tableDesc;
    setInSchema(scanNode.getInSchema());
    setOutSchema(scanNode.getOutSchema());
    this.qual = scanNode.qual;
    this.targets = scanNode.targets;
    this.inputPaths = inputPaths;

    if (scanNode.hasAlias()) {
      alias = scanNode.alias;
    }
  }

  public void setInputPaths(Path [] paths) {
    this.inputPaths = paths;
  }

  public Path [] getInputPaths() {
    return inputPaths;
  }
	
	public String toString() {
    StringBuilder sb = new StringBuilder("Partitions Scan (table=").append(getTableName());
    if (hasAlias()) {
      sb.append(", alias=").append(alias);
    }
    if (hasQual()) {
      sb.append(", filter=").append(qual);
    }
    sb.append(", uri=").append(getTableDesc().getUri()).append(")");
	  return sb.toString();
	}

  @Override
  public int hashCode() {
    return Objects.hashCode(this.tableDesc, this.qual, this.targets);
  }
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof PartitionedTableScanNode) {
	    PartitionedTableScanNode other = (PartitionedTableScanNode) obj;
	    
	    boolean eq = super.equals(other); 
	    eq = eq && TUtil.checkEquals(this.tableDesc, other.tableDesc);
	    eq = eq && TUtil.checkEquals(this.qual, other.qual);
	    eq = eq && TUtil.checkEquals(this.targets, other.targets);
      eq = eq && TUtil.checkEquals(this.inputPaths, other.inputPaths);
	    
	    return eq;
	  }	  
	  
	  return false;
	}	
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  PartitionedTableScanNode unionScan = (PartitionedTableScanNode) super.clone();
	  
	  unionScan.tableDesc = (TableDesc) this.tableDesc.clone();
	  
	  if (hasQual()) {
	    unionScan.qual = (EvalNode) this.qual.clone();
	  }
	  
	  if (hasTargets()) {
	    unionScan.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        unionScan.targets[i] = (Target) targets[i].clone();
      }
	  }

    unionScan.inputPaths = inputPaths;

    return unionScan;
	}
	
  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
	
	public void postOrder(LogicalNodeVisitor visitor) {        
    visitor.visit(this);
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this).appendTitle(" on " + getTableName());
    if (hasAlias()) {
      planStr.appendTitle(" as ").appendTitle(alias);
    }

    if (hasQual()) {
      planStr.addExplan("filter: ").appendExplain(this.qual.toString());
    }

    if (hasTargets()) {
      planStr.addExplan("target list: ");
      boolean first = true;
      for (Target target : targets) {
        if (!first) {
          planStr.appendExplain(", ");
        }
        planStr.appendExplain(target.toString());
        first = false;
      }
    }

    planStr.addDetail("out schema: ").appendDetail(getOutSchema().toString());
    planStr.addDetail("in schema: ").appendDetail(getInSchema().toString());

    if (inputPaths != null) {
      planStr.addExplan("num of filtered paths: ").appendExplain(""+ inputPaths.length);
      int i = 0;
      for (Path path : inputPaths) {
        planStr.addDetail((i++) + ": ").appendDetail(path.toString());
      }
    }

    return planStr;
  }
}
