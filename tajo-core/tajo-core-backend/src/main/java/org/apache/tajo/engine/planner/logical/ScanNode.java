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

package org.apache.tajo.engine.planner.logical;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.json.GsonCreator;
import org.apache.tajo.engine.parser.QueryBlock;
import org.apache.tajo.engine.parser.QueryBlock.FromTable;
import org.apache.tajo.engine.parser.QueryBlock.Target;
import org.apache.tajo.util.TUtil;

public class ScanNode extends LogicalNode {
	@Expose private FromTable table;
	@Expose private EvalNode qual;
	@Expose private QueryBlock.Target[] targets;
	@Expose private boolean local;
  @Expose private boolean broadcast;
	
	public ScanNode() {
		super();
		local = false;
	}
  
	public ScanNode(FromTable table) {
		super(ExprType.SCAN);
		this.table = table;
		this.setInSchema(table.getSchema());
		this.setOutSchema(table.getSchema());
		local = false;
	}
	
	public String getTableId() {
	  return table.getTableName();
	}
	
	public boolean hasAlias() {
	  return table.hasAlias();
	}
	
	public boolean hasQual() {
	  return qual != null;
	}
	
	public EvalNode getQual() {
	  return this.qual;
	}
	
	public boolean isLocal() {
	  return this.local;
	}
	
	public void setLocal(boolean local) {
	  this.local = local;
	}
	
	public void setQual(EvalNode evalTree) {
	  this.qual = evalTree;
	}
	
	public boolean hasTargetList() {
	  return this.targets != null;
	}
	
	public void setTargets(Target [] targets) {
	  this.targets = targets;
	}
	
	public Target [] getTargets() {
	  return this.targets;
	}

  public boolean isBroadcast() {
    return broadcast;
  }

  public void setBroadcast() {
    broadcast = true;
  }
	
	public FromTable getFromTable() {
	  return this.table;
	}
	
	public void setFromTable(FromTable from) {
	  this.table = from;
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder();	  
	  sb.append("\"Scan\" : {\"table\":\"")
	  .append(table.getTableName()).append("\"");
	  if (hasAlias()) {
	    sb.append(",\"alias\": \"").append(table.getAlias());
	  }

    if (isBroadcast()) {
      sb.append(",\"broadcast\": true\"");
    }
	  
	  if (hasQual()) {
	    sb.append(", \"qual\": \"").append(this.qual).append("\"");
	  }
	  
	  if (hasTargetList()) {
	    sb.append(", \"target list\": ");
      boolean first = true;
      for (Target target : targets) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(target);
        first = false;
      }
	  }
	  
	  sb.append(",");
	  sb.append("\n  \"out schema\": ").append(getOutSchema());
	  sb.append("\n  \"in schema\": ").append(getInSchema());
	  return sb.toString();
	}
	
	public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, LogicalNode.class);
	}

  @Override
  public int hashCode() {
    return Objects.hashCode(this.table, this.qual, this.targets);
  }
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof ScanNode) {
	    ScanNode other = (ScanNode) obj;
	    
	    boolean eq = super.equals(other); 
	    eq = eq && TUtil.checkEquals(this.table, other.table);
	    eq = eq && TUtil.checkEquals(this.qual, other.qual);
	    eq = eq && TUtil.checkEquals(this.targets, other.targets);
	    
	    return eq;
	  }	  
	  
	  return false;
	}	
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  ScanNode scanNode = (ScanNode) super.clone();
	  
	  scanNode.table = (FromTable) this.table.clone();
	  
	  if (hasQual()) {
	    scanNode.qual = (EvalNode) this.qual.clone();
	  }
	  
	  if (hasTargetList()) {
	    scanNode.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        scanNode.targets[i] = (Target) targets[i].clone();
      }
	  }
	  
	  return scanNode;
	}
	
  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
	
	public void postOrder(LogicalNodeVisitor visitor) {        
    visitor.visit(this);
  }
}
