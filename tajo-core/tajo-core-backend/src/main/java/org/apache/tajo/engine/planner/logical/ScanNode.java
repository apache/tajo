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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.FromTable;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.util.TUtil;

public class ScanNode extends RelationNode implements Projectable {
	@Expose private FromTable table;
	@Expose private EvalNode qual;
	@Expose private Target[] targets;
	
	public ScanNode() {
		super(NodeType.SCAN);
	}
  
	public ScanNode(FromTable table) {
		super(NodeType.SCAN);
		this.table = table;
		this.setInSchema(table.getSchema());
		this.setOutSchema(table.getSchema());
	}
	
	public String getTableName() {
	  return table.getTableName();
	}
	
	public boolean hasAlias() {
	  return table.hasAlias();
	}

  public String getCanonicalName() {
    return table.hasAlias() ? table.getAlias() : table.getTableName();
  }

  public Schema getTableSchema() {
    return table.getSchema();
  }
	
	public boolean hasQual() {
	  return qual != null;
	}
	
	public EvalNode getQual() {
	  return this.qual;
	}
	
	public void setQual(EvalNode evalTree) {
	  this.qual = evalTree;
	}

  @Override
	public boolean hasTargets() {
	  return this.targets != null;
	}

  @Override
	public void setTargets(Target [] targets) {
	  this.targets = targets;
	}

  @Override
	public Target [] getTargets() {
	  return this.targets;
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
	  
	  if (hasQual()) {
	    sb.append(", \"qual\": \"").append(this.qual).append("\"");
	  }
	  
	  if (hasTargets()) {
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
	  
	  if (hasTargets()) {
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

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString("Scan on ").appendTitle(getTableName());
    if (table.hasAlias()) {
      planStr.appendTitle(" as ").appendTitle(table.getAlias());
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

    return planStr;
  }
}
