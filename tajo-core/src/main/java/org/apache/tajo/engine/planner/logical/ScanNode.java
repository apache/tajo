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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.util.TUtil;

public class ScanNode extends RelationNode implements Projectable, Cloneable {
	@Expose protected TableDesc tableDesc;
  @Expose protected String alias;
  @Expose protected Schema logicalSchema;
	@Expose protected EvalNode qual;
	@Expose protected Target[] targets;
  @Expose protected boolean broadcastTable;

  protected ScanNode(int pid, NodeType nodeType) {
    super(pid, nodeType);
  }

  public ScanNode(int pid) {
    super(pid, NodeType.SCAN);
  }

  public void init(TableDesc desc) {
    this.tableDesc = desc;
    this.setInSchema(tableDesc.getSchema());
    this.setOutSchema(tableDesc.getSchema());
    logicalSchema = SchemaUtil.getQualifiedLogicalSchema(tableDesc, null);
  }
  
	public void init(TableDesc desc, String alias) {
    this.tableDesc = desc;
    this.alias = alias;

    if (!CatalogUtil.isFQTableName(this.tableDesc.getName())) {
      throw new IllegalArgumentException("the name in TableDesc must be qualified, but it is \"" +
          desc.getName() + "\"");
    }

    String databaseName = CatalogUtil.extractQualifier(this.tableDesc.getName());
    String qualifiedAlias = CatalogUtil.buildFQName(databaseName, alias);
    this.setInSchema(tableDesc.getSchema());
    this.getInSchema().setQualifier(qualifiedAlias);
    this.setOutSchema(new Schema(getInSchema()));
    logicalSchema = SchemaUtil.getQualifiedLogicalSchema(tableDesc, qualifiedAlias);
	}
	
	public String getTableName() {
	  return tableDesc.getName();
	}

  @Override
	public boolean hasAlias() {
	  return alias != null;
	}

  @Override
  public String getAlias() {
    return alias;
  }

  public void setBroadcastTable(boolean broadcastTable) {
    this.broadcastTable = broadcastTable;
  }

  public boolean isBroadcastTable() {
    return broadcastTable;
  }

  public String getCanonicalName() {
    if (CatalogUtil.isFQTableName(this.tableDesc.getName())) {
      String databaseName = CatalogUtil.extractQualifier(this.tableDesc.getName());
      return hasAlias() ? CatalogUtil.buildFQName(databaseName, alias) : tableDesc.getName();
    } else {
      return hasAlias() ? alias : tableDesc.getName();
    }
  }

  public Schema getTableSchema() {
    return logicalSchema;
  }

  public Schema getPhysicalSchema() {
    return getInSchema();
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
    setOutSchema(PlannerUtil.targetToSchema(targets));
	}

  @Override
	public Target [] getTargets() {
	  return this.targets;
	}

  public TableDesc getTableDesc() {
    return tableDesc;
  }
	
	public String toString() {
    StringBuilder sb = new StringBuilder("Scan (table=").append(getTableName());
    if (hasAlias()) {
      sb.append(", alias=").append(alias);
    }
    if (hasQual()) {
      sb.append(", filter=").append(qual);
    }
    sb.append(", path=").append(getTableDesc().getPath()).append(")");
    return sb.toString();
	}

  @Override
  public int hashCode() {
    return Objects.hashCode(this.tableDesc, this.qual, this.targets);
  }
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof ScanNode) {
	    ScanNode other = (ScanNode) obj;
	    
	    boolean eq = super.equals(other); 
	    eq = eq && TUtil.checkEquals(this.tableDesc, other.tableDesc);
	    eq = eq && TUtil.checkEquals(this.qual, other.qual);
	    eq = eq && TUtil.checkEquals(this.targets, other.targets);
	    
	    return eq;
	  }	  
	  
	  return false;
	}	
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  ScanNode scanNode = (ScanNode) super.clone();
	  
	  scanNode.tableDesc = (TableDesc) this.tableDesc.clone();
	  
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
    PlanString planStr = new PlanString(this).appendTitle(" on ").appendTitle(getTableName());
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

    return planStr;
  }

  public static boolean isScanNode(LogicalNode node) {
    return node.getType() == NodeType.SCAN ||
        node.getType() == NodeType.PARTITIONS_SCAN;
  }
}
