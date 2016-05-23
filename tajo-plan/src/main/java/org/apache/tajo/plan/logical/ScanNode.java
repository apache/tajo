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
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.commons.lang.StringUtils;
import org.apache.tajo.catalog.*;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang.StringUtils.capitalize;

public class ScanNode extends RelationNode implements Projectable, SelectableNode, Cloneable {
	@Expose protected TableDesc tableDesc;
  @Expose protected String alias;
  @Expose protected Schema logicalSchema;
	@Expose protected EvalNode qual;
	@Expose protected List<Target> targets = null;
  @Expose protected boolean broadcastTable;
  @Expose protected long limit = -1; // -1 means no set

  protected ScanNode(int pid, NodeType nodeType) {
    super(pid, nodeType);
  }

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public LogicalNode getChild(int idx) {
    return null;
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

    if (!IdentifierUtil.isFQTableName(this.tableDesc.getName())) {
      throw new IllegalArgumentException("the name in TableDesc must be qualified, but it is \"" +
          desc.getName() + "\"");
    }

    String databaseName = IdentifierUtil.extractQualifier(this.tableDesc.getName());
    String qualifiedAlias = IdentifierUtil.buildFQName(databaseName, alias);
    this.setInSchema(tableDesc.getSchema());
    this.getInSchema().setQualifier(qualifiedAlias);
    this.setOutSchema(SchemaBuilder.builder().addAll(getInSchema().getRootColumns()).build());
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
    if (IdentifierUtil.isFQTableName(this.tableDesc.getName())) {
      String databaseName = IdentifierUtil.extractQualifier(this.tableDesc.getName());
      return hasAlias() ? IdentifierUtil.buildFQName(databaseName, alias) : tableDesc.getName();
    } else {
      return hasAlias() ? alias : tableDesc.getName();
    }
  }

  public Schema getLogicalSchema() {
    return logicalSchema;
  }

  public Schema getPhysicalSchema() {
    return getInSchema();
  }

  @Override
	public boolean hasQual() {
	  return qual != null;
	}

  @Override
	public EvalNode getQual() {
	  return this.qual;
	}

  @Override
	public void setQual(EvalNode evalTree) {
	  this.qual = evalTree;
	}

  @Override
	public boolean hasTargets() {
	  return this.targets != null;
	}

  @Override
	public void setTargets(List<Target> targets) {
	  this.targets = targets;
    setOutSchema(PlannerUtil.targetToSchema(targets));
	}

  @Override
  public List<Target> getTargets() {
    if (hasTargets()) {
      return this.targets;
    } else {
      return null;
    }
  }

  /**
   *
   *
   * @return
   */
  public boolean hasLimit() {
    return limit > 0;
  }

  /**
   * How many rows will be retrieved?
   *
   * @return The number of rows to be retrieved
   */
  public long getLimit() {
    return limit;
  }

  public void setLimit(long num) {
    Preconditions.checkArgument(num > 0, "The number of fetch rows in limit is negative");
    this.limit = num;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }
	
	public String toString() {
    StringBuilder sb = new StringBuilder(capitalize(getType().name()) + " (table=").append(getTableName());
    if (hasAlias()) {
      sb.append(", alias=").append(alias);
    }
    if (hasQual()) {
      sb.append(", filter=").append(qual);
    }
    sb.append(", path=").append(getTableDesc().getUri()).append(")");
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
      eq = eq && TUtil.checkEquals(this.alias, other.alias);
	    
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
      scanNode.targets = new ArrayList<>();
      for (Target t : targets) {
        scanNode.targets.add((Target) t.clone());
      }
    }

    if (hasAlias()) {
      scanNode.alias = alias;
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
      planStr.addExplan("target list: ").appendExplain(StringUtils.join(targets, ", "));
    }

    planStr.addDetail("out schema: ").appendDetail(getOutSchema().toString());
    planStr.addDetail("in schema: ").appendDetail(getInSchema().toString());

    return planStr;
  }
}
