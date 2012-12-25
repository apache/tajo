/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import tajo.catalog.Column;
import tajo.engine.eval.EvalNode;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock;
import tajo.util.TUtil;

import java.util.Arrays;

/** 
 * @author Hyunsik Choi
 */
public class GroupbyNode extends UnaryNode implements Cloneable {
	@Expose
	private Column[] columns;
	@Expose
	private EvalNode havingCondition = null;
	@Expose
	private QueryBlock.Target[] targets;
	
	public GroupbyNode() {
		super();
	}
	
	public GroupbyNode(final Column [] columns) {
		super(ExprType.GROUP_BY);
		this.columns = columns;
	}
	
	public GroupbyNode(final Column [] columns, 
	    final EvalNode havingCondition) {
    this(columns);
    this.havingCondition = havingCondition;
  }
	
	public final Column [] getGroupingColumns() {
	  return this.columns;
	}
	
	public final boolean hasHavingCondition() {
	  return this.havingCondition != null;
	}
	
	public final EvalNode getHavingCondition() {
	  return this.havingCondition;
	}
	
	public final void setHavingCondition(final EvalNode evalTree) {
	  this.havingCondition = evalTree;
	}
	
  public boolean hasTargetList() {
    return this.targets != null;
  }

  public QueryBlock.Target[] getTargets() {
    return this.targets;
  }

  public void setTargetList(QueryBlock.Target[] targets) {
    this.targets = targets;
  }
  
  public void setSubNode(LogicalNode subNode) {
    super.setSubNode(subNode);
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("\"GroupBy\": {\"fields\":[");
    for (int i=0; i < columns.length; i++) {
      sb.append("\"").append(columns[i]).append("\"");
      if(i < columns.length - 1)
        sb.append(",");
    }
    
    if(hasHavingCondition()) {
      sb.append("], \"having qual\": \""+havingCondition+"\"");
    }
    if(hasTargetList()) {
      sb.append(", \"target\": [");
      for (int i = 0; i < targets.length; i++) {
        sb.append("\"").append(targets[i]).append("\"");
        if( i < targets.length - 1) {
          sb.append(",");
        }
      }
      sb.append("],");
    }
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInSchema());
    sb.append("}");
    
    return sb.toString() + "\n"
        + getSubNode().toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GroupbyNode) {
      GroupbyNode other = (GroupbyNode) obj;
      return super.equals(other) 
          && Arrays.equals(columns, other.columns)
          && TUtil.checkEquals(havingCondition, other.havingCondition)
          && TUtil.checkEquals(targets, other.targets)
          && getSubNode().equals(other.getSubNode());
    } else {
      return false;  
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    GroupbyNode grp = (GroupbyNode) super.clone();
    if (columns != null) {
      grp.columns = new Column[columns.length];
      for (int i = 0; i < columns.length; i++) {
        grp.columns[i] = (Column) columns[i].clone();
      }
    }
    grp.havingCondition = (EvalNode) (havingCondition != null 
        ? havingCondition.clone() : null);    
    if (targets != null) {
      grp.targets = new QueryBlock.Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        grp.targets[i] = (QueryBlock.Target) targets[i].clone();
      }
    }

    return grp;
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
