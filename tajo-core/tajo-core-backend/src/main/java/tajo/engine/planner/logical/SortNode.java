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

package tajo.engine.planner.logical;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import tajo.catalog.Schema;
import tajo.catalog.SortSpec;
import tajo.engine.json.GsonCreator;
import tajo.util.TUtil;

public final class SortNode extends UnaryNode implements Cloneable {
	@Expose
  private SortSpec[] sortKeys;
	
	public SortNode() {
		super();
	}

  public SortNode(SortSpec[] sortKeys) {
    super(ExprType.SORT);
    Preconditions.checkArgument(sortKeys.length > 0, 
        "At least one sort key must be specified");
    this.sortKeys = sortKeys;
  }

  public SortNode(SortSpec[] sortKeys, Schema inSchema, Schema outSchema) {
    this(sortKeys);
    this.setInSchema(inSchema);
    this.setOutSchema(outSchema);
  }
  
  public SortSpec[] getSortKeys() {
    return this.sortKeys;
  }
  
  @Override 
  public boolean equals(Object obj) {
    if (obj instanceof SortNode) {
      SortNode other = (SortNode) obj;
      return super.equals(other)
          && TUtil.checkEquals(sortKeys, other.sortKeys)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SortNode sort = (SortNode) super.clone();
    sort.sortKeys = sortKeys.clone();
    
    return sort;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("Sort [key= ");
    for (int i = 0; i < sortKeys.length; i++) {    
      sb.append(sortKeys[i].getSortKey().getQualifiedName()).append(" ")
          .append(sortKeys[i].isAscending() ? "asc" : "desc");
      if(i < sortKeys.length - 1) {
        sb.append(",");
      }
    }
    sb.append("]");

    sb.append("\n\"out schema: " + getOutSchema()
        + "\n\"in schema: " + getInSchema());
    return sb.toString()+"\n"
        + getSubNode().toString();
  }

  public String toJSON() {
    subExpr.toJSON();
    for (int i = 0; i < sortKeys.length; i++) {
      sortKeys[i].toJSON();
    }
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
