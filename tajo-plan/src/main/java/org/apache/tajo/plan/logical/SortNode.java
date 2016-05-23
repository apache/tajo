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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.util.TUtil;

import java.util.Arrays;

public final class SortNode extends UnaryNode implements Cloneable {
  public enum SortPurpose {
    NORMAL,
    STORAGE_SPECIFIED
  }

	@Expose private SortSpec [] sortKeys;
  @Expose private SortPurpose sortPurpose;

  public SortNode(int pid) {
    super(pid, NodeType.SORT);
    sortPurpose = SortPurpose.NORMAL;
  }

  public void setSortSpecs(SortSpec[] sortSpecs) {
    Preconditions.checkArgument(sortSpecs.length > 0, "At least one sort key must be specified");
    this.sortKeys = sortSpecs;
  }
  
  public SortSpec[] getSortKeys() {
    return this.sortKeys;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(sortKeys);
    result = prime * result + ((sortPurpose == null) ? 0 : sortPurpose.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SortNode) {
      SortNode other = (SortNode) obj;
      boolean eq = super.equals(other);
      eq = eq && TUtil.checkEquals(sortKeys, other.sortKeys);
      return eq;
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

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this);
    StringBuilder sb = new StringBuilder("Sort Keys: ");
    for (int i = 0; i < sortKeys.length; i++) {
      sb.append(sortKeys[i].toString());
      if( i < sortKeys.length - 1) {
        sb.append(",");
      }
    }
    planStr.addExplan(sb.toString());
    return planStr;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Sort [key= ");
    for (int i = 0; i < sortKeys.length; i++) {    
      sb.append(sortKeys[i].toString());
      if(i < sortKeys.length - 1) {
        sb.append(",");
      }
    }
    sb.append("]");

    sb.append("\n\"out schema: " + getOutSchema()
        + "\n\"in schema: " + getInSchema());
    return sb.toString()+"\n"
        + getChild().toString();
  }

  public SortPurpose getSortPurpose() {
    return sortPurpose;
  }

  public void setSortPurpose(SortPurpose sortPurpose) {
    this.sortPurpose = sortPurpose;
  }
}
