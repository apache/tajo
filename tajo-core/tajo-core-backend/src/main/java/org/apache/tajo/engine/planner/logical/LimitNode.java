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

import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.planner.PlanString;

public final class LimitNode extends UnaryNode implements Cloneable {
	@Expose private long fetchFirstNum;

  public LimitNode(int pid) {
    super(pid, NodeType.LIMIT);
  }

  public void setFetchFirst(long num) {
    this.fetchFirstNum = num;
  }
  
  public long getFetchFirstNum() {
    return fetchFirstNum;
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this).appendTitle(" " + fetchFirstNum);
  }
  
  @Override 
  public boolean equals(Object obj) {
    if (obj instanceof LimitNode) {
      LimitNode other = (LimitNode) obj;
      return super.equals(other)
          && fetchFirstNum == other.fetchFirstNum;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    LimitNode newLimitNode = (LimitNode) super.clone();
    newLimitNode.fetchFirstNum = fetchFirstNum;
    return newLimitNode;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Limit (").append(fetchFirstNum).append(")");

    sb.append("\n  \"out schema: ").append(getOutSchema())
        .append("\n  \"in schema: " + getInSchema());
    sb.append("\n").append(getChild().toString());

    return sb.toString();
  }
}
