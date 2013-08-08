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

public class LogicalRootNode extends UnaryNode implements Cloneable {
  public LogicalRootNode() {
    super(ExprType.ROOT);
  }
  
  public String toString() {
    return "Logical Plan Root\n\n" + getSubNode().toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LogicalRootNode) {
      LogicalRootNode other = (LogicalRootNode) obj;
      boolean b1 = super.equals(other);
      boolean b2 = subExpr.equals(other.subExpr);
      
      return b1 && b2;
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
