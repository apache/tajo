/*
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

import com.google.gson.annotations.Expose;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.PlanString;

public class SetSessionNode extends LogicalNode {
  @Expose private String name;
  @Expose private String value;

  public SetSessionNode(int pid) {
    super(pid, NodeType.SET_SESSION);
  }

  /**
   * If both name and value are given, it will set a session variable.
   * If a name is only given, it will unset a session variable.
   *
   * @param name Session variable name
   * @param value Session variable value
   */
  public void init(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public boolean hasValue() {
    return value != null;
  }

  public String getValue() {
    return value;
  }

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public LogicalNode getChild(int idx) {
    throw new UnsupportedException();
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public PlanString getPlanString() {
    PlanString planString = new PlanString("SET SESSION ");
    planString.appendTitle(name).appendTitle("=");
    if (value != null) {
      planString.appendTitle(String.valueOf(value));
    }
    return planString;
  }
}
