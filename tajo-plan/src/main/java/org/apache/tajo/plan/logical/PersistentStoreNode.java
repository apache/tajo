/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.logical;


import com.google.gson.annotations.Expose;

import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;


/**
 * <code>PersistentStoreNode</code> an expression for a persistent data store step.
 * This includes some basic information for materializing data.
 */
public abstract class PersistentStoreNode extends UnaryNode implements Cloneable {
  @Expose protected String storageType = BuiltinStorages.TEXT;
  @Expose protected KeyValueSet options = new KeyValueSet();

  protected PersistentStoreNode(int pid, NodeType nodeType) {
    super(pid, nodeType);
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public String getStorageType() {
    return this.storageType;
  }

  public boolean hasOptions() {
    return this.options != null;
  }

  public KeyValueSet getOptions() {
    return this.options;
  }

  public void setOptions(KeyValueSet options) {
    this.options = options;
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this);
    planStr.addExplan("Store type: " + storageType);

    return planStr;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((options == null) ? 0 : options.hashCode());
    result = prime * result + ((storageType == null) ? 0 : storageType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PersistentStoreNode) {
      PersistentStoreNode other = (PersistentStoreNode) obj;
      boolean eq = super.equals(other);
      eq = eq && this.storageType.equals(other.storageType);
      eq = eq && TUtil.checkEquals(options, other.options);
      return eq;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    PersistentStoreNode store = (PersistentStoreNode) super.clone();
    store.storageType = storageType != null ? storageType : null;
    store.options = options != null ? (KeyValueSet) options.clone() : null;
    return store;
  }
}
