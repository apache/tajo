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

package org.apache.tajo.engine.planner.global;

import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputContext implements GsonObject {
  @Expose private Map<Integer, ScanNode> scanNodes = new HashMap<Integer, ScanNode>();

  public InputContext() {

  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, InputContext.class);
  }

  public ScanNode[] getScanNodes() {
    return scanNodes.values().toArray(new ScanNode[scanNodes.size()]);
  }

  public ScanNode getScanNode(int pid) {
    return scanNodes.get(pid);
  }

  public void setScanNodes(List<ScanNode> scanNodes) {
    this.scanNodes.clear();
    for (ScanNode scan : scanNodes) {
      addScanNode(scan);
    }
  }

  public void addScanNode(ScanNode scanNode) {
    this.scanNodes.put(scanNode.getPID(), scanNode);
  }

  public int size() {
    return scanNodes.size();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof InputContext) {
      InputContext other = (InputContext) o;
      return TUtil.checkEquals(this.getScanNodes(), other.getScanNodes());
    }
    return false;
  }

  public boolean contains(ScanNode scanNode) {
    return this.scanNodes.containsKey(scanNode.getPID());
  }
}
