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

/**
 * 
 */
package org.apache.tajo.engine.planner.global;

import org.apache.tajo.master.ExecutionBlock;

public class MasterPlan {
  private ExecutionBlock root;
  private String outputTableName;

  public MasterPlan(ExecutionBlock root) {
    setRoot(root);
  }
  
  public void setRoot(ExecutionBlock root) {
    this.root = root;
  }
  
  public ExecutionBlock getRoot() {
    return this.root;
  }

  public void setOutputTableName(String tableName) {
    this.outputTableName = tableName;
  }

  public String getOutputTable() {
    return outputTableName;
  }
}
