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

package org.apache.tajo.engine.planner.logical;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.planner.Target;

/**
 * Projectable is an interface for a LogicalNode which has a list of targets.
 * What a logical node has a list of targets means that the node evaluated a list of expressions.
 * For example, {@link org.apache.tajo.engine.planner.logical.ScanNode},
 * {@link org.apache.tajo.engine.planner.logical.JoinNode},
 * {@link org.apache.tajo.engine.planner.logical.GroupbyNode}, and
 * {@link org.apache.tajo.engine.planner.logical.ProjectionNode} are all <i>Projectable</i> nodes.
 * The expression evaluation occurs only at those projectable nodes.
 */
public interface Projectable {

  /**
   * Get a PlanNode Id
   * @return PlanNodeId
   */
  int getPID();

  /**
   * check if this node has a target list
   * @return TRUE if this node has a target list. Otherwise, FALSE.
   */
  boolean hasTargets();

  /**
   * Set a target list
   *
   * @param targets The array of targets
   */
  void setTargets(Target[] targets);

  /**
   * Get a list of targets
   *
   * @return The array of targets
   */
  Target [] getTargets();

  /**
   * Get an input schema
   * @return The input schema
   */
  public Schema getInSchema();

  /**
   * Get an output schema
   *
   * @return The output schema
   */
  public Schema getOutSchema();
}
