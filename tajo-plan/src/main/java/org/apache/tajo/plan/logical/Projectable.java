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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.Target;

import java.util.List;
import java.util.stream.Stream;

/**
 * Projectable is an interface for a LogicalNode which has a list of targets.
 * What a logical node has a list of targets means that the node evaluated a list of expressions.
 * For example, {@link ScanNode},
 * {@link JoinNode},
 * {@link GroupbyNode}, and
 * {@link ProjectionNode} are all <i>Projectable</i> nodes.
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
  void setTargets(List<Target> targets);

  /**
   * Get a list of targets
   *
   * @return The array of targets
   */
  List<Target> getTargets();

  /**
   * Get a stream pipeline for Targets
   *
   * @return Stream for Targets
   */
  Stream<Target> targets();

  /**
   * Get an input schema
   * @return The input schema
   */
  Schema getInSchema();

  /**
   * Get an output schema
   *
   * @return The output schema
   */
  Schema getOutSchema();
}
