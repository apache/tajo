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
package org.apache.tajo.resource;


/**
 * A set of {@link NodeResource} comparison and manipulation interfaces.
 */

public abstract class ResourceCalculator {

  public abstract int 
  compare(NodeResource clusterResource, NodeResource lhs, NodeResource rhs);
  
  public static int divideAndCeil(int a, int b) {
    if (b == 0) {
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  public static int roundUp(int a, int b) {
    return divideAndCeil(a, b) * b;
  }

  public static int roundDown(int a, int b) {
    return (a / b) * b;
  }

  /**
   * Compute the number of containers which can be allocated given
   * <code>available</code> and <code>required</code> resources.
   * 
   * @param available available resources
   * @param required required resources
   * @return number of containers which can be allocated
   */
  public abstract int computeAvailableContainers(
      NodeResource available, NodeResource required);
  /**
   * Multiply resource <code>r</code> by factor <code>by</code> 
   * and normalize up using step-factor <code>stepFactor</code>.
   * 
   * @param r resource to be multiplied
   * @param by multiplier
   * @param stepFactor factor by which to normalize up 
   * @return resulting normalized resource
   */
  public abstract NodeResource multiplyAndNormalizeUp(
      NodeResource r, double by, NodeResource stepFactor);
  
  /**
   * Multiply resource <code>r</code> by factor <code>by</code> 
   * and normalize down using step-factor <code>stepFactor</code>.
   * 
   * @param r resource to be multiplied
   * @param by multiplier
   * @param stepFactor factor by which to normalize down 
   * @return resulting normalized resource
   */
  public abstract NodeResource multiplyAndNormalizeDown(
      NodeResource r, double by, NodeResource stepFactor);
  
  /**
   * Normalize resource <code>r</code> given the base 
   * <code>minimumResource</code> and verify against max allowed
   * <code>maximumResource</code>
   * 
   * @param r resource
   * @param minimumResource step-factor
   * @param maximumResource the upper bound of the resource to be allocated
   * @return normalized resource
   */
  public NodeResource normalize(NodeResource r, NodeResource minimumResource,
      NodeResource maximumResource) {
    return normalize(r, minimumResource, maximumResource, minimumResource);
  }

  /**
   * Normalize resource <code>r</code> given the base 
   * <code>minimumResource</code> and verify against max allowed
   * <code>maximumResource</code> using a step factor for hte normalization.
   *
   * @param r resource
   * @param minimumResource minimum value
   * @param maximumResource the upper bound of the resource to be allocated
   * @param stepFactor the increment for resources to be allocated
   * @return normalized resource
   */
  public abstract NodeResource normalize(NodeResource r, NodeResource minimumResource,
                                     NodeResource maximumResource,
                                     NodeResource stepFactor);


  /**
   * Round-up resource <code>r</code> given factor <code>stepFactor</code>.
   * 
   * @param r resource
   * @param stepFactor step-factor
   * @return rounded resource
   */
  public abstract NodeResource roundUp(NodeResource r, NodeResource stepFactor);
  
  /**
   * Round-down resource <code>r</code> given factor <code>stepFactor</code>.
   * 
   * @param r resource
   * @param stepFactor step-factor
   * @return rounded resource
   */
  public abstract NodeResource roundDown(NodeResource r, NodeResource stepFactor);
  
  /**
   * Divide resource <code>numerator</code> by resource <code>denominator</code>
   * using specified policy (domination, average, fairness etc.); hence overall
   * <code>clusterResource</code> is provided for context.
   *  
   * @param clusterResource cluster resources
   * @param numerator numerator
   * @param denominator denominator
   * @return <code>numerator</code>/<code>denominator</code> 
   *         using specific policy
   */
  public abstract float divide(
      NodeResource clusterResource, NodeResource numerator, NodeResource denominator);
  
  /**
   * Determine if a resource is not suitable for use as a divisor
   * (will result in divide by 0, etc)
   *
   * @param r resource
   * @return true if divisor is invalid (should not be used), false else
   */
  public abstract boolean isInvalidDivisor(NodeResource r);

  /**
   * Ratio of resource <code>a</code> to resource <code>b</code>.
   * 
   * @param a resource 
   * @param b resource
   * @return ratio of resource <code>a</code> to resource <code>b</code>
   */
  public abstract float ratio(NodeResource a, NodeResource b);

  /**
   * Divide-and-ceil <code>numerator</code> by <code>denominator</code>.
   * 
   * @param numerator numerator resource
   * @param denominator denominator
   * @return resultant resource
   */
  public abstract NodeResource divideAndCeil(NodeResource numerator, int denominator);
  
}
