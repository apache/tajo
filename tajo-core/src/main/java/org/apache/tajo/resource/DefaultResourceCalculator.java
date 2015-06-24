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


public class DefaultResourceCalculator extends ResourceCalculator {
  
  @Override
  public int compare(NodeResource unused, NodeResource lhs, NodeResource rhs) {
    return lhs.compareTo(rhs);
  }

  @Override
  public int computeAvailableContainers(NodeResource available, NodeResource required) {
    return Math.min(Math.min(
        available.getMemory() / required.getMemory(),
        available.getDisks() / required.getDisks()),
        available.getVirtualCores() / required.getVirtualCores());
  }

  @Override
  public float divide(NodeResource unused,
                      NodeResource numerator, NodeResource denominator) {
    return ratio(numerator, denominator);
  }
  
  public boolean isInvalidDivisor(NodeResource r) {
    if (r.getMemory() == 0.0f) {
      return true;
    }
    return false;
  }

  @Override
  public float ratio(NodeResource a, NodeResource b) {
    return (float)a.getMemory() / b.getMemory();
  }

  @Override
  public NodeResource divideAndCeil(NodeResource numerator, int denominator) {
    return NodeResources.createResource(
        divideAndCeil(numerator.getMemory(), denominator));
  }

  @Override
  public NodeResource normalize(NodeResource r, NodeResource minimumResource,
                                NodeResource maximumResource, NodeResource stepFactor) {
    int normalizedMemory = Math.min(
        roundUp(
            Math.max(r.getMemory(), minimumResource.getMemory()),
            stepFactor.getMemory()),
            maximumResource.getMemory());
    return NodeResources.createResource(normalizedMemory);
  }

  @Override
  public NodeResource normalize(NodeResource r, NodeResource minimumResource,
                                NodeResource maximumResource) {
    return normalize(r, minimumResource, maximumResource, minimumResource);
  }

  @Override
  public NodeResource roundUp(NodeResource r, NodeResource stepFactor) {
    return NodeResources.createResource(
        roundUp(r.getMemory(), stepFactor.getMemory())
    );
  }

  @Override
  public NodeResource roundDown(NodeResource r, NodeResource stepFactor) {
    return NodeResources.createResource(
        roundDown(r.getMemory(), stepFactor.getMemory()));
  }

  @Override
  public NodeResource multiplyAndNormalizeUp(NodeResource r, double by,
      NodeResource stepFactor) {
    return NodeResources.createResource(
        roundUp((int) (r.getMemory() * by + 0.5), stepFactor.getMemory())
    );
  }

  @Override
  public NodeResource multiplyAndNormalizeDown(NodeResource r, double by,
      NodeResource stepFactor) {
    return NodeResources.createResource(
        roundDown(
            (int) (r.getMemory() * by),
            stepFactor.getMemory()
        )
    );
  }

}
