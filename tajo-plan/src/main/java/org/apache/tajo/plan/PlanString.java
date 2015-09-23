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

package org.apache.tajo.plan;

import org.apache.tajo.plan.logical.LogicalNode;

import java.util.ArrayList;
import java.util.List;


public class PlanString {
  final StringBuilder title;

  final List<String> explanations = new ArrayList<>();
  final List<String> details = new ArrayList<>();

  StringBuilder currentExplanation;
  StringBuilder currentDetail;

  public PlanString(LogicalNode node) {
    this.title = new StringBuilder(node.getType().name() + "(" + node.getPID() + ")");
  }

  public PlanString(String title) {
    this.title = new StringBuilder(title);
  }

  public PlanString appendTitle(String str) {
    title.append(str);
    return this;
  }

  public PlanString addExplan(String explain) {
    flushCurrentExplanation();
    currentExplanation = new StringBuilder(explain);
    return this;
  }

  public PlanString appendExplain(String explain) {
    if (currentExplanation == null) {
      currentExplanation = new StringBuilder();
    }
    currentExplanation.append(explain);
    return this;
  }

  public PlanString addDetail(String detail) {
    flushCurrentDetail();
    currentDetail = new StringBuilder(detail);
    return this;
  }

  public PlanString appendDetail(String detail) {
    if (currentDetail == null) {
      currentDetail = new StringBuilder();
    }
    currentDetail.append(detail);
    return this;

  }

  public String getTitle() {
    return title.toString();
  }

  public List<String> getExplanations() {
    flushCurrentExplanation();
    return explanations;
  }

  public List<String> getDetails() {
    flushCurrentDetail();
    return details;
  }

  private void flushCurrentExplanation() {
    if (currentExplanation != null) {
      explanations.add(currentExplanation.toString());
      currentExplanation = null;
    }
  }

  private void flushCurrentDetail() {
    if (currentDetail != null) {
      details.add(currentDetail.toString());
      currentDetail = null;
    }
  }

  public String toString() {
    StringBuilder output = new StringBuilder();
    output.append(getTitle()).append("\n");

    for (String str : getExplanations()) {
      output.append("  => ").append(str).append("\n");
    }

    for (String str : getDetails()) {
      output.append("  => ").append(str).append("\n");
    }
    return output.toString();
  }
}
