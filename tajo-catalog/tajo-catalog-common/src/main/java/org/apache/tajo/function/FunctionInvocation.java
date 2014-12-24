/***
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

package org.apache.tajo.function;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.ProtoObject;

import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionInvocationProto;

public class FunctionInvocation implements ProtoObject<FunctionInvocationProto> {
  @Expose
  ClassBaseInvocationDesc<Function> legacy;
  @Expose
  StaticMethodInvocationDesc scalar;
  @Expose
  ClassBaseInvocationDesc<?> aggregation;
  @Expose
  StaticMethodInvocationDesc scalarJIT;
  @Expose
  ClassBaseInvocationDesc<?> aggregationJIT;

  public FunctionInvocation() {
  }

  public FunctionInvocation(FunctionInvocationProto proto) {
    if (proto.hasLegacy()) {
      this.legacy = new ClassBaseInvocationDesc(proto.getLegacy());
    }
    if (proto.hasScalar()) {
      this.scalar = new StaticMethodInvocationDesc(proto.getScalar());
    }
    if (proto.hasAggregation()) {
      this.aggregation = new ClassBaseInvocationDesc(proto.getAggregation());
    }
    if (proto.hasScalarJIT()) {
      this.scalarJIT = new StaticMethodInvocationDesc(proto.getScalarJIT());
    }
    if (proto.hasAggregationJIT()) {
      this.aggregationJIT = new ClassBaseInvocationDesc(proto.getAggregation());
    }
  }

  public boolean isAvailable() {
    return legacy != null && scalar != null && scalarJIT != null;
  }

  public boolean hasLegacy() {
    return legacy != null;
  }

  public void setLegacy(ClassBaseInvocationDesc<Function> legacy) {
    this.legacy = legacy;
  }

  public ClassBaseInvocationDesc<Function> getLegacy() {
    return legacy;
  }

  public boolean hasScalar() {
    return scalar != null;
  }

  public void setScalar(StaticMethodInvocationDesc scalar) {
    this.scalar = scalar;
  }

  public StaticMethodInvocationDesc getScalar() {
    return scalar;
  }

  public boolean hasAggregation() {
    return aggregation != null;
  }

  public void setAggregation(ClassBaseInvocationDesc<?> aggregation) {
    this.aggregation = aggregation;
  }

  public ClassBaseInvocationDesc<?> getAggregation() {
    return aggregation;
  }

  public boolean hasScalarJIT() {
    return scalarJIT != null;
  }

  public void setScalarJIT(StaticMethodInvocationDesc scalarJIT) {
    this.scalarJIT = scalarJIT;
  }

  public StaticMethodInvocationDesc getScalarJIT() {
    return scalarJIT;
  }

  public boolean hasAggregationJIT() {
    return aggregationJIT != null;
  }

  public void setAggregationJIT(ClassBaseInvocationDesc<?> aggregationJIT) {
    this.aggregationJIT = aggregationJIT;
  }

  public ClassBaseInvocationDesc<?> getAggregationJIT() {
    return aggregationJIT;
  }

  @Override
  public FunctionInvocationProto getProto() {
    FunctionInvocationProto.Builder builder = FunctionInvocationProto.newBuilder();
    if (hasLegacy()) {
      builder.setLegacy(legacy.getProto());
    }
    if (hasScalar()) {
      builder.setScalar(scalar.getProto());
    }
    if (hasAggregation()) {
      builder.setAggregation(aggregation.getProto());
    }
    if (hasScalarJIT()) {
      builder.setScalarJIT(scalarJIT.getProto());
    }
    if (hasAggregationJIT()) {
      builder.setAggregationJIT(aggregationJIT.getProto());
    }
    return builder.build();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(legacy, scalar, scalarJIT);
  }

  public String toString() {
    return "legacy=" + hasLegacy() + ",scalar=" + hasScalar() + ",agg=" + hasAggregation() +
        ",scalarJIT=" + hasScalarJIT() + ",aggJIT=" + hasAggregationJIT();
  }
}
