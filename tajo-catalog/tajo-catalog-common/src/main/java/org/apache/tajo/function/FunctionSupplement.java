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

import com.google.gson.annotations.Expose;
import org.apache.tajo.common.ProtoObject;

import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionSupplementProto;

/**
 * Supplement information for a function
 */
public class FunctionSupplement implements ProtoObject<FunctionSupplementProto>, Cloneable {

  @Expose
  private String shortDescription;
  @Expose
  private String detail;
  @Expose
  private String example;

  public FunctionSupplement() {
    this("", "", "");
  }

  public FunctionSupplement(String shortDescription, String detail, String example) {
    this.shortDescription = shortDescription;
    this.detail = detail;
    this.example = example;
  }

  public FunctionSupplement(FunctionSupplementProto proto) {
    if (proto.hasShortDescription()) {
      shortDescription = proto.getShortDescription();
    } else {
      shortDescription = "";
    }
    if (proto.hasDetail()) {
      detail = proto.getDetail();
    } else {
      detail = "";
    }
    if (proto.hasExample()) {
      example = proto.getExample();
    } else {
      example = "";
    }
  }

  public void setShortDescription(String shortDescription) {
    this.shortDescription = shortDescription;
  }

  public String getShortDescription() {
    return shortDescription;
  }

  public void setDetail(String detail) {
    this.detail = detail;
  }

  public String getDetail() {
    return detail;
  }

  public void setExample(String example) {
    this.example = example;
  }

  public String getExample() {
    return example;
  }

  @Override
  public FunctionSupplementProto getProto() {
    FunctionSupplementProto.Builder builder = FunctionSupplementProto.newBuilder();
    if (shortDescription != null) {
      builder.setShortDescription(shortDescription);
    }
    if (detail != null) {
      builder.setDetail(detail);
    }
    if (example != null) {
      builder.setExample(example);
    }
    return builder.build();
  }

  @Override
  public FunctionSupplement clone() throws CloneNotSupportedException {
    FunctionSupplement newSupplement = (FunctionSupplement) super.clone();
    newSupplement.shortDescription = shortDescription;
    newSupplement.detail = detail;
    newSupplement.example = example;
    return newSupplement;
  }
}
