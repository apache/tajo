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

package org.apache.tajo.catalog.partition;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

public class Specifier implements ProtoObject<CatalogProtos.SpecifierProto>, Cloneable,
    GsonObject {

  private static final Log LOG = LogFactory.getLog(Specifier.class);
  protected CatalogProtos.SpecifierProto.Builder builder = null;


  @Expose protected String name;
  @Expose protected String expressions;

  public Specifier() {
    builder = CatalogProtos.SpecifierProto.newBuilder();
  }

  public Specifier(String name) {
    this();
    this.name = name;
  }

  public Specifier(String name, String expressions) {
    this();
    this.name = name;
    this.expressions = expressions;
  }

  public Specifier(CatalogProtos.SpecifierProto proto) {
    this();
    this.name = proto.getName().toLowerCase();
    this.expressions = proto.getExpressions();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getExpressions() {
    return expressions;
  }

  public void setExpressions(String expressions) {
    this.expressions = expressions;
  }

  public boolean equals(Object o) {
    if (o instanceof Specifier) {
      Specifier other = (Specifier)o;
      boolean eq = TUtil.checkEquals(this.name, other.name);
      eq = eq && TUtil.checkEquals(this.expressions, other.expressions);
      return  eq;
    }
    return false;
  }

  public int hashCode() {
    return Objects.hashCode(this.name, this.expressions);

  }

  public Object clone() throws CloneNotSupportedException {
    Specifier clone = (Specifier) super.clone();
    clone.builder = CatalogProtos.SpecifierProto.newBuilder();
    clone.name = this.name;
    clone.expressions = this.expressions;
    return clone;
  }

  public String toString() {
    Gson gson = CatalogGsonHelper.getPrettyInstance();
    return gson.toJson(this);
  }

  @Override
  public CatalogProtos.SpecifierProto getProto() {
    if(builder == null) {
      builder = CatalogProtos.SpecifierProto.newBuilder();
    }

    if(this.name != null) {
      builder.setName(this.name);
    }

    if(this.expressions != null) {
      builder.setExpressions(this.expressions);
    }

    return builder.build();
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, Specifier.class);
  }
}
