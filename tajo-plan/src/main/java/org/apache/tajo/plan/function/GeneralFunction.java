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

package org.apache.tajo.plan.function;

import com.google.gson.annotations.Expose;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.function.Function;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.storage.Tuple;

import java.util.TimeZone;

@Deprecated
public abstract class GeneralFunction extends Function implements GsonObject {
  @Expose
  protected TimeZone timeZone;
  public GeneralFunction(Column[] definedArgs) {
    super(definedArgs);
  }

  /**
   * This method gives hints to an actual function instance.
   */
  @SuppressWarnings("unused")
  public void init(OverridableConf queryContext, FunctionEval.ParamType [] paramTypes) {}

  public abstract Datum eval(Tuple params);

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, GeneralFunction.class);
  }

  @Override
  public CatalogProtos.FunctionType getFunctionType() {
    return CatalogProtos.FunctionType.GENERAL;
  }

  public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public boolean hasTimeZone() {
    return timeZone != null;
  }
}