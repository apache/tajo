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

package org.apache.tajo.engine.function.builtin;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.InternalTypes.StringAggProto;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * TEXT string_agg(expr TEXT, delimiter TEXT)
 */
@Description(
  functionName = "string_agg",
  description = "input values concatenated into a string, separated by delimiter",
  example = "> SELECT string_agg(expr, delimiter);",
  returnType = Type.TEXT,
  paramTypes = { @ParamTypes(paramTypes = { Type.TEXT, Type.TEXT }) }
)
public class StringAgg extends AggFunction<Datum> {

  public StringAgg() {
    super(new Column[] {
      new Column("expr", Type.TEXT),
      new Column("delimiter", Type.TEXT)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new ConcatContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    ConcatContext concatCtx = (ConcatContext) ctx;

    String concatData = null;
    if (!params.isBlankOrNull(0)) {
      concatData = params.getText(0);
    }

    if (concatCtx.concatData.length() > 0) {
      concatCtx.concatData.append(concatCtx.delimiter);
    } else {
      // When first time, set the delimiter.
      concatCtx.delimiter = StringEscapeUtils.unescapeJava(params.getText(1));
    }
    concatCtx.concatData.append(concatData);
  }

  @Override
  public void merge(FunctionContext context, Tuple part) {
    ConcatContext concatCtx = (ConcatContext) context;
    if (part.isBlankOrNull(0)) {
      return;
    }

    ProtobufDatum datum = (ProtobufDatum) part.getProtobufDatum(0);
    StringAggProto proto = (StringAggProto) datum.get();

    String delimiter = proto.getDelimiter();
    String concatData = proto.getValue();

    if (concatCtx.concatData.length() > 0) {
      concatCtx.concatData.append(delimiter);
    }
    concatCtx.concatData.append(concatData);
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    ConcatContext concatCtx = (ConcatContext) ctx;
    if (concatCtx.concatData.length() == 0) {
      return NullDatum.get();
    }

    StringAggProto.Builder builder = StringAggProto.newBuilder();
    builder.setDelimiter(concatCtx.delimiter);
    builder.setValue(concatCtx.concatData.toString());
    return new ProtobufDatum(builder.build());
  }

  @Override
  public DataType getPartialResultType() {
    return CatalogUtil.newDataType(Type.PROTOBUF, StringAggProto.class.getName());
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    ConcatContext concatCtx = (ConcatContext) ctx;
    if (concatCtx.concatData.length() == 0) {
      return NullDatum.get();
    } else {
      return DatumFactory.createText(concatCtx.concatData.toString());
    }
  }

  protected static class ConcatContext implements FunctionContext {
    String delimiter;
    StringBuilder concatData = new StringBuilder(128);
  }
}
