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

package org.apache.tajo.engine.function.string;

import com.google.gson.annotations.Expose;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.tajo.plan.expr.FunctionEval.ParamType;

/**
 * This function is defined as:
 * <pre>
 * regexp_replace(string text, pattern text, replacement text [, flags text])
 * </pre>
 *
 * flags is not supported yet.
 */
@Description(
  functionName = "regexp_replace",
  description = " Replace substring(s) matching a POSIX regular expression.",
  example = "> SELECT regexp_replace('Thomas', '.[mN]a.', 'M');\n"
          + "ThM",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT,
          TajoDataTypes.Type.TEXT,TajoDataTypes.Type.TEXT})}
)
public class RegexpReplace extends GeneralFunction {
  @Expose protected boolean isPatternConstant;

  // transient variables
  private boolean isAlwaysNull = false;
  protected Pattern compiled;

  public RegexpReplace() {
    super(new Column[] {
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("pattern", TajoDataTypes.Type.TEXT),
        new Column("replacement", TajoDataTypes.Type.INT4),
        new Column("flags", TajoDataTypes.Type.INT4), // it is not supported yet.
    });
  }

  @Override
  public void init(OverridableConf context, ParamType[] paramTypes) {
    if (paramTypes[0] == ParamType.NULL || paramTypes[1] == ParamType.NULL || paramTypes[2] == ParamType.NULL) {
      isAlwaysNull = true;
    } else if (paramTypes[1] == ParamType.CONSTANT) {
      isPatternConstant = true;
    }
  }

  @Override
  public Datum eval(Tuple params) {
    if (isAlwaysNull || params.isBlankOrNull(0) || params.isBlankOrNull(1) || params.isBlankOrNull(2)) {
      return NullDatum.get();
    }

    String value = params.getText(0);
    String replacement = params.getText(2);

    Pattern thisCompiled;
    if (compiled != null) {
      thisCompiled = compiled;
    } else {
      thisCompiled = Pattern.compile(params.getText(1));

      // if a regular expression pattern is a constant,
      // it will be reused in every call
      if (isPatternConstant) {
        compiled = thisCompiled;
      }
    }

    Matcher matcher = thisCompiled.matcher(value);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      matcher.appendReplacement(sb, replacement);
    }
    matcher.appendTail(sb);

    return DatumFactory.createText(sb.toString());
  }
}
