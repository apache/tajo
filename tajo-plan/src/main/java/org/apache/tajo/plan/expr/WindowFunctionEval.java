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

package org.apache.tajo.plan.expr;

import java.util.Arrays;

import com.google.gson.annotations.Expose;

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.WindowSpec;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

public class WindowFunctionEval extends AggregationFunctionCallEval implements Cloneable {
  @Expose private SortSpec [] sortSpecs;
  @Expose WindowSpec.WindowFrame windowFrame;

  public WindowFunctionEval(FunctionDesc desc, AggFunction instance, EvalNode[] givenArgs,
                            WindowSpec.WindowFrame windowFrame) {
    super(EvalType.WINDOW_FUNCTION, desc, instance, givenArgs);
    this.windowFrame = windowFrame;
  }

  public boolean hasSortSpecs() {
    return sortSpecs != null;
  }

  public void setSortSpecs(SortSpec [] sortSpecs) {
    this.sortSpecs = sortSpecs;
  }

  public SortSpec [] getSortSpecs() {
    return sortSpecs;
  }

  public WindowSpec.WindowFrame getWindowFrame() {
    return windowFrame;
  }

  @Override
  protected void mergeParam(FunctionContext context, Tuple params) {
    instance.eval(context, params);
  }

  @Override
  public Datum terminate(FunctionContext context) {
    if (!isBinded) {
      throw new IllegalStateException("bind() must be called before terminate()");
    }
    return instance.terminate(context);
  }

  @Override
  public DataType getValueType() {
    return funcDesc.getReturnType();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Arrays.hashCode(sortSpecs);
    result = prime * result + ((windowFrame == null) ? 0 : windowFrame.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof WindowFunctionEval) {
      WindowFunctionEval other = (WindowFunctionEval) obj;
      boolean eq = TUtil.checkEquals(sortSpecs, other.sortSpecs);
      eq &= TUtil.checkEquals(windowFrame, other.windowFrame);
      return eq;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    WindowFunctionEval windowFunctionEval = (WindowFunctionEval) super.clone();
    if (sortSpecs != null) {
      windowFunctionEval.sortSpecs = new SortSpec[sortSpecs.length];
      for (int i = 0; i < sortSpecs.length; i++) {
        windowFunctionEval.sortSpecs[i] = (SortSpec) sortSpecs[i].clone();
      }
    }
    windowFunctionEval.windowFrame = windowFrame.clone();
    return windowFunctionEval;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (argEvals != null) {
      for(int i=0; i < argEvals.length; i++) {
        sb.append(argEvals[i]);
        if(i+1 < argEvals.length)
          sb.append(",");
      }
    }
    sb.append(funcDesc.getFunctionName()).append("(").append(isDistinct() ? " distinct" : "").append(sb)
        .append(")");
    if (hasSortSpecs()) {
      sb.append("ORDER BY ").append(TUtil.arrayToString(sortSpecs));
    }
    return sb.toString();
  }
}
