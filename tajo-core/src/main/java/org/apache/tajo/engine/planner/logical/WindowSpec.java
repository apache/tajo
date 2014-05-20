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

package org.apache.tajo.engine.planner.logical;


import com.google.gson.annotations.Expose;
import org.apache.tajo.algebra.WindowSpecExpr;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.util.TUtil;

import java.util.Comparator;

import static org.apache.tajo.algebra.WindowSpecExpr.WindowFrameEndBoundType;
import static org.apache.tajo.algebra.WindowSpecExpr.WindowFrameStartBoundType;

public class WindowSpec {
  @Expose private String windowName;

  @Expose private Column[] partitionKeys;

  @Expose private WindowFrame windowFrame;

  public String getWindowName() {
    return windowName;
  }

  public boolean hasPartitionKeys() {
    return partitionKeys != null;
  }

  public Column [] getPartitionKeys() {
    return partitionKeys;
  }

  public boolean hasWindowFrame() {
    return windowFrame != null;
  }

  public WindowFrame getWindowFrame() {
    return windowFrame;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof WindowSpec) {
      WindowSpec another = (WindowSpec) obj;
      return TUtil.checkEquals(partitionKeys, another.partitionKeys) &&

          TUtil.checkEquals(windowFrame, another.windowFrame);
    } else {
      return false;
    }
  }

  public static class WindowFrame {
    @Expose private WindowStartBound startBound;
    @Expose private WindowEndBound endBound;
    @Expose WindowSpecExpr.WindowFrameUnit unit; // TODO - to be supported

    public WindowFrame() {
      this.startBound = new WindowStartBound(WindowFrameStartBoundType.UNBOUNDED_PRECEDING);
      this.endBound = new WindowEndBound(WindowFrameEndBoundType.UNBOUNDED_FOLLOWING);
    }

    public WindowFrame(WindowStartBound startBound) {
      this.startBound = startBound;
    }

    public WindowFrame(WindowStartBound startBound, WindowEndBound endBound) {
      this(startBound);
      this.endBound = endBound;
    }

    public WindowStartBound getStartBound() {
      return startBound;
    }

    public boolean hasEndBound() {
      return endBound != null;
    }

    public WindowEndBound getEndBound() {
      return endBound;
    }

    public boolean hasFrameUnit() {
      return this.unit != null;
    }

    public void setFrameUnit(WindowSpecExpr.WindowFrameUnit unit) {
      this.unit = unit;
    }

    public WindowSpecExpr.WindowFrameUnit getFrameUnit() {
      return this.unit;
    }
  }

  public static class WindowStartBound {
    @Expose private WindowFrameStartBoundType boundType;
    @Expose private EvalNode number;

    public WindowStartBound(WindowFrameStartBoundType type) {
      this.boundType = type;
    }

    public WindowFrameStartBoundType getBoundType() {
      return boundType;
    }

    public void setNumber(EvalNode number) {
      this.number = number;
    }

    public EvalNode getNumber() {
      return number;
    }
  }

  public static class WindowEndBound {
    @Expose private WindowFrameEndBoundType boundType;
    @Expose private EvalNode number;

    public WindowEndBound(WindowFrameEndBoundType type) {
      this.boundType = type;
    }

    public WindowFrameEndBoundType getBoundType() {
      return boundType;
    }

    public EvalNode setNumber(EvalNode number) {
      return number;
    }

    public EvalNode getNumber() {
      return number;
    }
  }
}
