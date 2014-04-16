/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import org.apache.tajo.util.TUtil;

public class WindowSpecExpr extends Expr {
  private String windowName;
  private Expr [] partitionKeys; // OVER (PARTITION BY ?,...,?)
  private Sort.SortSpec [] sortSpecs; // OVER (... ORDER BY ?,...,?)
  private WindowFrame windowFrame;

  public WindowSpecExpr() {
    super(OpType.WindowSpec);
  }

  public boolean hasWindowName() {
    return windowName != null;
  }

  public void setWindowName(String windowName) {
    this.windowName = windowName;
  }

  public String getWindowName() {
    return this.windowName;
  }

  public boolean hasPartitionBy() {
    return this.partitionKeys != null;
  }

  public void setPartitionKeys(Expr[] partitionKeys) {
    this.partitionKeys = partitionKeys;
  }

  public Expr [] getPartitionKeys() {
    return this.partitionKeys;
  }

  public boolean hasOrderBy() {
    return this.sortSpecs != null;
  }

  public void setSortSpecs(Sort.SortSpec [] sortSpecs) {
    this.sortSpecs = sortSpecs;
  }

  public Sort.SortSpec [] getSortSpecs() {
    return this.sortSpecs;
  }

  public boolean hasWindowFrame() {
    return windowFrame != null;
  }

  public void setWindowFrame(WindowFrame frame) {
    this.windowFrame = frame;
  }

  public WindowFrame getWindowFrame() {
    return windowFrame;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(windowName, partitionKeys, sortSpecs);
  }

  @Override
  boolean equalsTo(Expr expr) {
    WindowSpecExpr another = (WindowSpecExpr) expr;
    return TUtil.checkEquals(windowName, another.windowName) &&
        TUtil.checkEquals(partitionKeys, another.partitionKeys) &&
        TUtil.checkEquals(sortSpecs, another.sortSpecs);
  }

  public static enum WindowFrameUnit {
    ROW,
    RANGE
  }

  public static enum WindowFrameStartBoundType {
    UNBOUNDED_PRECEDING,
    CURRENT_ROW,
    PRECEDING
  }

  public static enum WindowFrameEndBoundType {
    UNBOUNDED_FOLLOWING,
    CURRENT_ROW,
    FOLLOWING
  }

  public static class WindowFrame {
    WindowFrameUnit unit;
    private WindowStartBound startBound;
    private WindowEndBound endBound;

    public WindowFrame(WindowFrameUnit unit, WindowStartBound startBound) {
      this.unit = unit;
      this.startBound = startBound;
    }

    public WindowFrame(WindowFrameUnit unit, WindowStartBound startBound, WindowEndBound endBound) {
      this(unit, startBound);
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
  }

  public static class WindowStartBound {
    private WindowFrameStartBoundType boundType;
    private Expr number;

    public WindowStartBound(WindowFrameStartBoundType type) {
      this.boundType = type;
    }

    public WindowFrameStartBoundType getBoundType() {
      return boundType;
    }

    public void setNumber(Expr number) {
      this.number = number;
    }

    public Expr getNumber() {
      return number;
    }
  }

  public static class WindowEndBound {
    private WindowFrameEndBoundType boundType;
    private Expr number;

    public WindowEndBound(WindowFrameEndBoundType type) {
      this.boundType = type;
    }

    public WindowFrameEndBoundType getBoundType() {
      return boundType;
    }

    public Expr setNumber(Expr number) {
      return number;
    }

    public Expr getNumber() {
      return number;
    }
  }
}
