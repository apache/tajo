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
import com.google.gson.annotations.Expose;
import org.apache.tajo.util.TUtil;

public class WindowSpec implements Cloneable {
  @Expose private String windowName;
  @Expose private Expr [] partitionKeys; // OVER (PARTITION BY ?,...,?)
  @Expose private Sort.SortSpec [] sortSpecs; // OVER (... ORDER BY ?,...,?)
  @Expose private WindowFrame windowFrame;

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

  public Object clone() throws CloneNotSupportedException {
    WindowSpec windowSpec = (WindowSpec) super.clone();
    windowSpec.windowName = windowName;
    if (hasPartitionBy()) {
      windowSpec.partitionKeys = new Expr[windowSpec.partitionKeys.length];
      for (int i = 0; i < partitionKeys.length; i++) {
        windowSpec.partitionKeys[i] = (Expr) partitionKeys[i].clone();
      }
    }
    if (hasOrderBy()) {
      windowSpec.sortSpecs = new Sort.SortSpec[sortSpecs.length];
      for (int i = 0; i < sortSpecs.length; i++) {
        windowSpec.sortSpecs[i] = (Sort.SortSpec) sortSpecs[i].clone();
      }
    }
    if (hasWindowFrame()) {
      windowSpec.windowFrame = (WindowFrame) windowFrame.clone();
    }
    return windowSpec;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(windowName, partitionKeys, sortSpecs);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj instanceof WindowSpec) {
      WindowSpec another = (WindowSpec) obj;
      return TUtil.checkEquals(windowName, another.windowName) &&
          TUtil.checkEquals(partitionKeys, another.partitionKeys) &&
          TUtil.checkEquals(sortSpecs, another.sortSpecs) &&
          TUtil.checkEquals(windowFrame, another.windowFrame);
    } else {
      return false;
    }

  }

  public enum WindowFrameUnit {
    ROW,
    RANGE
  }

  public enum WindowFrameBoundType {
    UNBOUNDED_PRECEDING,
    UNBOUNDED_FOLLOWING,
    CURRENT_ROW,
    PRECEDING,
    FOLLOWING
  }

  public static class WindowFrame implements Cloneable {
    @Expose private WindowFrameUnit unit;
    @Expose private WindowBound startBound;
    @Expose private WindowBound endBound;

    public WindowFrame(WindowFrameUnit unit, WindowBound startBound) {
      this.unit = unit;
      this.startBound = startBound;
    }

    public WindowFrame(WindowFrameUnit unit, WindowBound startBound, WindowBound endBound) {
      this(unit, startBound);
      this.endBound = endBound;
    }

    public WindowFrameUnit getFrameUnit() {
      return unit;
    }

    public WindowBound getStartBound() {
      return startBound;
    }

    public boolean hasEndBound() {
      return endBound != null;
    }

    public WindowBound getEndBound() {
      return endBound;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      WindowFrame frame = (WindowFrame) super.clone();
      frame.unit = unit;
      frame.startBound = (WindowBound) startBound.clone();
      frame.endBound = (WindowBound) endBound.clone();
      return frame;
    }
  }

  public static class WindowBound implements Cloneable {
    @Expose private WindowFrameBoundType boundType;
    @Expose private Expr number;

    public WindowBound(WindowFrameBoundType type) {
      this.boundType = type;
    }

    public WindowFrameBoundType getBoundType() {
      return boundType;
    }

    public boolean hasNumber() {
      return this.number != null;
    }

    public void setNumber(Expr number) {
      this.number = number;
    }

    public Expr getNumber() {
      return number;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      WindowBound start = (WindowBound) super.clone();
      start.boundType = boundType;
      start.number = (Expr) number.clone();
      return start;
    }
  }
}
