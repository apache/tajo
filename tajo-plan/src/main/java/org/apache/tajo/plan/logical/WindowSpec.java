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

package org.apache.tajo.plan.logical;


import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.algebra.WindowSpec.WindowFrameEndBoundType;
import static org.apache.tajo.algebra.WindowSpec.WindowFrameStartBoundType;

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
      return
          TUtil.checkEquals(windowName, another.windowName) &&
          TUtil.checkEquals(partitionKeys, another.partitionKeys) &&
          TUtil.checkEquals(windowFrame, another.windowFrame);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(windowName, partitionKeys, windowFrame);
  }

  public static class WindowFrame implements Cloneable {
    @Expose private WindowStartBound startBound;
    @Expose private WindowEndBound endBound;
    @Expose org.apache.tajo.algebra.WindowSpec.WindowFrameUnit unit; // TODO - to be supported

    public WindowFrame() {
      this.startBound = new WindowStartBound(WindowFrameStartBoundType.UNBOUNDED_PRECEDING);
      this.endBound = new WindowEndBound(WindowFrameEndBoundType.UNBOUNDED_FOLLOWING);
    }

    public WindowFrame(WindowStartBound startBound, WindowEndBound endBound) {
      this.startBound = startBound;
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

    public void setFrameUnit(org.apache.tajo.algebra.WindowSpec.WindowFrameUnit unit) {
      this.unit = unit;
    }

    public org.apache.tajo.algebra.WindowSpec.WindowFrameUnit getFrameUnit() {
      return this.unit;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof WindowFrame) {
        WindowFrame another = (WindowFrame) obj;
        boolean eq = TUtil.checkEquals(startBound, another.startBound);
        eq &= TUtil.checkEquals(endBound, another.endBound);
        eq &= TUtil.checkEquals(unit, another.unit);
        return eq;
      } else {
        return false;
      }
    }

    public WindowFrame clone() throws CloneNotSupportedException {
      WindowFrame newFrame = (WindowFrame) super.clone();
      newFrame.startBound = startBound.clone();
      newFrame.endBound = endBound.clone();
      newFrame.unit = unit;
      return newFrame;
    }

    public int hashCode() {
      return Objects.hashCode(startBound, endBound, unit);
    }
  }

  public static class WindowStartBound implements Cloneable {
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

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof WindowStartBound) {
        WindowStartBound other = (WindowStartBound) obj;
        boolean eq = boundType == other.boundType;
        eq &= TUtil.checkEquals(number, other.number);
        return eq;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(boundType, number);
    }

    @Override
    public WindowStartBound clone() throws CloneNotSupportedException {
      WindowStartBound newStartBound = (WindowStartBound) super.clone();
      newStartBound.boundType = boundType;
      if (number != null) {
        newStartBound.number = (EvalNode) number.clone();
      }
      return newStartBound;
    }
  }

  public static class WindowEndBound implements Cloneable {
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

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof WindowEndBound) {
        WindowEndBound other = (WindowEndBound) obj;
        boolean eq = boundType == other.boundType;
        eq &= TUtil.checkEquals(number, other.number);
        return eq;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(boundType, number);
    }

    public WindowEndBound clone() throws CloneNotSupportedException {
      WindowEndBound newEndBound = (WindowEndBound) super.clone();
      newEndBound.boundType = boundType;
      if (number != null) {
        newEndBound.number = (EvalNode) number.clone();
      }
      return newEndBound;
    }
  }
}
