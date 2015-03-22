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
import static org.apache.tajo.algebra.WindowSpec.WindowFrameUnit;

public class LogicalWindowSpec {
  @Expose private String windowName;

  @Expose private Column[] partitionKeys;

  @Expose private LogicalWindowFrame logicalWindowFrame;

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
    return logicalWindowFrame != null;
  }

  public LogicalWindowFrame getLogicalWindowFrame() {
    return logicalWindowFrame;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LogicalWindowSpec) {
      LogicalWindowSpec another = (LogicalWindowSpec) obj;
      return
          TUtil.checkEquals(windowName, another.windowName) &&
          TUtil.checkEquals(partitionKeys, another.partitionKeys) &&
          TUtil.checkEquals(logicalWindowFrame, another.logicalWindowFrame);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(windowName, partitionKeys, logicalWindowFrame);
  }

  public static class LogicalWindowFrame implements Cloneable {
    @Expose private LogicalWindowStartBound startBound;
    @Expose private LogicalWindowEndBound endBound;
    @Expose private WindowFrameUnit unit; // TODO - to be supported

    public LogicalWindowFrame() {
      this.unit = WindowFrameUnit.RANGE;
      this.startBound = new LogicalWindowStartBound(WindowFrameStartBoundType.UNBOUNDED_PRECEDING);
      this.endBound = new LogicalWindowEndBound(WindowFrameEndBoundType.CURRENT_ROW);
    }

    public LogicalWindowFrame(WindowFrameUnit unit, LogicalWindowStartBound startBound, LogicalWindowEndBound endBound) {
      this.unit = unit;
      this.startBound = startBound;
      this.endBound = endBound;
    }

    public LogicalWindowStartBound getStartBound() {
      return startBound;
    }

    public boolean hasEndBound() {
      return endBound != null;
    }

    public LogicalWindowEndBound getEndBound() {
      return endBound;
    }

    public boolean hasFrameUnit() {
      return this.unit != null;
    }

    public void setFrameUnit(WindowFrameUnit unit) {
      this.unit = unit;
    }

    public WindowFrameUnit getFrameUnit() {
      return this.unit;
    }

    public static enum WindowFrameType {
      ENTIRE_PARTITION,
      TO_CURRENT_ROW,
      FROM_CURRENT_ROW,
      SLIDING_WINDOW
    }

    private WindowFrameType frameType;

    public WindowFrameType getFrameType() {
      return frameType;
    }

    public void setFrameType(WindowFrameType type) {
      frameType = type;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof LogicalWindowFrame) {
        LogicalWindowFrame another = (LogicalWindowFrame) obj;
        boolean eq = TUtil.checkEquals(startBound, another.startBound);
        eq &= TUtil.checkEquals(endBound, another.endBound);
        eq &= TUtil.checkEquals(unit, another.unit);
        return eq;
      } else {
        return false;
      }
    }

    public LogicalWindowFrame clone() throws CloneNotSupportedException {
      LogicalWindowFrame newFrame = (LogicalWindowFrame) super.clone();
      newFrame.startBound = startBound.clone();
      newFrame.endBound = endBound.clone();
      newFrame.unit = unit;
      return newFrame;
    }

    public int hashCode() {
      return Objects.hashCode(startBound, endBound, unit);
    }
  }

  public static class LogicalWindowStartBound implements Cloneable {
    @Expose private WindowFrameStartBoundType boundType;
    @Expose private int number;

    public LogicalWindowStartBound(WindowFrameStartBoundType type) {
      this.boundType = type;
    }

    public WindowFrameStartBoundType getBoundType() {
      return boundType;
    }

    public void setNumber(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof LogicalWindowStartBound) {
        LogicalWindowStartBound other = (LogicalWindowStartBound) obj;
        boolean eq = boundType == other.boundType;
        eq &= (number == other.number);
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
    public LogicalWindowStartBound clone() throws CloneNotSupportedException {
      LogicalWindowStartBound newStartBound = (LogicalWindowStartBound) super.clone();
      newStartBound.boundType = boundType;
      newStartBound.number = number;
      return newStartBound;
    }
  }

  public static class LogicalWindowEndBound implements Cloneable {
    @Expose private WindowFrameEndBoundType boundType;
    @Expose private int number;

    public LogicalWindowEndBound(WindowFrameEndBoundType type) {
      this.boundType = type;
    }

    public WindowFrameEndBoundType getBoundType() {
      return boundType;
    }

    public void setNumber(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof LogicalWindowEndBound) {
        LogicalWindowEndBound other = (LogicalWindowEndBound) obj;
        boolean eq = boundType == other.boundType;
        eq &= (number == other.number);
        return eq;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(boundType, number);
    }

    public LogicalWindowEndBound clone() throws CloneNotSupportedException {
      LogicalWindowEndBound newEndBound = (LogicalWindowEndBound) super.clone();
      newEndBound.boundType = boundType;
      newEndBound.number = number;
      return newEndBound;
    }
  }
}
