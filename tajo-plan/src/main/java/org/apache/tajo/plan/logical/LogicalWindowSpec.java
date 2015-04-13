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
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.algebra.WindowSpec.WindowFrameBoundType;
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
    @Expose private LogicalWindowBound startBound;
    @Expose private LogicalWindowBound endBound;
    @Expose private WindowFrameUnit unit;

    public LogicalWindowFrame() {
      this.unit = WindowFrameUnit.RANGE;
      this.startBound = new LogicalWindowBound(WindowFrameBoundType.UNBOUNDED_PRECEDING);
      this.endBound = new LogicalWindowBound(WindowFrameBoundType.CURRENT_ROW);
    }

    public LogicalWindowFrame(WindowFrameUnit unit, LogicalWindowBound startBound, LogicalWindowBound endBound) {
      this.unit = unit;
      this.startBound = startBound;
      this.endBound = endBound;
    }

    public LogicalWindowBound getStartBound() {
      return startBound;
    }

    public boolean hasEndBound() {
      return endBound != null;
    }

    public LogicalWindowBound getEndBound() {
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

    public enum WindowFrameType {
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

  public static class LogicalWindowBound implements Cloneable {
    @Expose private WindowFrameBoundType boundType;
    @Expose private int number;

    public LogicalWindowBound(WindowFrameBoundType type) {
      this.boundType = type;
    }

    public WindowFrameBoundType getBoundType() {
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
      if (obj instanceof LogicalWindowBound) {
        LogicalWindowBound other = (LogicalWindowBound) obj;
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
    public LogicalWindowBound clone() throws CloneNotSupportedException {
      LogicalWindowBound newStartBound = (LogicalWindowBound) super.clone();
      newStartBound.boundType = boundType;
      newStartBound.number = number;
      return newStartBound;
    }
  }
}
