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

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class TimestampLiteral extends Expr implements Cloneable {
  @Expose @SerializedName("Date")
  private DateValue date;
  @Expose @SerializedName("Time")
  private TimeValue time;

  public TimestampLiteral(DateValue date, TimeValue time) {
    super(OpType.TimestampLiteral);
    this.date = date;
    this.time = time;
  }

  public DateValue getDate() {
    return date;
  }

  public TimeValue getTime() {
    return time;
  }

  public String toString() {
    return date + " " + time;
  }

  public int hashCode() {
    return Objects.hashCode(date, time);
  }

  @Override
  boolean equalsTo(Expr expr) {
    if (expr instanceof TimestampLiteral) {
      TimestampLiteral another = (TimestampLiteral) expr;
      return date.equals(another.date) && time.equals(another.time);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    TimestampLiteral timestampLiteral = (TimestampLiteral) super.clone();
    timestampLiteral.date = date;
    timestampLiteral.time = time;
    return timestampLiteral;
  }
}
