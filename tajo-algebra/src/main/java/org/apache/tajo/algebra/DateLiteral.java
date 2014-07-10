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

public class DateLiteral extends Expr {
  @Expose @SerializedName("Date")
  private DateValue date;

  public DateLiteral(DateValue date) {
    super(OpType.DateLiteral);
    this.date = date;
  }

  public DateValue getDate() {
    return date;
  }

  public String toString() {
    return date.toString();
  }

  public int hashCode() {
    return Objects.hashCode(date);
  }

  @Override
  boolean equalsTo(Expr expr) {
    if (expr instanceof DateLiteral) {
      DateLiteral another = (DateLiteral) expr;
      return date.equals(another.date);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DateLiteral newDate = (DateLiteral) super.clone();
    newDate.date = date;
    return newDate;
  }
}
