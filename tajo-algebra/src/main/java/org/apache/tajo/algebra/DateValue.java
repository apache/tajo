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

public class DateValue implements Cloneable {
  @Expose @SerializedName("Year")
  private String years;
  @Expose @SerializedName("Month")
  private String months;
  @Expose @SerializedName("Day")
  private String days;

  public DateValue(String years, String months, String days) {

    this.years = years;
    this.months = months;
    this.days = days;
  }

  public String getYears() {
    return years;
  }

  public String getMonths() {
    return months;
  }

  public String getDays() {
    return days;
  }

  public String toString() {
    return String.format("%s-%s-%s", years, months, days);
  }

  public int hashCode() {
    return Objects.hashCode(years, months, days);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof DateValue) {
      DateValue another = (DateValue) object;
      return years.equals(another.years) && months.equals(another.months) && days.equals(another.days);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DateValue date = (DateValue) super.clone();
    date.years = years;
    date.months = months;
    date.days = days;
    return date;
  }
}
