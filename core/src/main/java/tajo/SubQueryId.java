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

package tajo;

import java.text.NumberFormat;

public abstract class SubQueryId implements Comparable<SubQueryId> {

  /**
   * @return the associated <code>QueryId</code>
   */
  public abstract QueryId getQueryId();

  /**
   * @return the subquery number.
   */
  public abstract int getId();

  public abstract void setQueryId(QueryId jobId);

  public abstract void setId(int id);

  protected static final String SUBQUERY = "query";

  static final ThreadLocal<NumberFormat> subQueryIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getId();
    result = prime * result + getQueryId().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SubQueryId other = (SubQueryId) obj;
    if (getId() != other.getId())
      return false;
    if (!getQueryId().equals(other.getQueryId()))
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(SUBQUERY);
    QueryId queryId = getQueryId();
    builder.append("_").append(queryId.getAppId().getClusterTimestamp());
    builder.append("_").append(
        QueryId.queryIdFormat.get().format(queryId.getAppId().getId()));
    builder.append("_").append(
        subQueryIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  public int compareTo(SubQueryId other) {
    int queryIdComp = this.getQueryId().compareTo(other.getQueryId());
    if (queryIdComp == 0) {
      return this.getId() - other.getId();
    } else {
      return queryIdComp;
    }
  }
}