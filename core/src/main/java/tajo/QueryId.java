/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package tajo;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.text.NumberFormat;

/**
 * QueryId represents a unique identifier of a query.
 */
public abstract class QueryId implements Comparable<QueryId> {
  public static final String PREFIX = "query";
  public static final String SEPARATOR = "_";

  static final ThreadLocal<NumberFormat> queryIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(4);
          return fmt;
        }
      };
  

  public abstract ApplicationId getAppId();

  public abstract void setAppId(ApplicationId appId);

  public abstract int getId();

  public abstract void setId(int id);

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(PREFIX);
    builder.append(SEPARATOR);
    builder.append(getAppId().getClusterTimestamp());
    builder.append(SEPARATOR);
    builder.append(queryIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QueryId other = (QueryId) obj;
    if (!this.getAppId().equals(other.getAppId()))
      return false;
    if (this.getId() != other.getId())
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getAppId().hashCode();
    result = prime * result + getId();
    return result;
  }

  @Override
  public int compareTo(QueryId other) {
    int appIdComp = this.getAppId().compareTo(other.getAppId());
    if (appIdComp == 0) {
      return this.getId() - other.getId();
    } else {
      return appIdComp;
    }
  }
}