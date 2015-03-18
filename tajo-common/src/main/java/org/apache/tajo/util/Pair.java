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

package org.apache.tajo.util;

import com.google.common.base.Objects;

public class Pair<T1, T2> {
  private T1 first;
  private T2 second;

  public Pair() {

  }

  public Pair(T1 first, T2 second) {
    this.first = first;
    this.second = second;
  }

  public void set(T1 first, T2 second) {
    this.first = first;
    this.second = second;
  }

  public T1 getFirst() {
    return first;
  }

  public T2 getSecond() {
    return second;
  }

  public void setFirst(T1 first) {
    this.first = first;
  }

  public void setSecond(T2 second) {
    this.second = second;
  }

  @Override
  public String toString() {
    return first + "," + second;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Pair) {
      Pair other = (Pair) obj;
      return first.equals(other.first) && second.equals(other.second);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(first, second);
  }
}
