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

package org.apache.tajo.storage.fragment;

import com.google.common.collect.ImmutableList;

import java.net.URI;

/**
 * The Fragment is similar to the split in MapReduce.
 * For distributed processing of a a single large table,
 * it contains the information of which part of data will be processed by each task.
 *
 * @param <T> type of fragment key. It should implement the Comparable interface.
 */
public abstract class Fragment<T extends Comparable> implements Comparable<Fragment<T>>, Cloneable {

  protected String kind;
  protected URI uri;
  protected String inputSourceId;
  protected T startKey;
  protected T endKey;
  protected long length;
  protected ImmutableList<String> hostNames;

  protected Fragment(String kind,
                     URI uri,
                     String inputSourceId,
                     T startKey,
                     T endKey,
                     long length,
                     String[] hostNames) {
    this.kind = kind;
    this.uri = uri;
    this.inputSourceId = inputSourceId;
    this.startKey = startKey;
    this.endKey = endKey;
    this.length = length;
    this.hostNames = hostNames == null ? ImmutableList.of() : ImmutableList.copyOf(hostNames);
  }

  /**
   * Returns the fragment type.
   *
   * @return fragment type
   */
  public final String getKind() {
    return kind;
  }

  /**
   * Returns an unique URI of the input source.
   *
   * @return URI of the input source
   */
  public final URI getUri() {
    return uri;
  }

  /**
   * Returns a unique id of the input source.
   *
   * @return id of the input source
   */
  public final String getInputSourceId() {
    return this.inputSourceId;
  }

  /**
   * Returns the start key of the data range.
   * {@link org.apache.tajo.storage.Scanner} will start reading data from the point indicated by this key.
   *
   * @return start key
   */
  public final T getStartKey() {
    return startKey;
  }

  /**
   * Returns the end key of the data range.
   * {@link org.apache.tajo.storage.Scanner} will stop reading data when it reaches the point indicated by this key.
   *
   * @return end key
   */
  public final T getEndKey() {
    return endKey;
  }

  /**
   * Returns the length of the data range between start key and end key.
   *
   * @return length of the range
   */
  public final long getLength() {
    return length;
  }

  /**
   * Returns host names which have any portion of the data between start key and end key.
   *
   * @return host names
   */
  public final ImmutableList<String> getHostNames() {
    return hostNames;
  }

  /**
   * Indicates the fragment is empty or not.
   * An empty fragment means that there is no data to read.
   *
   * @return true if the fragment is empty. Otherwise, false.
   */
  public boolean isEmpty() {
    return length == 0;
  }

  /**
   * First compares URIs of fragments, and then compares their start keys.
   *
   * @param t
   * @return return 0 if two fragments are same. If not same, return -1 if this fragment is smaller than the other.
   * Otherwise, return 1;
   */
  @Override
  public final int compareTo(Fragment<T> t) {
    int cmp = uri.compareTo(t.uri);
    if (cmp == 0) {
      if (startKey != null && t.startKey != null) {
        return startKey.compareTo(t.startKey);
      } else if (startKey == null) {  // nulls last
        return 1;
      } else {
        return -1;
      }
    } else {
      return cmp;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Fragment clone = (Fragment) super.clone();
    clone.uri = this.uri;
    clone.inputSourceId = this.inputSourceId;
    clone.startKey = this.startKey;
    clone.endKey = this.endKey;
    clone.hostNames = this.hostNames;
    clone.length = this.length;
    return clone;
  }
}
