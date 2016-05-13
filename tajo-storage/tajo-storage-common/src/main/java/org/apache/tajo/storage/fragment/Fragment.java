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

import org.apache.tajo.common.ProtoObject;

import java.net.URI;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public abstract class Fragment<T extends Comparable>
    implements Comparable<Fragment<T>>, ProtoObject<FragmentProto>, Cloneable {

  protected URI uri;
  protected String tableName;
  protected T startKey;
  protected T endKey;
  protected long length;
  protected String[] hostNames;

  protected Fragment() {}

  protected Fragment(URI uri, String tableName, T startKey, T endKey, long length, String[] hostNames) {
    this.uri = uri;
    this.tableName = tableName;
    this.startKey = startKey;
    this.endKey = endKey;
    this.length = length;
    this.hostNames = hostNames;
  }

  /**
   * Returns an URI of the target table.
   *
   * @return URI of the target table
   */
  public URI getUri() {
    return uri;
  }

  /**
   * Returns the name of the target table.
   *
   * @return target table name
   */
  public String getTableName() {
    return this.tableName;
  }

  /**
   * Returns a serialized protocol buffer message object.
   *
   * @return serialized message
   */
  @Override
  public abstract FragmentProto getProto();

  /**
   * Returns a start key of the data range.
   *
   * @return start key
   */
  public T getStartKey() {
    return startKey;
  }

  /**
   * Returns an end key of the data range.
   *
   * @return end key
   */
  public T getEndKey() {
    return endKey;
  }

  /**
   * Returns the length of the data range.
   *
   * @return length of the range
   */
  public long getLength() {
    return length;
  }

  /**
   * Returns host names which has the part of the table specified in this fragment.
   *
   * @return host names
   */
  public String[] getHostNames() {
    return hostNames;
  }

  /**
   * Indicates the fragment is empty or not.
   *
   * @return true if the length is 0. Otherwise, false.
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
    clone.tableName = this.tableName;
    clone.startKey = this.startKey;
    clone.endKey = this.endKey;
    clone.hostNames = this.hostNames;
    clone.length = this.length;
    return clone;
  }
}
