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

public interface Fragment extends ProtoObject<FragmentProto> {

  /**
   * Returns an URI of the target table.
   *
   * @return URI of the target table
   */
  URI getUri();

  /**
   * Returns the name of the target table.
   *
   * @return target table name
   */
  String getTableName();

  /**
   * Returns a serialized protocol buffer message object.
   *
   * @return serialized message
   */
  @Override
  FragmentProto getProto();

  /**
   * Returns a start key of the data range.
   *
   * @param <T> a key class implementing {@link FragmentKey}
   * @return start key
   */
  <T extends FragmentKey> T getStartKey();

  /**
   * Returns an end key of the data range.
   *
   * @param <T> a key class implementing {@link FragmentKey}
   * @return end key
   */
  <T extends FragmentKey> T getEndKey();

  /**
   * Returns the length of the data range.
   *
   * @return length of the range
   */
  long getLength();

  /**
   * Returns host names which has the part of the table specified in this fragment.
   *
   * @return host names
   */
  String[] getHosts();

  /**
   * Indicates the fragment is empty or not.
   *
   * @return true if the length is 0. Otherwise, false.
   */
  boolean isEmpty();
}
