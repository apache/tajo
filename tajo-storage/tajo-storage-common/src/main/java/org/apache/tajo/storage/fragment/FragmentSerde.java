/*
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

import com.google.protobuf.Message;
import com.google.protobuf.GeneratedMessage.Builder;

/**
 * FragmentSerde abstracts how a fragment is serialized / deserialized to / from a Protocol Buffer message.
 *
 * @param <F> Fragment class
 * @param <P> Protocol Buffer Message class corresponding to the Fragment class
 */
public interface FragmentSerde<F extends Fragment, P extends Message> {

  /**
   * Creates a new builder of {@link P}.
   *
   * @return a Protocol Buffer message builder
   */
  Builder newBuilder();

  /**
   * Serializes a fragment into a Protocol Buffer message.
   *
   * @param fragment
   * @return a serialized Protocol Buffer message
   */
  P serialize(F fragment);

  /**
   * Deserializes a Protocol Buffer message to a fragment.
   *
   * @param proto
   * @return a deserialized fragment instance
   */
  F deserialize(P proto);
}
