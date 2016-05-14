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

package org.apache.tajo.storage.jdbc;

import com.google.protobuf.GeneratedMessage.Builder;
import org.apache.tajo.storage.fragment.FragmentSerdeHelper;
import org.apache.tajo.storage.jdbc.JdbcFragmentProtos.JdbcFragmentProto;

import java.net.URI;

public class JdbcFragmentSerdeHelper implements FragmentSerdeHelper<JdbcFragment, JdbcFragmentProto> {

  @Override
  public Builder newBuilder() {
    return JdbcFragmentProto.newBuilder();
  }

  @Override
  public JdbcFragmentProto serialize(JdbcFragment fragment) {
    return JdbcFragmentProto.newBuilder()
        .setInputSourceId(fragment.getInputSourceId())
        .setUri(fragment.getUri().toASCIIString())
        .addAllHosts(fragment.getHostNames())
        .build();
  }

  @Override
  public JdbcFragment deserialize(JdbcFragmentProto proto) {
    return new JdbcFragment(proto.getInputSourceId(), URI.create(proto.getUri()), proto.getHostsList());
  }
}
