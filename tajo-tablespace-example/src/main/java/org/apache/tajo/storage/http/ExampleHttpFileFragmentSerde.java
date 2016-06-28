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

package org.apache.tajo.storage.http;

import com.google.protobuf.GeneratedMessage.Builder;
import org.apache.tajo.storage.fragment.FragmentSerde;
import org.apache.tajo.storage.http.ExampleHttpFragmentProtos.ExampleHttpFileFragmentProto;

import java.net.URI;

public class ExampleHttpFileFragmentSerde
    implements FragmentSerde<ExampleHttpFileFragment, ExampleHttpFileFragmentProto> {

  @Override
  public Builder newBuilder() {
    return ExampleHttpFileFragmentProto.newBuilder();
  }

  @Override
  public ExampleHttpFileFragmentProto serialize(ExampleHttpFileFragment fragment) {
    return ExampleHttpFileFragmentProto.newBuilder()
        .setUri(fragment.getUri().toASCIIString())
        .setTableName(fragment.getInputSourceId())
        .setStartKey(fragment.getStartKey())
        .setEndKey(fragment.getEndKey())
        .build();
  }

  @Override
  public ExampleHttpFileFragment deserialize(ExampleHttpFileFragmentProto proto) {
    return new ExampleHttpFileFragment(
        URI.create(proto.getUri()),
        proto.getTableName(),
        proto.getStartKey(),
        proto.getEndKey()
    );
  }
}
