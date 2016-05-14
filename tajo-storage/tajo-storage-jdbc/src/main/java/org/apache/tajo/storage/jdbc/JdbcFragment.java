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

import org.apache.tajo.TajoConstants;
import org.apache.tajo.storage.fragment.BuiltinFragmentKinds;
import org.apache.tajo.storage.fragment.Fragment;

import java.net.URI;
import java.util.List;

public class JdbcFragment extends Fragment<Long> {

//  public JdbcFragment(ByteString raw) throws InvalidProtocolBufferException {
//    JdbcFragmentProto.Builder builder = JdbcFragmentProto.newBuilder();
//    builder.mergeFrom(raw);
//    builder.build();
//    init(builder.build());
//  }

  // TODO: set start and end keys properly
  public JdbcFragment(String inputSourceId, URI uri) {
    super(BuiltinFragmentKinds.JDBC, uri, inputSourceId, null, null, TajoConstants.UNKNOWN_LENGTH, extractHosts(uri));
  }

  public JdbcFragment(String inputSourceId, URI uri, List<String> hostNames) {
    super(BuiltinFragmentKinds.JDBC, uri, inputSourceId, null, null, TajoConstants.UNKNOWN_LENGTH,
        hostNames.toArray(new String[hostNames.size()]));
  }

//  private void init(JdbcFragmentProto proto) {
//    this.uri = URI.create(proto.getUri());
//    this.inputSourceId = proto.getInputSourceId();
//    this.hostNames = proto.getHostsList().toArray(new String [proto.getHostsCount()]);
//  }

  private static String[] extractHosts(URI uri) {
    return new String[] {ConnectionInfo.fromURI(uri).host};
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

//  @Override
//  public CatalogProtos.FragmentProto getProto() {
//    JdbcFragmentProto.Builder builder = JdbcFragmentProto.newBuilder();
//    builder.setInputSourceId(this.inputSourceId);
//    builder.setUri(this.uri.toASCIIString());
//    if(hostNames != null) {
//      builder.addAllHosts(Arrays.asList(hostNames));
//    }
//
//    CatalogProtos.FragmentProto.Builder fragmentBuilder = CatalogProtos.FragmentProto.newBuilder();
//    fragmentBuilder.setId(this.inputSourceId);
//    fragmentBuilder.setDataFormat(BuiltinFragmentKinds.JDBC);
//    fragmentBuilder.setContents(builder.buildPartial().toByteString());
//    return fragmentBuilder.build();
//  }
}
