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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.jdbc.JdbcFragmentProtos.JdbcFragmentProto;

import java.util.Arrays;

public class JdbcFragment implements Fragment, Comparable<JdbcFragment>, Cloneable {
  String uri;
  String inputSourceId;
  String [] hostNames;


  public JdbcFragment(ByteString raw) throws InvalidProtocolBufferException {
    JdbcFragmentProto.Builder builder = JdbcFragmentProto.newBuilder();
    builder.mergeFrom(raw);
    builder.build();
    init(builder.build());
  }

  public JdbcFragment(String inputSourceId, String uri) {
    this.inputSourceId = inputSourceId;
    this.uri = uri;
    this.hostNames = extractHosts(uri);
  }

  private void init(JdbcFragmentProto proto) {
    this.uri = proto.getUri();
    this.inputSourceId = proto.getInputSourceId();
    this.hostNames = proto.getHostsList().toArray(new String [proto.getHostsCount()]);
  }

  private String [] extractHosts(String uri) {
    return new String[] {ConnectionInfo.fromURI(uri).host};
  }

  @Override
  public String getTableName() {
    return inputSourceId;
  }

  public String getUri() {
    return uri;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String getKey() {
    return null;
  }

  @Override
  public String[] getHosts() {
    return hostNames;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public CatalogProtos.FragmentProto getProto() {
    JdbcFragmentProto.Builder builder = JdbcFragmentProto.newBuilder();
    builder.setInputSourceId(this.inputSourceId);
    builder.setUri(this.uri);
    if(hostNames != null) {
      builder.addAllHosts(Arrays.asList(hostNames));
    }

    CatalogProtos.FragmentProto.Builder fragmentBuilder = CatalogProtos.FragmentProto.newBuilder();
    fragmentBuilder.setId(this.inputSourceId);
    fragmentBuilder.setDataFormat("JDBC");
    fragmentBuilder.setContents(builder.buildPartial().toByteString());
    return fragmentBuilder.build();
  }

  @Override
  public int compareTo(JdbcFragment o) {
    return this.uri.compareTo(o.uri);
  }
}
