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
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.jdbc.JdbcFragmentProtos.JdbcFragmentProto;

import java.net.URI;
import java.util.Arrays;

public class JdbcFragment extends Fragment<Long> implements Comparable<JdbcFragment> {

  public JdbcFragment(String inputSourceId, URI uri) {
    this.tableName = inputSourceId;
    this.uri = uri;
    this.hostNames = extractHosts(uri);
  }

  private String [] extractHosts(URI uri) {
    return new String[] {ConnectionInfo.fromURI(uri).host};
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String[] getHostNames() {
    return hostNames;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public CatalogProtos.FragmentProto getProto() {
    JdbcFragmentProto.Builder builder = JdbcFragmentProto.newBuilder();
    builder.setInputSourceId(this.tableName);
    builder.setUri(this.uri.toASCIIString());
    if(hostNames != null) {
      builder.addAllHosts(Arrays.asList(hostNames));
    }

    CatalogProtos.FragmentProto.Builder fragmentBuilder = CatalogProtos.FragmentProto.newBuilder();
    fragmentBuilder.setId(this.tableName);
    fragmentBuilder.setDataFormat("JDBC");
    fragmentBuilder.setContents(builder.buildPartial().toByteString());
    return fragmentBuilder.build();
  }

  @Override
  public long getLength() {
    return TajoConstants.UNKNOWN_LENGTH;
  }

  @Override
  public Long getStartKey() {
    return null;
  }

  @Override
  public Long getEndKey() {
    return null;
  }

  @Override
  public int compareTo(JdbcFragment o) {
    return this.uri.compareTo(o.uri);
  }
}
