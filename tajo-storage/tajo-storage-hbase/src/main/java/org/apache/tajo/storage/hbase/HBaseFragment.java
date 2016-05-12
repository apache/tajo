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

package org.apache.tajo.storage.hbase;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.HBaseFragmentKey;
import org.apache.tajo.storage.hbase.StorageFragmentProtos.HBaseFragmentProto;

import java.net.URI;

public class HBaseFragment implements Fragment, Comparable<HBaseFragment>, Cloneable {
  @Expose
  private URI uri;
  @Expose
  private String tableName;
  @Expose
  private String hbaseTableName;
  @Expose
  private HBaseFragmentKey startKey;
  @Expose
  private HBaseFragmentKey endKey;
  @Expose
  private String regionLocation;
  @Expose
  private boolean last;
  @Expose
  private long length;

  public HBaseFragment(URI uri, String tableName, String hbaseTableName, byte[] startRow, byte[] stopRow,
                       String regionLocation) {
    this.uri = uri;
    this.tableName = tableName;
    this.hbaseTableName = hbaseTableName;
    this.startKey = new HBaseFragmentKey(startRow);
    this.endKey = new HBaseFragmentKey(stopRow);
    this.regionLocation = regionLocation;
    this.last = false;
  }

  public HBaseFragment(ByteString raw) throws InvalidProtocolBufferException {
    HBaseFragmentProto.Builder builder = HBaseFragmentProto.newBuilder();
    builder.mergeFrom(raw);
    builder.build();
    init(builder.build());
  }

  private void init(HBaseFragmentProto proto) {
    this.uri = URI.create(proto.getUri());
    this.tableName = proto.getTableName();
    this.hbaseTableName = proto.getHbaseTableName();
    this.startKey = new HBaseFragmentKey(proto.getStartRow().toByteArray());
    this.endKey = new HBaseFragmentKey(proto.getStopRow().toByteArray());
    this.regionLocation = proto.getRegionLocation();
    this.length = proto.getLength();
    this.last = proto.getLast();
  }

  @Override
  public int compareTo(HBaseFragment t) {
    return Bytes.compareTo(startKey.getKey(), t.startKey.getKey());
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public boolean isEmpty() {
    return startKey.isEmpty() || endKey.isEmpty();
  }

  @Override
  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  @Override
  public String[] getHosts() {
    return new String[] {regionLocation};
  }

  public Object clone() throws CloneNotSupportedException {
    HBaseFragment frag = (HBaseFragment) super.clone();
    frag.uri = uri;
    frag.tableName = tableName;
    frag.hbaseTableName = hbaseTableName;
    frag.startKey = new HBaseFragmentKey(startKey.getKey());
    frag.endKey = new HBaseFragmentKey(endKey.getKey());
    frag.regionLocation = regionLocation;
    frag.last = last;
    frag.length = length;
    return frag;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof HBaseFragment) {
      HBaseFragment t = (HBaseFragment) o;
      if (tableName.equals(t.tableName)
          && Bytes.equals(startKey.getKey(), t.startKey.getKey())
          && Bytes.equals(endKey.getKey(), t.endKey.getKey())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, hbaseTableName, startKey, endKey);
  }

  @Override
  public String toString() {
    return
        "\"fragment\": {\"uri:\"" + uri.toString() +"\", \"tableName\": \""+ tableName +
            "\", hbaseTableName\": \"" + hbaseTableName + "\"" +
            ", \"startRow\": \"" + new String(startKey.getKey()) + "\"" +
            ", \"stopRow\": \"" + new String(endKey.getKey()) + "\"" +
            ", \"length\": \"" + length + "\"}" ;
  }

  @Override
  public FragmentProto getProto() {
    HBaseFragmentProto.Builder builder = HBaseFragmentProto.newBuilder();
    builder
        .setUri(uri.toString())
        .setTableName(tableName)
        .setHbaseTableName(hbaseTableName)
        .setStartRow(ByteString.copyFrom(startKey.getKey()))
        .setStopRow(ByteString.copyFrom(endKey.getKey()))
        .setLast(last)
        .setLength(length)
        .setRegionLocation(regionLocation);

    FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
    fragmentBuilder.setId(this.tableName);
    fragmentBuilder.setContents(builder.buildPartial().toByteString());
    fragmentBuilder.setDataFormat(BuiltinStorages.HBASE);
    return fragmentBuilder.build();
  }

  @Override
  public HBaseFragmentKey getStartKey() {
    return startKey;
  }

  @Override
  public HBaseFragmentKey getEndKey() {
    return endKey;
  }

  public String getRegionLocation() {
    return regionLocation;
  }

  public boolean isLast() {
    return last;
  }

  public void setLast(boolean last) {
    this.last = last;
  }

  public String getHbaseTableName() {
    return hbaseTableName;
  }

  public void setHbaseTableName(String hbaseTableName) {
    this.hbaseTableName = hbaseTableName;
  }

  public void setStartRow(byte[] startRow) {
    this.startKey.setKey(startRow);
  }

  public void setStopRow(byte[] stopRow) {
    this.endKey.setKey(stopRow);
  }
}
