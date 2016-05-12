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
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.hbase.StorageFragmentProtos.HBaseFragmentProto;

import java.net.URI;

public class HBaseFragment extends Fragment<byte[]> implements Comparable<HBaseFragment> {
  private String hbaseTableName;
  private String regionLocation;
  private boolean last;

  public HBaseFragment(URI uri, String tableName, String hbaseTableName, byte[] startRow, byte[] stopRow,
                       String regionLocation) {
    this.uri = uri;
    this.tableName = tableName;
    this.hbaseTableName = hbaseTableName;
    this.startKey = startRow;
    this.endKey = stopRow;
    this.regionLocation = regionLocation;
    this.last = false;
  }

  @Override
  public int compareTo(HBaseFragment t) {
    return Bytes.compareTo(startKey, t.startKey);
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
    return startKey == null || endKey == null;
  }

  @Override
  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  @Override
  public String[] getHostNames() {
    return new String[] {regionLocation};
  }

  public Object clone() throws CloneNotSupportedException {
    HBaseFragment frag = (HBaseFragment) super.clone();
    frag.hbaseTableName = hbaseTableName;
    frag.regionLocation = regionLocation;
    frag.last = last;
    return frag;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof HBaseFragment) {
      HBaseFragment t = (HBaseFragment) o;
      if (tableName.equals(t.tableName)
          && Bytes.equals(startKey, t.startKey)
          && Bytes.equals(endKey, t.endKey)) {
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
            ", \"startRow\": \"" + new String(startKey) + "\"" +
            ", \"stopRow\": \"" + new String(endKey) + "\"" +
            ", \"length\": \"" + length + "\"}" ;
  }

  @Override
  public FragmentProto getProto() {
    HBaseFragmentProto.Builder builder = HBaseFragmentProto.newBuilder();
    builder
        .setUri(uri.toString())
        .setTableName(tableName)
        .setHbaseTableName(hbaseTableName)
        .setStartRow(ByteString.copyFrom(startKey))
        .setStopRow(ByteString.copyFrom(endKey))
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
  public byte[] getStartKey() {
    return startKey;
  }

  @Override
  public byte[] getEndKey() {
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
    this.startKey = startRow;
  }

  public void setStopRow(byte[] stopRow) {
    this.endKey = stopRow;
  }
}
