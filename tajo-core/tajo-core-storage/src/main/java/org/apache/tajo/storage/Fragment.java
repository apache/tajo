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

package org.apache.tajo.storage;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.storage.json.StorageGsonHelper;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.Arrays;

public class Fragment implements TableDesc, Comparable<Fragment>, SchemaObject, GsonObject {
  protected FragmentProto.Builder builder = null;

  @Expose private String tableName; // required
  @Expose private Path uri; // required
  @Expose private TableMeta meta; // required
  @Expose private Long startOffset; // required
  @Expose private Long length; // required
  @Expose private boolean distCached = false; // optional

  private String[] hosts; // Datanode hostnames
  private int[] hostsBlockCount; // list of block count of hosts
  private int[] diskIds;

  public Fragment() {
    builder = FragmentProto.newBuilder();
  }

  public Fragment(String tableName, Path uri, TableMeta meta, BlockLocation blockLocation, int[] diskIds) throws IOException {
    this();
    TableMeta newMeta = new TableMetaImpl(meta.getProto());
    SchemaProto newSchemaProto = CatalogUtil.getQualfiedSchema(tableName, meta
        .getSchema().getProto());
    newMeta.setSchema(new Schema(newSchemaProto));
    this.set(tableName, uri, newMeta, blockLocation.getOffset(), blockLocation.getLength());
    this.hosts = blockLocation.getHosts();
    this.diskIds = diskIds;
  }

  // Non splittable
  public Fragment(String tableName, Path uri, TableMeta meta, long start, long length, String[] hosts, int[] hostsBlockCount) {
    this();
    TableMeta newMeta = new TableMetaImpl(meta.getProto());
    SchemaProto newSchemaProto = CatalogUtil.getQualfiedSchema(tableName, meta
        .getSchema().getProto());
    newMeta.setSchema(new Schema(newSchemaProto));
    this.set(tableName, uri, newMeta, start, length);
    this.hosts = hosts;
    this.hostsBlockCount = hostsBlockCount;
  }

  public Fragment(String fragmentId, Path path, TableMeta meta, long start, long length) {
    this();
    TableMeta newMeta = new TableMetaImpl(meta.getProto());
    SchemaProto newSchemaProto = CatalogUtil.getQualfiedSchema(fragmentId, meta
        .getSchema().getProto());
    newMeta.setSchema(new Schema(newSchemaProto));
    this.set(fragmentId, path, newMeta, start, length);
  }

  public Fragment(FragmentProto proto) {
    this();
    TableMeta newMeta = new TableMetaImpl(proto.getMeta());
    this.set(proto.getId(), new Path(proto.getPath()), newMeta,
        proto.getStartOffset(), proto.getLength());
    if (proto.hasDistCached() && proto.getDistCached()) {
      distCached = true;
    }
  }

  private void set(String tableName, Path path, TableMeta meta, long start,
      long length) {
    this.tableName = tableName;
    this.uri = path;
    this.meta = meta;
    this.startOffset = start;
    this.length = length;
  }


  /**
   * Get the list of hosts (hostname) hosting this block
   */
  public String[] getHosts() {
    if (hosts == null) {
      this.hosts = new String[0];
    }
    return hosts;
  }

  /**
   * Get the list of hosts block count
   * if a fragment given multiple block, it returned 'host0:3, host1:1 ...'
   */
  public int[] getHostsBlockCount() {
    if (hostsBlockCount == null) {
      this.hostsBlockCount = new int[getHosts().length];
      Arrays.fill(this.hostsBlockCount, 1);
    }
    return hostsBlockCount;
  }

  /**
   * Get the list of Disk Ids
   * Unknown disk is -1. Others 0 ~ N
   */
  public int[] getDiskIds() {
    if (diskIds == null) {
      this.diskIds = new int[getHosts().length];
      Arrays.fill(this.diskIds, -1);
    }
    return diskIds;
  }

  public String getName() {
    return this.tableName;
  }

  @Override
  public void setName(String tableName) {
    this.tableName = tableName;
  }
  
  @Override
  public Path getPath() {
    return this.uri;
  }

  @Override
  public void setPath(Path path) {
    this.uri = path;
  }
  
  public Schema getSchema() {
    return getMeta().getSchema();
  }

  public TableMeta getMeta() {
    return this.meta;
  }

  @Override
  public void setMeta(TableMeta meta) {
    this.meta = meta;
  }

  public Long getStartOffset() {
    return this.startOffset;
  }

  public Long getLength() {
    return this.length;
  }

  public Boolean isDistCached() {
    return this.distCached;
  }

  public void setDistCached() {
    this.distCached = true;
  }

  /**
   * 
   * The offset range of tablets <b>MUST NOT</b> be overlapped.
   * 
   * @param t
   * @return If the table paths are not same, return -1.
   */
  @Override
  public int compareTo(Fragment t) {
    if (getPath().equals(t.getPath())) {
      long diff = this.getStartOffset() - t.getStartOffset();
      if (diff < 0) {
        return -1;
      } else if (diff > 0) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Fragment) {
      Fragment t = (Fragment) o;
      if (getPath().equals(t.getPath())
          && TUtil.checkEquals(t.getStartOffset(), this.getStartOffset())
          && TUtil.checkEquals(t.getLength(), this.getLength())
          && TUtil.checkEquals(t.isDistCached(), this.isDistCached())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, uri, startOffset, length, isDistCached());
  }
  
  public Object clone() throws CloneNotSupportedException {
    Fragment frag = (Fragment) super.clone();
    frag.builder = FragmentProto.newBuilder();
    frag.tableName = tableName;
    frag.uri = uri;
    frag.meta = (TableMeta) (meta != null ? meta.clone() : null);
    frag.distCached = distCached;
    
    return frag;
  }

  @Override
  public String toString() {
    return "\"fragment\": {\"id\": \""+ tableName +"\", \"path\": "
    		+getPath() + "\", \"start\": " + this.getStartOffset() + ",\"length\": "
        + getLength() + ", \"distCached\": " + distCached + "}" ;
  }

  @Override
  public FragmentProto getProto() {
    if (builder == null) {
      builder = FragmentProto.newBuilder();
    }
    builder.setId(this.tableName);
    builder.setStartOffset(this.startOffset);
    builder.setMeta(meta.getProto());
    builder.setLength(this.length);
    builder.setPath(this.uri.toString());
    builder.setDistCached(this.distCached);

    return builder.build();
  }

  @Override
  public String toJson() {
	  Gson gson = StorageGsonHelper.getInstance();
	  return gson.toJson(this, TableDesc.class);
  }
}
