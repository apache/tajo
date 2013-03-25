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

package tajo.storage.hcfile;

import tajo.catalog.proto.CatalogProtos.*;
import tajo.catalog.proto.CatalogProtos.ColumnMetaProto.Builder;
import tajo.common.ProtoObject;

public class HCFileMetaImpl implements ColumnMeta, ProtoObject<ColumnMetaProto> {
  private ColumnMetaProto proto = ColumnMetaProto.getDefaultInstance();
  private Builder builder = null;
  private boolean viaProto = false;

  private Long startRid;
  private Integer recordNum;
  private Integer offsetToIndex;
  private DataType dataType;
  private StoreType storeType;
  private CompressType compType;
  private Boolean compressed;
  private Boolean sorted;
  private Boolean contiguous;

  public HCFileMetaImpl() {
    setModified();
    storeType = StoreType.HCFILE;
  }

  public HCFileMetaImpl(long startRid, DataType dataType,
                        CompressType compType, boolean compressed,
                        boolean sorted, boolean contiguous) {
    this();
    this.startRid = startRid;
    this.dataType = dataType;
    this.compType = compType;
    this.compressed = compressed;
    this.sorted = sorted;
    this.contiguous = contiguous;
  }

  public HCFileMetaImpl(ColumnMetaProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  public void setStartRid(Long startRid) {
    setModified();
    this.startRid = startRid;
  }

  public void setRecordNum(Integer recordNum) {
    setModified();
    this.recordNum = recordNum;
  }

  public void setOffsetToIndex(Integer offsetToIndex) {
    setModified();
    this.offsetToIndex = offsetToIndex;
  }

  public void setDataType(DataType dataType) {
    setModified();
    this.dataType = dataType;
  }

  public void setCompressed(Boolean compressed) {
    setModified();
    this.compressed = compressed;
  }

  public void setSorted(Boolean sorted) {
    setModified();
    this.sorted = sorted;
  }

  public void setContiguous(Boolean contiguous) {
    setModified();
    this.contiguous = contiguous;
  }

  public void setCompType(CompressType compType) {
    setModified();
    this.compType = compType;
  }

  public Long getStartRid() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (startRid != null) {
      return this.startRid;
    }
    if (!p.hasStartRid()) {
      return null;
    }
    this.startRid = p.getStartRid();

    return startRid;
  }

  public Integer getRecordNum() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (recordNum != null) {
      return this.recordNum;
    }
    if (!p.hasRecordNum()) {
      return null;
    }
    this.recordNum = p.getRecordNum();
    return recordNum;
  }

  public Integer getOffsetToIndex() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (offsetToIndex != null) {
      return this.offsetToIndex;
    }
    if (!p.hasOffsetToIndex()) {
      return null;
    }
    this.offsetToIndex = p.getOffsetToIndex();
    return offsetToIndex;
  }

  @Override
  public StoreType getStoreType() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (storeType != null) {
      return this.storeType;
    }
    if (!p.hasStoreType()) {
      return null;
    }
    storeType = p.getStoreType();
    return this.storeType;
  }

  @Override
  public DataType getDataType() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (dataType != null) {
      return dataType;
    }
    if (!p.hasDataType()) {
      return null;
    }
    dataType = p.getDataType();
    return dataType;
  }

  @Override
  public CompressType getCompressType() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (compType != null) {
      return compType;
    }
    if (!p.hasCompType()) {
      return null;
    }
    compType = p.getCompType();
    return compType;
  }

  @Override
  public boolean isCompressed() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (compressed != null) {
      return compressed;
    }
    if (!p.hasCompressed()) {
      return false;
    }

    compressed = p.getCompressed();
    return compressed;
  }

  @Override
  public boolean isSorted() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (sorted != null) {
      return sorted;
    }
    if (!p.hasSorted()) {
      return false;
    }

    sorted = p.getSorted();
    return sorted;
  }

  @Override
  public boolean isContiguous() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (contiguous != null) {
      return contiguous;
    }
    if (!p.hasContiguous()) {
      return false;
    }
    contiguous = p.getContiguous();
    return contiguous;
  }

  @Override
  public void initFromProto() {
    ColumnMetaProtoOrBuilder p = viaProto ? proto : builder;
    if (this.startRid == null && p.hasStartRid()) {
      this.startRid = p.getStartRid();
    }
    if (this.recordNum == null && p.hasRecordNum()) {
      this.recordNum = p.getRecordNum();
    }
    if (this.offsetToIndex == null && p.hasOffsetToIndex()) {
      this.offsetToIndex = p.getOffsetToIndex();
    }
    if (this.dataType == null && p.hasDataType()) {
      this.dataType = p.getDataType();
    }
    if (this.compressed == null && p.hasCompressed()) {
      this.compressed = p.getCompressed();
    }
    if (this.sorted == null && p.hasSorted()) {
      this.sorted = p.getSorted();
    }
    if (this.contiguous == null && p.hasContiguous()) {
      this.contiguous = p.getContiguous();
    }
    if (this.storeType == null && p.hasStoreType()) {
      this.storeType = p.getStoreType();
    }
    if (this.compType == null && p.hasCompType()) {
      this.compType = p.getCompType();
    }
  }

  @Override
  public ColumnMetaProto getProto() {
    if(!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }

    return proto;
  }

  private void setModified() {
    if (viaProto && builder == null) {
      builder = ColumnMetaProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (builder == null) {
      builder = ColumnMetaProto.newBuilder(proto);
    }
    if (this.startRid != null) {
      builder.setStartRid(startRid);
    }
    if (this.recordNum != null) {
      builder.setRecordNum(recordNum);
    }
    if (this.offsetToIndex != null) {
      builder.setOffsetToIndex(offsetToIndex);
    }
    if (this.dataType != null) {
      builder.setDataType(dataType);
    }
    if (this.compressed != null) {
      builder.setCompressed(compressed);
    }
    if (this.sorted != null) {
      builder.setSorted(sorted);
    }
    if (this.contiguous != null) {
      builder.setContiguous(contiguous);
    }
    if (this.storeType != null) {
      builder.setStoreType(storeType);
    }
    if (this.compType != null) {
      builder.setCompType(compType);
    }
  }
}
