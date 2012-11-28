/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

import tajo.catalog.proto.CatalogProtos.DataType;

/**
 * Meta information for blocks.
 * BlockMeta is not stored and it is only maintained in the memory
 */
public class HBlockMetaImpl implements BlockMeta {
  private DataType dataType;
  private int recordNum;
  private long startRid;
  private boolean sorted;
  private boolean contiguous;
  private boolean compressed;

  public HBlockMetaImpl() {

  }

  public HBlockMetaImpl(DataType dataType, int recordNum, long startRid,
                        boolean sorted, boolean contiguous, boolean compressed) {
    this.set(dataType, recordNum, startRid, sorted, contiguous, compressed);
  }

  public void set(DataType dataType, int recordNum, long startRid,
                  boolean sorted, boolean contiguous, boolean compressed) {
    this.dataType = dataType;
    this.recordNum = recordNum;
    this.startRid = startRid;
    this.sorted = sorted;
    this.contiguous = contiguous;
    this.compressed = compressed;
  }

  @Override
  public BlockMeta setStartRid(long startRid) {
    this.startRid = startRid;
    return this;
  }

  @Override
  public BlockMeta setRecordNum(int recordNum) {
    this.recordNum = recordNum;
    return this;
  }

  @Override
  public DataType getType() {
    return this.dataType;
  }

  @Override
  public int getRecordNum() {
    return this.recordNum;
  }

  public long getStartRid() {
    return this.startRid;
  }

  @Override
  public boolean isSorted() {
    return this.sorted;
  }

  @Override
  public boolean isContiguous() {
    return this.contiguous;
  }

  @Override
  public boolean isCompressed() {
    return this.compressed;
  }
}
