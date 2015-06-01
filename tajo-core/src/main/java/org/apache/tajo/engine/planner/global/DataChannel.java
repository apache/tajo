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

package org.apache.tajo.engine.planner.global;

import com.google.common.base.Preconditions;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.util.StringUtils;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.*;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import static org.apache.tajo.plan.serder.PlanProto.TransmitType;

public class DataChannel {
  private ExecutionBlockId srcId;
  private ExecutionBlockId targetId;
  private TransmitType transmitType = TransmitType.PULL_TRANSMIT;
  private ShuffleType shuffleType;
  private Integer numOutputs = 1;
  private Column[] shuffleKeys;

  private Schema schema;

  private String storeType = "RAW";

  public DataChannel(ExecutionBlockId srcId, ExecutionBlockId targetId) {
    this.srcId = srcId;
    this.targetId = targetId;
  }

  public DataChannel(ExecutionBlockId srcId, ExecutionBlockId targetId, ShuffleType shuffleType) {
    this(srcId, targetId);
    this.shuffleType = shuffleType;
  }

  public DataChannel(ExecutionBlock src, ExecutionBlock target, ShuffleType shuffleType, int numOutput) {
    this(src.getId(), target.getId(), shuffleType, numOutput);
    setSchema(src.getPlan().getOutSchema());
  }

  public DataChannel(ExecutionBlockId srcId, ExecutionBlockId targetId, ShuffleType shuffleType, int numOutputs) {
    this(srcId, targetId, shuffleType);
    this.numOutputs = numOutputs;
  }

  public DataChannel(DataChannelProto proto) {
    this.srcId = new ExecutionBlockId(proto.getSrcId());
    this.targetId = new ExecutionBlockId(proto.getTargetId());
    this.transmitType = proto.getTransmitType();
    this.shuffleType = proto.getShuffleType();
    if (proto.hasSchema()) {
      this.setSchema(new Schema(proto.getSchema()));
    }
    if (proto.getShuffleKeysCount() > 0) {
      shuffleKeys = new Column[proto.getShuffleKeysCount()];
      for (int i = 0; i < proto.getShuffleKeysCount(); i++) {
        shuffleKeys[i] = new Column(proto.getShuffleKeys(i));
      }
    } else {
      shuffleKeys = new Column[] {};
    }
    if (proto.hasNumOutputs()) {
      this.numOutputs = proto.getNumOutputs();
    }

    if (proto.hasStoreType()) {
      this.storeType = proto.getStoreType();
    }
  }

  public ExecutionBlockId getSrcId() {
    return srcId;
  }

  public ExecutionBlockId getTargetId() {
    return targetId;
  }

  public ShuffleType getShuffleType() {
    return shuffleType;
  }

  public boolean needShuffle() {
    return shuffleType != ShuffleType.NONE_SHUFFLE;
  }

  public TransmitType getTransmitType() {
    return this.transmitType;
  }

  public void setTransmitType(TransmitType transmitType) {
    this.transmitType = transmitType;
  }

  public void setShuffle(ShuffleType shuffleType, Column[] keys, int numOutputs) {
    Preconditions.checkArgument(keys.length >= 0, "At least one shuffle key must be specified.");
    Preconditions.checkArgument(numOutputs > 0, "The number of outputs must be positive: %s", numOutputs);

    this.shuffleType = shuffleType;
    this.shuffleKeys = keys;
    this.numOutputs = numOutputs;
  }

  public void setShuffleType(ShuffleType shuffleType) {
    this.shuffleType = shuffleType;
  }

  public boolean hasShuffleKeys() {
    return shuffleKeys != null;
  }

  public void setShuffleKeys(Column[] key) {
    this.shuffleKeys = key;
  }

  public Column [] getShuffleKeys() {
    return this.shuffleKeys;
  }

  public void setShuffleOutputNum(int partNum) {
    this.numOutputs = partNum;
  }

  public int getShuffleOutputNum() {
    return numOutputs;
  }

  public boolean hasStoreType() {
    return this.storeType != null;
  }

  public void setStoreType(String storeType) {
    this.storeType = storeType;
  }

  public String getStoreType() {
    return storeType;
  }

  public DataChannelProto getProto() {
    DataChannelProto.Builder builder = DataChannelProto.newBuilder();
    builder.setSrcId(srcId.getProto());
    builder.setTargetId(targetId.getProto());
    if (transmitType != null) {
      builder.setTransmitType(transmitType);
    }
    builder.setShuffleType(shuffleType);
    if (schema != null) {
      builder.setSchema(schema.getProto());
    }
    if (shuffleKeys != null) {
      for (Column column : shuffleKeys) {
        builder.addShuffleKeys(column.getProto());
      }
    }
    if (numOutputs != null) {
      builder.setNumOutputs(numOutputs);
    }

    if(storeType != null){
      builder.setStoreType(storeType);
    }
    return builder.build();
  }

  public void setSchema(Schema schema) {
    this.schema = SchemaUtil.clone(schema);
  }

  public Schema getSchema() {
    return schema;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(srcId.getQueryId()).append("] ");
    sb.append(srcId.getId()).append(" => ").append(targetId.getId());
    sb.append(" (type=").append(shuffleType);
    if (hasShuffleKeys()) {
      sb.append(", key=");
      sb.append(StringUtils.join(shuffleKeys));
      sb.append(", num=").append(numOutputs);
    }
    sb.append(")");
    return sb.toString();
  }
}
