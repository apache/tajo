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

package org.apache.tajo.worker;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.ResourceProtos.FetchProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.querymaster.Task;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.tajo.plan.serder.PlanProto.ShuffleType;

/**
 * <code>FetchImpl</code> information to indicate the locations of intermediate data.
 */
public class FetchImpl implements ProtoObject<FetchProto>, Cloneable {
  private Task.PullHost host;             // The pull server host information
  private ShuffleType type; // hash or range partition method.
  private ExecutionBlockId executionBlockId;   // The executionBlock id
  private int partitionId;                     // The hash partition id
  private final String name;                   // The intermediate source name
  private RangeParam rangeParam;               // optional, range parameter for range shuffle
  private boolean hasNext = false;             // optional, if true, has more taskIds

  private List<Integer> taskIds;               // repeated, the task ids
  private List<Integer> attemptIds;            // repeated, the attempt ids

  private long offset = -1;
  private long length = -1;

  public static class RangeParam {
    private byte[] start;
    private byte[] end;
    private boolean lastInclusive;

    public RangeParam(TupleRange range, boolean lastInclusive, RowStoreEncoder encoder) {
      this.start = encoder.toBytes(range.getStart());
      this.end = encoder.toBytes(range.getEnd());
      this.lastInclusive = lastInclusive;
    }

    public RangeParam(byte[] start, byte[] end, boolean lastInclusive) {
      this.start = start;
      this.end = end;
      this.lastInclusive = lastInclusive;
    }

    public byte[] getStart() {
      return start;
    }

    public byte[] getEnd() {
      return end;
    }

    public boolean isLastInclusive() {
      return lastInclusive;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(Arrays.hashCode(start), Arrays.hashCode(end), lastInclusive);
    }
  }

  public FetchImpl(FetchProto proto) {
    this(proto.getName(),
        new Task.PullHost(proto.getHost(), proto.getPort()),
        proto.getType(),
        new ExecutionBlockId(proto.getExecutionBlockId()),
        proto.getPartitionId(),
        proto.getHasNext(),
        proto.getTaskIdList(), proto.getAttemptIdList());

    if (proto.hasOffset()) {
      this.offset = proto.getOffset();
    }

    if (proto.hasLength()) {
      this.length = proto.getLength();
    }

    if (proto.hasRangeStart()) {
      this.rangeParam = new RangeParam(proto.getRangeStart().toByteArray(),
          proto.getRangeEnd().toByteArray(), proto.getRangeLastInclusive());
    }
  }

  public FetchImpl(String name, Task.PullHost host, ShuffleType type, ExecutionBlockId executionBlockId,
                   int partitionId) {
    this(name, host, type, executionBlockId, partitionId, null, false, new ArrayList<>(), new ArrayList<>());
  }

  public FetchImpl(String name, Task.PullHost host, ShuffleType type, ExecutionBlockId executionBlockId,
                   int partitionId, List<Task.IntermediateEntry> intermediateEntryList) {
    this(name, host, type, executionBlockId, partitionId, null, false,
            new ArrayList<>(), new ArrayList<>());
    for (Task.IntermediateEntry entry : intermediateEntryList){
      addPart(entry.getTaskId(), entry.getAttemptId());
    }
  }

  public FetchImpl(String name, Task.PullHost host, ShuffleType type, ExecutionBlockId executionBlockId,
                   int partitionId, boolean hasNext,
                   List<Integer> taskIds, List<Integer> attemptIds) {
    this(name, host, type, executionBlockId, partitionId, null, hasNext, taskIds, attemptIds);
  }

  public FetchImpl(String name, Task.PullHost host, ShuffleType type, ExecutionBlockId executionBlockId,
                   int partitionId, RangeParam rangeParam, boolean hasNext,
                   List<Integer> taskIds, List<Integer> attemptIds) {
    this.host = host;
    this.type = type;
    this.executionBlockId = executionBlockId;
    this.partitionId = partitionId;
    this.rangeParam = rangeParam;
    this.hasNext = hasNext;
    this.name = name;
    this.taskIds = taskIds;
    this.attemptIds = attemptIds;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(host, type, executionBlockId, partitionId, name, rangeParam.hashCode(),
        hasNext, taskIds, attemptIds, offset, length);
  }

  @Override
  public FetchProto getProto() {
    FetchProto.Builder builder = FetchProto.newBuilder();

    builder.setHost(host.getHost());
    builder.setPort(host.getPort());
    builder.setType(type);
    builder.setExecutionBlockId(executionBlockId.getProto());
    builder.setPartitionId(partitionId);
    builder.setHasNext(hasNext);
    builder.setName(name);

    if (rangeParam != null) {
      builder.setRangeStart(ByteString.copyFrom(rangeParam.getStart()));
      builder.setRangeEnd(ByteString.copyFrom(rangeParam.getEnd()));
      builder.setRangeLastInclusive(rangeParam.isLastInclusive());
    }

    Preconditions.checkArgument(taskIds.size() == attemptIds.size());

    builder.addAllTaskId(taskIds);
    builder.addAllAttemptId(attemptIds);

    builder.setOffset(offset);
    builder.setLength(length);
    return builder.build();
  }

  public void addPart(int taskId, int attemptId) {
    this.taskIds.add(taskId);
    this.attemptIds.add(attemptId);
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public void setExecutionBlockId(ExecutionBlockId executionBlockId) {
    this.executionBlockId = executionBlockId;
  }

  public void setRangeParams(RangeParam rangeParams) {
    this.rangeParam = rangeParams;
  }

  public boolean hasNext() {
    return hasNext;
  }

  public void setHasNext(boolean hasNext) {
    this.hasNext = hasNext;
  }

  public ShuffleType getType() {
    return type;
  }

  public void setType(ShuffleType type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public FetchImpl clone() throws CloneNotSupportedException {
    FetchImpl newFetchImpl = (FetchImpl) super.clone();

    newFetchImpl.host = host.clone();
    newFetchImpl.type = type;
    newFetchImpl.executionBlockId = executionBlockId;
    newFetchImpl.partitionId = partitionId;
    newFetchImpl.rangeParam = rangeParam;
    newFetchImpl.hasNext = hasNext;
    if (taskIds != null) {
      newFetchImpl.taskIds = Lists.newArrayList(taskIds);
    }
    if (attemptIds != null) {
      newFetchImpl.attemptIds = Lists.newArrayList(attemptIds);
    }
    newFetchImpl.offset = offset;
    newFetchImpl.length = length;
    return newFetchImpl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FetchImpl fetch = (FetchImpl) o;

    return TUtil.checkEquals(hasNext, fetch.hasNext) &&
        TUtil.checkEquals(partitionId, fetch.partitionId) &&
        TUtil.checkEquals(attemptIds, fetch.attemptIds) &&
        TUtil.checkEquals(executionBlockId, fetch.executionBlockId) &&
        TUtil.checkEquals(host, fetch.host) &&
        TUtil.checkEquals(name, fetch.name) &&
        TUtil.checkEquals(rangeParam, fetch.rangeParam) &&
        TUtil.checkEquals(taskIds, fetch.taskIds) &&
        TUtil.checkEquals(type, fetch.type) &&
        TUtil.checkEquals(offset, fetch.offset) &&
        TUtil.checkEquals(length, fetch.length);
  }
}
