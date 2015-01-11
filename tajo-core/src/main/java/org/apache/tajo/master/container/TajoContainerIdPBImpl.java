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

package org.apache.tajo.master.container;


import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;

import com.google.common.base.Preconditions;
import org.apache.tajo.ipc.ContainerProtocol;
import org.apache.tajo.master.container.TajoContainerId;

/**
 * This class is borrowed from the following source code :
 * ${hadoop-yarn-common}/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/ContainerIdPBImpl.java
 *
 */
@Private
@Unstable
public class TajoContainerIdPBImpl extends TajoContainerId {
  ContainerProtocol.TajoContainerIdProto proto = null;
  ContainerProtocol.TajoContainerIdProto.Builder builder = null;
  private ApplicationAttemptId applicationAttemptId = null;

  public TajoContainerIdPBImpl() {
    builder = ContainerProtocol.TajoContainerIdProto.newBuilder();
  }

  public TajoContainerIdPBImpl(ContainerProtocol.TajoContainerIdProto proto) {
    this.proto = proto;
    this.applicationAttemptId = convertFromProtoFormat(proto.getAppAttemptId());
  }

  public ContainerProtocol.TajoContainerIdProto getProto() {
    return proto;
  }

  @Override
  public int getId() {
    Preconditions.checkNotNull(proto);
    return proto.getId();
  }

  @Override
  protected void setId(int id) {
    Preconditions.checkNotNull(builder);
    builder.setId((id));
  }


  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.applicationAttemptId;
  }

  @Override
  protected void setApplicationAttemptId(ApplicationAttemptId atId) {
    if (atId != null) {
      Preconditions.checkNotNull(builder);
      builder.setAppAttemptId(convertToProtoFormat(atId));
    }
    this.applicationAttemptId = atId;
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
    ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(
    ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}  

