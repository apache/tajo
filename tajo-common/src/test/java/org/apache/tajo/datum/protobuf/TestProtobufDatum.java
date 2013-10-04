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

package org.apache.tajo.datum.protobuf;

import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestProtobufDatum {

  @Test
  public void testCreate() throws IOException, NoSuchMethodException, ClassNotFoundException {
    TajoDataTypes.DataType.Builder builder = TajoDataTypes.DataType.newBuilder();
    builder.setType(TajoDataTypes.Type.PROTOBUF);
    builder.setCode(TajoIdProtos.QueryIdProto.class.getName());

    ProtobufDatumFactory factory = ProtobufDatumFactory.get(builder.build());
    TajoIdProtos.QueryIdProto.Builder queryIdBuilder = factory.newBuilder();
    queryIdBuilder.setId(String.valueOf(System.currentTimeMillis()));
    queryIdBuilder.setSeq(1);

    TajoIdProtos.QueryIdProto queryId = queryIdBuilder.build();
    ProtobufDatum datum = factory.createDatum(queryId);

    ProtobufJsonFormat formatter = ProtobufJsonFormat.getInstance();
    String json = formatter.printToString(datum.get());

    TajoIdProtos.QueryIdProto.Builder fromJson = factory.newBuilder();
    formatter.merge(TextUtils.toInputStream(json), fromJson);
    assertEquals(queryId, fromJson.build());
  }
}
