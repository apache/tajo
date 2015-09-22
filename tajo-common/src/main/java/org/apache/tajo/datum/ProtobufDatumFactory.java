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

package org.apache.tajo.datum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.protobuf.ProtobufJsonFormat;

import java.lang.reflect.Method;
import java.util.Map;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class ProtobufDatumFactory {
  private static Map<String, ProtobufDatumFactory> factories = Maps.newHashMap();
  private static ProtobufJsonFormat protobufFormatter = ProtobufJsonFormat.getInstance();
  private Method getDefaultInstance;
  Class<? extends GeneratedMessage> messageClass;
  private Message defaultInstance;

  private ProtobufDatumFactory(String protobufClass) {
    try {
      String messageClassName = protobufClass;
      this.messageClass = (Class<? extends GeneratedMessage>) Class.forName(messageClassName);
      this.getDefaultInstance = messageClass.getMethod("getDefaultInstance");
      defaultInstance = (Message) getDefaultInstance.invoke(null);
    } catch (Throwable t) {
      t.printStackTrace();
      throw new RuntimeException(t);
    }
  }

  public <T extends Message.Builder> T newBuilder() {
    Message.Builder builder;
    try {
      builder = defaultInstance.newBuilderForType();
    } catch (Throwable t) {
      t.printStackTrace();
      throw new RuntimeException(t);
    }
    return (T) builder;
  }

  public static ProtobufDatum createDatum(Message.Builder builder) {
    return createDatum(builder.build());
  }

  public static ProtobufDatum createDatum(Message message) {
    return new ProtobufDatum(message);
  }

  public static ProtobufDatum createDatum(String className, byte [] bytes, int offset, int length)
      throws InvalidProtocolBufferException {
    ProtobufDatumFactory factory = get(className);
    Message.Builder builder = factory.newBuilder();
    builder.mergeFrom(bytes, offset, length);
    return createDatum(builder);
  }

  public static Datum createDatum(DataType type, byte[] bytes)
      throws InvalidProtocolBufferException {
    ProtobufDatumFactory factory = get(type);
    Message.Builder builder = factory.newBuilder();
    builder.mergeFrom(bytes);
    return createDatum(builder);
  }

  public static ProtobufDatumFactory get(DataType dataType) {
    Preconditions.checkArgument(dataType.getType() == TajoDataTypes.Type.PROTOBUF,
        "ProtobufDatumFactory only can accepts Protocol Buffer Datum Type.");
    return get(dataType.getCode());
  }

  public static ProtobufDatumFactory get(String className) {
    ProtobufDatumFactory factory;
    if (factories.containsKey(className)) {
      factory = factories.get(className);
    } else {
      factory = new ProtobufDatumFactory(className);
      factories.put(className, factory);
    }
    return factory;
  }

  public static String toJson(Message message) {
    return protobufFormatter.printToString(message);
  }

}
