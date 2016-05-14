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

import com.google.protobuf.Message;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.type.Protobuf;

@Deprecated
public class ProtobufDatum extends Datum {
  private final Message value;

  public ProtobufDatum(Message message) {
    super(new Protobuf(null));
    this.value = message;
  }

  public Message get() {
    return value;
  }

  @Override
  public byte [] asByteArray() {
    return value.toByteArray();
  }

  @Override
  public int size() {
    return value.getSerializedSize();
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.kind() == TajoDataTypes.Type.PROTOBUF) {
      return value.equals(((ProtobufDatum)datum).get()) ? 0 : -1;
    } else {
      return -1;
    }
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ProtobufDatum) {
      ProtobufDatum another = (ProtobufDatum) object;
      return value.equals(another.value);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
