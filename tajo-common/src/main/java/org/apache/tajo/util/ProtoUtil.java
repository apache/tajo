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

package org.apache.tajo.util;

import com.google.common.collect.Lists;
import org.apache.tajo.common.ProtoObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.*;

public class ProtoUtil {
  public static final BoolProto TRUE = BoolProto.newBuilder().setValue(true).build();
  public static final BoolProto FALSE = BoolProto.newBuilder().setValue(false).build();

  public static final NullProto NULL_PROTO = NullProto.newBuilder().build();

  public static StringProto convertString(String value) {
    return StringProto.newBuilder().setValue(value).build();
  }

  public static StringListProto convertStrings(Collection<String> strings) {
    return StringListProto.newBuilder().addAllValues(strings).build();
  }

  public static Collection<String> convertStrings(StringListProto strings) {
    return strings.getValuesList();
  }

  public static Map<String, String> convertToMap(KeyValueSetProto proto) {
    Map<String, String> keyVals = new HashMap<>();
    for(KeyValueProto keyval : proto.getKeyvalList()) {
      keyVals.put(keyval.getKey(), keyval.getValue());
    }
    return keyVals;
  }

  public static KeyValueSetProto convertFromMap(Map<String, String> map) {
    return new KeyValueSet(map).getProto();
  }

  /**
   * It converts an array of ProtoObjects into Iteratable one.
   *
   * @param protoObjects
   * @param <T>
   * @return
   */
  public static <T> Iterable<T> toProtoObjects(ProtoObject[] protoObjects) {
    List<T> converted = Lists.newArrayList();
    for (ProtoObject protoObject : protoObjects) {
      converted.add((T) protoObject.getProto());
    }
    return converted;
  }
}
