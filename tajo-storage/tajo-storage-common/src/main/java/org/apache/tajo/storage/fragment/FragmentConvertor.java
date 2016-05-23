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

package org.apache.tajo.storage.fragment;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.exception.TajoInternalError;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

@ThreadSafe
public class FragmentConvertor {

  private static final Map<String, FragmentSerde> SERDE_MAP = Maps.newConcurrentMap();

  private static FragmentSerde getFragmentSerde(Configuration conf, String fragmentKind) {
    fragmentKind = fragmentKind.toLowerCase();
    FragmentSerde serde = SERDE_MAP.get(fragmentKind);
    if (serde == null) {
      Class<? extends FragmentSerde> serdeClass = conf.getClass(
          String.format("tajo.storage.fragment.serde.%s", fragmentKind), null, FragmentSerde.class);
      try {
        serde = serdeClass.getConstructor(null).newInstance();
      } catch (InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | NoSuchMethodException e) {
        throw new TajoInternalError(e);
      }
      SERDE_MAP.put(fragmentKind, serde);
    }

    if (serde == null) {
      throw new TajoInternalError("No such a serde for " + fragmentKind);
    }

    return serde;
  }

  public static <T extends Fragment> T convert(Configuration conf, String fragmentKind, FragmentProto fragment) {
    FragmentSerde serde = getFragmentSerde(conf, fragmentKind);
    try {
      return (T) serde.deserialize(
          serde.newBuilder()
              .mergeFrom(fragment.getContents())
              .build());
    } catch (InvalidProtocolBufferException e) {
      throw new TajoInternalError(e);
    }
  }

  public static <T extends Fragment> T convert(Configuration conf, FragmentProto fragment) {
    return convert(conf, fragment.getKind(), fragment);
  }

  public static <T extends Fragment> List<T> convert(Configuration conf, FragmentProto...fragments) {
    List<T> list = Lists.newArrayList();
    if (fragments == null) {
      return list;
    }
    for (FragmentProto proto : fragments) {
      list.add(convert(conf, proto));
    }
    return list;
  }

  public static FragmentProto toFragmentProto(Configuration conf, Fragment fragment) {
    FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
    fragmentBuilder.setId(fragment.getInputSourceId());
    fragmentBuilder.setKind(fragment.getKind());
    fragmentBuilder.setContents(getFragmentSerde(conf, fragment.getKind()).serialize(fragment).toByteString());
    return fragmentBuilder.build();
  }

  public static List<FragmentProto> toFragmentProtoList(Configuration conf, Fragment... fragments) {
    List<FragmentProto> list = Lists.newArrayList();
    if (fragments == null) {
      return list;
    }
    for (Fragment fragment : fragments) {
      list.add(toFragmentProto(conf, fragment));
    }
    return list;
  }

  public static FragmentProto [] toFragmentProtoArray(Configuration conf, Fragment... fragments) {
    List<FragmentProto> list = toFragmentProtoList(conf, fragments);
    return list.toArray(new FragmentProto[list.size()]);
  }
}
