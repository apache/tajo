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
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.exception.TajoInternalError;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

@ThreadSafe
public class FragmentConvertor {
  /**
   * Cache of fragment classes
   */
  protected static final Map<String, Class<? extends Fragment>> CACHED_FRAGMENT_CLASSES = Maps.newConcurrentMap();
  /**
   * Cache of constructors for each class.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = Maps.newConcurrentMap();
  /**
   * default parameter for all constructors
   */
  private static final Class<?>[] DEFAULT_FRAGMENT_PARAMS = { ByteString.class };

  public static Class<? extends Fragment> getFragmentClass(Configuration conf, String storeType)
  throws IOException {
    Class<? extends Fragment> fragmentClass = CACHED_FRAGMENT_CLASSES.get(storeType.toLowerCase());
    if (fragmentClass == null) {
      fragmentClass = conf.getClass(
          String.format("tajo.storage.fragment.%s.class", storeType.toLowerCase()), null, Fragment.class);
      CACHED_FRAGMENT_CLASSES.put(storeType.toLowerCase(), fragmentClass);
    }

    if (fragmentClass == null) {
      throw new IOException("No such a fragment for " + storeType.toLowerCase());
    }

    return fragmentClass;
  }

  public static <T extends Fragment> T convert(Class<T> clazz, FragmentProto fragment) {
    T result;
    try {
      Constructor<T> constructor = (Constructor<T>) CONSTRUCTOR_CACHE.get(clazz);
      if (constructor == null) {
        constructor = clazz.getDeclaredConstructor(DEFAULT_FRAGMENT_PARAMS);
        constructor.setAccessible(true);
        CONSTRUCTOR_CACHE.put(clazz, constructor);
      }
      result = constructor.newInstance(new Object[]{fragment.getContents()});
    } catch (Throwable e) {
      throw new TajoInternalError(e);
    }

    return result;
  }

  public static <T extends Fragment> T convert(Configuration conf, FragmentProto fragment)
      throws IOException {
    Class<T> fragmentClass = (Class<T>) getFragmentClass(conf, fragment.getStoreType().toLowerCase());
    if (fragmentClass == null) {
      throw new IOException("No such a fragment class for " + fragment.getStoreType());
    }
    return convert(fragmentClass, fragment);
  }

  public static <T extends Fragment> List<T> convert(Class<T> clazz, FragmentProto...fragments)
      throws IOException {
    List<T> list = Lists.newArrayList();
    if (fragments == null) {
      return list;
    }
    for (FragmentProto proto : fragments) {
      list.add(convert(clazz, proto));
    }
    return list;
  }

  public static <T extends Fragment> List<T> convert(Configuration conf, FragmentProto...fragments) throws IOException {
    List<T> list = Lists.newArrayList();
    if (fragments == null) {
      return list;
    }
    for (FragmentProto proto : fragments) {
      list.add((T) convert(conf, proto));
    }
    return list;
  }

  public static List<FragmentProto> toFragmentProtoList(Fragment... fragments) {
    List<FragmentProto> list = Lists.newArrayList();
    if (fragments == null) {
      return list;
    }
    for (Fragment fragment : fragments) {
      list.add(fragment.getProto());
    }
    return list;
  }

  public static FragmentProto [] toFragmentProtoArray(Fragment... fragments) {
    List<FragmentProto> list = toFragmentProtoList(fragments);
    return list.toArray(new FragmentProto[list.size()]);
  }
}
