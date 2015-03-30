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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.CacheHolder.BasicCacheHolder;
import org.apache.tajo.engine.utils.TableCache;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.Deallocatable;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HashedTableLoader {

  private final TaskAttemptContext context;
  private final PhysicalExec child;
  private final int[] keyIndex;

  private final ExecutionBlockId blockId;
  private final ScanExec scanExec;

  HashedTableLoader(TaskAttemptContext context, PhysicalExec child, int[] keyIndex) throws IOException {
    this.context = context;
    this.child = child;
    this.keyIndex = keyIndex;
    this.blockId = context.getTaskId().getTaskId().getExecutionBlockId();
    this.scanExec = PhysicalPlanUtil.findExecutor(child, ScanExec.class);
  }
  
  HashedTable loadTable() throws IOException {
    if (scanExec.canBroadcast()) {
      /* If this table can broadcast, all tasks in a node will share the same cache */
      TableCacheKey key = getCacheKey(context, scanExec.getCanonicalName(), scanExec.getFragments());
      return loadFromCache(key);
    }
    Iterator<Tuple> tuples = TupleUtil.tupleIterator(child, context);
    return new HashedTable(load(tuples, keyIndex), null);
  }

  private HashedTable loadFromCache(TableCacheKey key) throws IOException {
    final TableCache tableCache = TableCache.getInstance();
    CacheHolder<HashedTable> holder;
    synchronized (tableCache.getLock()) {
      if (!tableCache.hasCache(key)) {
        Iterator<Tuple> tuples = TupleUtil.tupleIterator(child, context);
        tableCache.addCache(key, holder = new HashedTableHolder(tuples, keyIndex));
      } else {
        holder = tableCache.getCache(key);
      }
    }

    try {
      return holder.acquire();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private TableCacheKey getCacheKey(TaskAttemptContext ctx, String canonicalName,
                                    CatalogProtos.FragmentProto[] fragments) throws IOException {
    String pathNameKey = "";
    if (fragments != null) {
      StringBuilder stringBuilder = new StringBuilder();
      for (CatalogProtos.FragmentProto f : fragments) {
        Fragment fragment = FragmentConvertor.convert(ctx.getConf(), f);
        stringBuilder.append(fragment.getKey());
      }
      pathNameKey = stringBuilder.toString();
    }

    return new TableCacheKey(blockId.toString(), canonicalName, pathNameKey);
  }

  public void release(HashedTable hashedTable) {
    if (scanExec.canBroadcast()) {
      TableCache.getInstance().releaseCache(blockId);
    } else {
      hashedTable.clear();
    }
  }

  public static class HashedTable {
    final Map<Tuple, List<Tuple>> tupleSlots;
    final Deallocatable rowBlock;

    HashedTable(Map<Tuple, List<Tuple>> tupleSlots, Deallocatable rowBlock) {
      this.tupleSlots = tupleSlots;
      this.rowBlock = rowBlock;
    }

    public void clear() {
      tupleSlots.clear();
      if (rowBlock != null) {
        rowBlock.release();
      }
    }
  }

  public static class HashedTableHolder extends BasicCacheHolder<HashedTable> {

    private final Iterator<Tuple> iterator;
    private final int[] keyIndex;

    HashedTableHolder(Iterator<Tuple> iterator, int[] keyIndex) {
      this.iterator = iterator;
      this.keyIndex = keyIndex;
    }

    @Override
    protected HashedTable init() throws IOException {
      return new HashedTable(load(iterator, keyIndex), null);
    }

    @Override
    protected void clear(HashedTable cached) {
      if (cached != null) {
        cached.clear();
      }
    }
  }

  private static Map<Tuple, List<Tuple>> load(Iterator<Tuple> iterator, int[] keyIndex)
      throws IOException {

    Map<Tuple, List<Tuple>> map = new HashMap<Tuple, List<Tuple>>(100000);

    while (iterator.hasNext()) {
      Tuple tuple;
      try {
        tuple = iterator.next();
      } catch (RuntimeException e) {
        throw new IOException(e.getCause());
      }
      Tuple keyTuple = new VTuple(keyIndex.length);
      for (int i = 0; i < keyIndex.length; i++) {
        keyTuple.put(i, tuple.get(keyIndex[i]));
      }

      List<Tuple> newValue = map.get(keyTuple);

      if (newValue != null) {
        newValue.add(tuple);
      } else {
        newValue = new ArrayList<Tuple>();
        newValue.add(tuple);
        map.put(keyTuple, newValue);
      }
    }
    return map;
  }
}
