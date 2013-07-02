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

/**
 * 
 */
package org.apache.tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.logical.IndexWriteNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexWriter;
import org.apache.tajo.util.IndexUtil;

import java.io.IOException;

public class IndexWriteExec extends UnaryPhysicalExec {
  private int [] indexKeys = null;
  private final BSTIndexWriter indexWriter;
  private final TupleComparator comp;

  public IndexWriteExec(final TaskAttemptContext context, final StorageManager sm,
      final IndexWriteNode plan, final Fragment fragment,
      final PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    Preconditions.checkArgument(inSchema.equals(child.getSchema()));

    indexKeys = new int[plan.getSortSpecs().length];
    Schema keySchema = new Schema();
    Column col;
    for (int i = 0 ; i < plan.getSortSpecs().length; i++) {
      col = plan.getSortSpecs()[i].getSortKey();
      indexKeys[i] = inSchema.getColumnId(col.getQualifiedName());
      keySchema.addColumn(inSchema.getColumn(col.getQualifiedName()));
    }
    this.comp = new TupleComparator(keySchema, plan.getSortSpecs());
    
    BSTIndex bst = new BSTIndex(new TajoConf());
    Path dir = new Path(sm.getTablePath(plan.getTableName()) , "index");
    // TODO - to be improved
    this.indexWriter = bst.getIndexWriter(new Path(dir,
        IndexUtil.getIndexNameOfFrag(fragment, plan.getSortSpecs())),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
  }

  @Override
  public Tuple next() throws IOException {
    indexWriter.open();
    Tuple tuple;
    
    while ((tuple = child.next()) != null) {
      Tuple keys = new VTuple(indexKeys.length);
      for (int idx = 0; idx < indexKeys.length; idx++) {
        keys.put(idx, tuple.get(indexKeys[idx]));
      }
      indexWriter.write(keys, tuple.getOffset());
    }
    
    indexWriter.flush();
    indexWriter.close();
    return null;
  }

  @Override
  public void rescan() throws IOException {
  }
}