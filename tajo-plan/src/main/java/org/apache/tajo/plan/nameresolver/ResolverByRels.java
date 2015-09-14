/*
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

package org.apache.tajo.plan.nameresolver;

import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.exception.AmbiguousTableException;
import org.apache.tajo.exception.UndefinedColumnException;
import org.apache.tajo.exception.UndefinedTableException;
import org.apache.tajo.exception.AmbiguousColumnException;
import org.apache.tajo.plan.LogicalPlan;

public class ResolverByRels extends NameResolver {
  @Override
  public Column resolve(LogicalPlan plan,
                        LogicalPlan.QueryBlock block,
                        ColumnReferenceExpr columnRef,
                        boolean includeSeflDescTable)
      throws AmbiguousColumnException, AmbiguousTableException, UndefinedColumnException, UndefinedTableException {

    Column column = resolveFromRelsWithinBlock(plan, block, columnRef, includeSeflDescTable);
    if (column != null) {
      return column;
    }

    throw new UndefinedColumnException(columnRef.getCanonicalName());
  }
}
