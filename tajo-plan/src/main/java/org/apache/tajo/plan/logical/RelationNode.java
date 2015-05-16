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

package org.apache.tajo.plan.logical;

import org.apache.tajo.catalog.Schema;

/**
 * It provides a logical view of a relation. Regarding a table, the main difference between a logical view and a
 * physical view is as follows:
 *
 * <ul>
 * <li>In logical view, each column in the table has qualified name by table alias name. In addition, the schema of
 * logical view will includes partition columns if we use column-partitioned tables.</li>
 * <li>In contrast, in physical view: each column in the table has qualified name by the original table.</li>
 * </ul>
 */
public abstract class RelationNode extends LogicalNode {

  protected RelationNode(int pid, NodeType nodeType) {
    super(pid, nodeType);
    assert(nodeType == NodeType.SCAN || nodeType == NodeType.PARTITIONS_SCAN || nodeType == NodeType.TABLE_SUBQUERY);
  }

  public abstract boolean hasAlias();

  public abstract String getAlias();

  public abstract String getTableName();

  /**
   * Return a full qualified table name (i.e., dbname.table_name)
   *
   * @return A full qualified table name
   */
  public abstract String getCanonicalName();

  /**
   * Return a logical schema, meaning physically stored columns and virtual columns.
   * Since partition keys in the column partition are not physically stored to files or tables,
   * we call the partition keys virtual columns.
   *
   * @return A logical schema
   */
  public abstract Schema getLogicalSchema();
}
