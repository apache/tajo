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

package org.apache.tajo.engine.planner.nameresolver;

/**
 *
 * <h2>Motivation</h2>
 *
 * Please take a look at the following example query:
 *
 *  <pre>
 *   select (l_orderkey + l_orderkey) l_orderkey from lineitem where l_orderkey > 2 order by l_orderkey;
 * </pre>
 *
 * Although <code>l_orderkey</code> seems to be ambiguous, the above usages are available in commercial DBMSs.
 * In order to eliminate the ambiguity, Tajo follows the behaviors of PostgreSQL.
 *
 * <h2>Resolving Modes</h2>
 *
 * From the behaviors of PostgreSQL, we found that there are three kinds of name resolving modes.
 * Each definition is as follows:
 *
 * <ul>
 *   <li><b>RELS_ONLY</b> finds a column from the relations in the current block.
 *  <li><b>RELS_AND_SUBEXPRS</b> finds a column from the all relations in the current block and
 *  from aliased temporal fields; a temporal field means an explicitly aliased expression. If there are duplicated
 *  columns in the relation and temporal fields, this level firstly chooses the field in a relation.</li>
 *  <li><b>SUBEXPRS_AND_RELS</b> is very similar to <code>RELS_AND_SUBEXPRS</code>. The main difference is that it
 *  firstly chooses an aliased temporal field instead of the fields in a relation.</li>
 * </ul>
 *
 * <h2>The relationship between resolving modes and operators</h3>
 *
 * <ul>
 *   <li>fields in select list and LIMIT are resolved in the REL_ONLY mode.</li>
 *   <li>fields in WHERE clause are resolved in the RELS_AND_SUBEXPRS mode.</li>
 *   <li>fields in GROUP BY, HAVING, and ORDER BY are resolved in the SUBEXPRS_AND_RELS mode.</li>
 * </ul>
 *
 * <h2>Example</h2>
 *
 * Please revisit the aforementioned example:
 *
 * <pre>
 *   select (l_orderkey + l_orderkey) l_orderkey from lineitem where l_orderkey > 2 order by l_orderkey;
 * </pre>
 *
 * With the above rules and the relationship between modes and operators, we can easily identify which reference
 * points to which field.
 * <ol>
 *   <li><code>l_orderkey</code> included in <code>(l_orderkey + l_orderkey)</code> points to the field
 *   in the relation <code>lineitem</code>.</li>
 *   <li><code>l_orderkey</code> included in WHERE clause also points to the field in the relation
 *   <code>lineitem</code>.</li>
 *   <li><code>l_orderkey</code> included in ORDER BY clause points to the temporal field
 *   <code>(l_orderkey + l_orderkey)</code>.</li>
 * </ol>
 */
public enum NameResolvingMode {
  RELS_ONLY,          // finding from only relations
  RELS_AND_SUBEXPRS,  // finding from relations and subexprs in a place
  SUBEXPRS_AND_RELS,  // finding from subexprs and relations in a place
  LEGACY              // Finding in a legacy manner (globally)
}
