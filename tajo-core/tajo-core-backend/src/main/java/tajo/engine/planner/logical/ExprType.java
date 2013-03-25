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
package tajo.engine.planner.logical;

public enum ExprType {
  BST_INDEX_SCAN,
  CREATE_INDEX,
  CREATE_TABLE,	
	DESC_TABLE,
	EXCEPT,
	EXPRS,
	GROUP_BY,
	INSERT_INTO,
	INTERSECT,
  LIMIT,
	JOIN,
	PROJECTION,
	RECEIVE,
	RENAME,
	ROOT,
	SCAN,
	SELECTION,
	SEND,
	SET_DIFF, 
	SET_UNION,
  SET_INTERSECT,
	SHOW_TABLE,
	SHOW_FUNCTION,
	SORT,
	STORE,
	UNION
}
