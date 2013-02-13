/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.frontend.sql;

import tajo.algebra.*;

public class QueryBlock {
  private String relName;

  /* projection */
  private Projection projection;
  /* distinct or all? */
  private boolean distinct = false;
  /* from clause or join clause */
  private Expr tableExpression = null;
  /* where clause */
  private Expr searchCondition = null;
  /* if true, there is at least grouping field. */
  private Aggregation aggregation = null;
  /* having condition */
  private Expr havingCondition = null;
  /* keys for ordering */
  private Sort sort = null;
  /* limit clause */
  private Limit limit = null;

  public QueryBlock() {
  }

  public void setProjection(Projection projection) {
    this.projection = projection;
  }

  public boolean hasTableExpression() {
    return this.tableExpression != null;
  }

  public Expr getTableExpression() {
    return this.tableExpression;
  }

  public void setTableExpression(Expr fromClause) {
    this.tableExpression = fromClause;
  }

  public boolean hasSearchCondition() {
    return searchCondition != null;
  }

  public Expr getSearchCondition() {
    return this.searchCondition;
  }

  public void setSearchCondition(Expr expr) {
    this.searchCondition = expr;
  }

  public boolean hasAggregation() {
    return this.aggregation != null;
  }

  public Aggregation getAggregation() {
    return this.aggregation;
  }

  public void setAggregation(Aggregation groupby) {
    this.aggregation = groupby;
  }

  public boolean hasHavingCondition(Expr expr) {
    return havingCondition != null;
  }

  public Expr getHavingCondition() {
    return havingCondition;
  }

  public void setHavingCondition(Expr expr) {
    havingCondition = expr;
  }

  public boolean hasSort() {
    return sort != null;
  }

  public Sort getSort() {
    return sort;
  }

  public void setSort(Sort sort) {
    this.sort = sort;
  }

  public boolean hasLimit() {
    return limit != null;
  }

  public Limit getLimit() {
    return this.limit;
  }

  public void setLimit(Limit limit) {
    this.limit = limit;
  }

  public Projection getProjection() {
    return this.projection;
  }

  public boolean isDistinct() {
    return this.distinct;
  }

  public void setDistinct() {
    this.distinct = true;
  }
}
