/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.NamedExpr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalType;
import org.apache.tajo.plan.expr.FieldEval;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * NamedExprsManager manages an expressions used in a query block. All expressions used in a query block must be
 * added to NamedExprsManager. When an expression is added to NamedExprsManager, NamedExprsManager gives a reference
 * to the expression. If the expression already has an alias name, it gives the alias name as the reference
 * to the expression. If the expression does not have any alias, it gives a generated name as the reference to the
 * expression. Usually, predicates in WHERE clause, expressions in GROUP-BY, ORDER-BY, LIMIT clauses are not given
 * any alias name. Those reference names are used to identify an individual expression.
 *
 * NamedExprsManager only keeps unique expressions. Since expressions in a query block can be duplicated,
 * one or more reference names can point one expressions. Due to this process, it naturally removes duplicated
 * expression.
 *
 * As we mentioned above, one or more reference names can indicate one expression. Primary names are used for
 * representing expressions. A primary name of an expression indicates the reference obtained when
 * the expression is added firstly. All output schemas uses only primary names of expressions.
 *
 * Each expression that NamedExprsManager keeps has an boolean state to indicate whether the expression is evaluated
 * or not. The <code>evaluated</code> state means that upper logical operators can access this expression like a column
 * reference. For it, the reference name is used to access this expression like a column reference,
 * The evaluated state is set with an EvalNode which is an annotated expression.
 * {@link #getTarget(String)} returns EvalNodes by a reference name.
 */
public class NamedExprsManager {
  /** a sequence id */
  private int sequenceId = 0;

  /** Map: Name -> ID. Two or more different names can indicates the same id. */
  private LinkedHashMap<String, Integer> nameToIdMap = Maps.newLinkedHashMap();

  /** Map; ID <-> EvalNode */
  private BiMap<Integer, EvalNode> idToEvalMap = HashBiMap.create();

  /** Map: ID -> Names */
  private LinkedHashMap<Integer, List<String>> idToNamesMap = Maps.newLinkedHashMap();

  /** Map: ID -> Expr */
  private BiMap<Integer, Expr> idToExprBiMap = HashBiMap.create();

  /** Map; Name -> Boolean (if it is resolved or not) */
  private LinkedHashMap<Integer, Boolean> evaluationStateMap = Maps.newLinkedHashMap();

  /** Map: Alias Name <-> Original Name */
  private BiMap<String, String> aliasedColumnMap = HashBiMap.create();

  private final LogicalPlan plan;

  private final LogicalPlan.QueryBlock block;

  public NamedExprsManager(LogicalPlan plan, LogicalPlan.QueryBlock block) {
    this.plan = plan;
    this.block = block;
  }

  private int getNextId() {
    return sequenceId++;
  }

  /**
   * Check whether the expression corresponding to a given name was evaluated.
   *
   * @param name The name of a certain expression to be checked
   * @return true if resolved. Otherwise, false.
   */
  public boolean isEvaluated(String name) {
    if (nameToIdMap.containsKey(name)) {
      int refId = nameToIdMap.get(name);
      return evaluationStateMap.containsKey(refId) && evaluationStateMap.get(refId);
    } else {
      return false;
    }
  }

  public boolean contains(String name) {
    return nameToIdMap.containsKey(name);
  }

  public boolean contains(Expr expr) {
    return idToExprBiMap.inverse().containsKey(expr);
  }

  private Expr getExpr(String name) {
    return idToExprBiMap.get(nameToIdMap.get(name));
  }

  public NamedExpr getNamedExpr(String name) {
    String normalized = name;
    return new NamedExpr(getExpr(name), normalized);
  }

  public boolean isAliased(String name) {
    return aliasedColumnMap.containsKey(name);
  }

  public String getAlias(String originalName) {
    return aliasedColumnMap.get(originalName);
  }

  public boolean isAliasedName(String aliasName) {
    return aliasedColumnMap.inverse().containsKey(aliasName);
  }

  public String getOriginalName(String aliasName) {
    return aliasedColumnMap.inverse().get(aliasName);
  }

  /**
   * Adds an expression and returns a reference name.
   * @param expr added expression
   */
  public String addExpr(Expr expr) {
    if (idToExprBiMap.inverse().containsKey(expr)) {
      int refId = idToExprBiMap.inverse().get(expr);
      return idToNamesMap.get(refId).get(0);
    }

    if (block.isRegisteredConst(expr)) {
      return block.getConstReference(expr);
    }

    String generatedName = plan.generateUniqueColumnName(expr);
    return addExpr(expr, generatedName);
  }

  /**
   * Adds an expression with an alias name and returns a reference name.
   * It specifies the alias as an reference name.
   */
  public String addExpr(Expr expr, String alias) {

    if (OpType.isLiteralType(expr.getType())) {
      return alias;
    }

    // if this name already exists, just returns the name.
    if (nameToIdMap.containsKey(alias)) {
      return alias;
    }

    // if the name is first
    int refId;
    if (idToExprBiMap.inverse().containsKey(expr)) {
      refId = idToExprBiMap.inverse().get(expr);
    } else {
      refId = getNextId();
      idToExprBiMap.put(refId, expr);
    }

    nameToIdMap.put(alias, refId);
    evaluationStateMap.put(refId, false);

    // add the entry to idToNames map
    TUtil.putToNestedList(idToNamesMap, refId, alias);

    return alias;
  }

  /**
   * Adds an expression and returns a reference name.
   * If an alias is given, it specifies the alias as an reference name.
   */
  public String addNamedExpr(NamedExpr namedExpr) {
    if (namedExpr.hasAlias()) {
      return addExpr(namedExpr.getExpr(), namedExpr.getAlias());
    } else {
      return addExpr(namedExpr.getExpr());
    }
  }

  /**
   * Adds a list of expressions and returns a list of reference names.
   * If some NamedExpr has an alias, NamedExprsManager specifies the alias for the NamedExpr.
   */
  public String [] addNamedExprArray(@Nullable Collection<NamedExpr> namedExprs) {
    if (namedExprs != null && namedExprs.size() > 0) {
      String [] names = new String[namedExprs.size()];
      int i = 0;
      for (NamedExpr target : namedExprs) {
        names[i++] = addNamedExpr(target);
      }
      return names;
    } else {
      return null;
    }
  }

  public Collection<NamedExpr> getAllNamedExprs() {
    List<NamedExpr> namedExprList = Lists.newArrayList();
    for (Map.Entry<Integer, Expr> entry: idToExprBiMap.entrySet()) {
      namedExprList.add(new NamedExpr(entry.getValue(), idToNamesMap.get(entry.getKey()).get(0)));
    }
    return namedExprList;
  }

  /**
   * It marks the expression identified by the reference name as <code>evaluated</code>.
   * In addition, it adds an EvanNode for the expression identified by the reference.
   *
   * @param referenceName The reference name to be marked as 'evaluated'.
   * @param evalNode EvalNode to be added.
   */
  public void markAsEvaluated(String referenceName, EvalNode evalNode) {
    String normalized = referenceName;

    int refId = nameToIdMap.get(normalized);
    evaluationStateMap.put(refId, true);
    idToEvalMap.put(refId, evalNode);

    String originalName = checkAndGetIfAliasedColumn(normalized);
    if (originalName != null) {
      aliasedColumnMap.put(originalName, normalized);
    }
  }

  /**
   * It returns an original column name if it is aliased column reference.
   * Otherwise, it will return NULL.
   */
  public String checkAndGetIfAliasedColumn(String name) {
    Expr expr = getExpr(name);
    if (expr != null && expr.getType() == OpType.Column) {
      ColumnReferenceExpr column = (ColumnReferenceExpr) expr;
      if (!column.getCanonicalName().equals(name)) {
        return column.getCanonicalName();
      }
    }
    return null;
  }

  public Target getTarget(String name) {
    return getTarget(name, false);
  }

  /**
   * It checks if a given name is the primary name.
   *
   * @See {@link NamedExprsManager}
   * @see {@link NamedExprsManager#getPrimaryName}
   *
   * @param id The expression id
   * @param name The name to be checked if it is primary name.
   * @return The primary name
   */
  private boolean isPrimaryName(int id, String name) {
    return idToNamesMap.get(id).get(0).equals(name);
  }

  /**
   * One or more reference names can indicate one expression. Primary names are used for
   * representing expressions. A primary name of an expression indicates the reference obtained when
   * the expression is added firstly. All output schemas uses only primary names of expressions.
   *
   * @param id The expression id
   * @return The primary name
   */
  private String getPrimaryName(int id) {
    return idToNamesMap.get(id).get(0);
  }

  /**
   * get a Target instance. A target consists of a reference name and an EvalNode corresponding to the reference name.
   * According to evaluation state, it returns different EvalNodes.
   * If the expression corresponding to the reference name is evaluated, it just returns {@link FieldEval}
   * (i.e., a column reference). Otherwise, it returns the original EvalNode of the expression.
   *
   * @param referenceName The reference name to get EvalNode
   * @param unevaluatedForm If TRUE, it always return the annotated EvalNode of the expression.
   * @return
   */
  public Target getTarget(String referenceName, boolean unevaluatedForm) {
    String normalized = referenceName;
    int refId = nameToIdMap.get(normalized);

    if (!unevaluatedForm && evaluationStateMap.containsKey(refId) && evaluationStateMap.get(refId)) {
      EvalNode evalNode = idToEvalMap.get(refId);

      // If the expression is already evaluated, it should use the FieldEval to access a field value.
      // But, if this reference name is not primary name, it cannot use the reference name.
      // It changes the given reference name to the primary name.
      if (evalNode.getType() != EvalType.CONST && isEvaluated(normalized) && !isPrimaryName(refId, referenceName)) {
        return new Target(new FieldEval(getPrimaryName(refId),evalNode.getValueType()), referenceName);
      }

      EvalNode referredEval;
      if (evalNode.getType() == EvalType.CONST) {
        referredEval = evalNode;
      } else {
        referredEval = new FieldEval(idToNamesMap.get(refId).get(0), evalNode.getValueType());
      }
      return new Target(referredEval, referenceName);

    } else {
      if (idToEvalMap.containsKey(refId)) {
        return new Target(idToEvalMap.get(refId), referenceName);
      } else {
        return null;
      }
    }
  }

  public String toString() {
    return "unevaluated=" + nameToIdMap.size() + ", evaluated=" + idToEvalMap.size()
        + ", renamed=" + aliasedColumnMap.size();
  }

  /**
   * It returns an iterator for unevaluated NamedExprs.
   */
  public Iterator<NamedExpr> getIteratorForUnevaluatedExprs() {
    return new UnevaluatedIterator();
  }

  public class UnevaluatedIterator implements Iterator<NamedExpr> {
    private final Iterator<NamedExpr> iterator;

    public UnevaluatedIterator() {
      List<NamedExpr> unEvaluatedList = new ArrayList<>();
      for (Integer refId: idToNamesMap.keySet()) {
        String name = idToNamesMap.get(refId).get(0);
        if (!isEvaluated(name)) {
          Expr expr = idToExprBiMap.get(refId);
          unEvaluatedList.add(new NamedExpr(expr, name));
        }
      }
      if (unEvaluatedList.size() == 0) {
        iterator = null;
      } else {
        iterator = unEvaluatedList.iterator();
      }
    }

    @Override
    public boolean hasNext() {
      return iterator != null && iterator.hasNext();
    }

    @Override
    public NamedExpr next() {
      return iterator.next();
    }

    @Override
    public void remove() {
    }
  }
}
