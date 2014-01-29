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

package org.apache.tajo.engine.planner;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.NamedExpr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.util.TUtil;

import java.util.*;

import static java.util.Map.Entry;

/**
 * NamedExprsManager manages an expressions to be evaluated in a query block.
 * NamedExprsManager uses a reference name to identify one expression or one
 * EvalNode (annotated expression).
 */
public class NamedExprsManager {
  /** Map; Reference name -> EvalNode */
  private Map<String, EvalNode> nameToEvalMap = new LinkedHashMap<String, EvalNode>();
  /** Map: EvalNode -> String */
  private Map<EvalNode, String> evalToNameMap = new LinkedHashMap<EvalNode, String>();
  /** Map; Reference name -> Expr */
  private LinkedHashMap<String, Expr> nameToExprMap = new LinkedHashMap<String, Expr>();
  /** Map; Expr -> Reference Name */
  private LinkedHashMap<Expr, String> exprToNameMap = new LinkedHashMap<Expr, String>();
  /** Map; Reference Name -> Boolean (if it is resolved or not) */
  private LinkedHashMap<String, Boolean> resolvedFlags = new LinkedHashMap<String, Boolean>();

  private BiMap<String, String> aliasedColumnMap = HashBiMap.create();

  private LogicalPlan plan;

  public NamedExprsManager(LogicalPlan plan) {
    this.plan = plan;
  }

  /**
   * Check whether the expression corresponding to a given name was resolved.
   *
   * @param name The name of a certain expression to be checked
   * @return true if resolved. Otherwise, false.
   */
  public boolean isResolved(String name) {
    String normalized = name.toLowerCase();
    return resolvedFlags.containsKey(normalized) && resolvedFlags.get(normalized);
  }

  public boolean contains(String name) {
    return nameToExprMap.containsKey(name);
  }

  public boolean contains(Expr expr) {
    return exprToNameMap.containsKey(expr);
  }

  public String getName(Expr expr) {
    return exprToNameMap.get(expr);
  }

  public NamedExpr getNamedExpr(String name) {
    String normalized = name.toLowerCase();
    return new NamedExpr(nameToExprMap.get(normalized), normalized);
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

  public String addExpr(Expr expr, String alias) {
    if (exprToNameMap.containsKey(expr)) {
      return exprToNameMap.get(expr);
    } else {
      String normalized = alias.toLowerCase();
      nameToExprMap.put(normalized, expr);
      exprToNameMap.put(expr, normalized);
      resolvedFlags.put(normalized, false);
      return normalized;
    }
  }

  public String [] addReferences(Expr expr) throws PlanningException {
    Set<ColumnReferenceExpr> foundSet = ExprFinder.finds(expr, OpType.Column);
    String [] names = new String[foundSet.size()];
    int i = 0;
    for (ColumnReferenceExpr column : foundSet) {
      addExpr(column);
      names[i++] = column.getCanonicalName();
    }
    return names;
  }

  public String addExpr(Expr expr) {
    String name;

    // all columns are projected automatically. BTW, should we add column reference to this list?
    if (expr.getType() == OpType.Column) {
      name = ((ColumnReferenceExpr)expr).getCanonicalName();
      if (nameToExprMap.containsKey(name)) { // if it is column and another one already exists, skip.
        return name;
      }
    } else {
      name = plan.newGeneratedFieldName(expr);
    }
    return addExpr(expr, name);
  }

  public String addNamedExpr(NamedExpr namedExpr) {
    if (namedExpr.hasAlias()) {
      return addExpr(namedExpr.getExpr(), namedExpr.getAlias());
    } else {
      return addExpr(namedExpr.getExpr());
    }
  }

  public String [] addNamedExprArray(@Nullable Collection<NamedExpr> targets) {
    if (targets != null || targets.size() > 0) {
      String [] names = new String[targets.size()];
      int i = 0;
      for (NamedExpr target : targets) {
        names[i++] = addNamedExpr(target);
      }
      return names;
    } else {
      return null;
    }
  }

  public Collection<NamedExpr> getAllNamedExprs() {
    List<NamedExpr> namedExprList = new ArrayList<NamedExpr>();
    for (Entry<String, Expr> entry: nameToExprMap.entrySet()) {
      namedExprList.add(new NamedExpr(entry.getValue(), entry.getKey()));
    }
    return namedExprList;
  }

  public void resolveExpr(String name, EvalNode evalNode) throws PlanningException {
    String normalized = name.toLowerCase();

    if (evalNode.getType() == EvalType.CONST) {
      resolvedFlags.put(normalized, true);
    }

    nameToEvalMap.put(normalized, evalNode);
    evalToNameMap.put(evalNode, normalized);
    resolvedFlags.put(normalized, true);

    String originalName = checkAndGetIfAliasedColumn(normalized);
    if (originalName != null) {
      aliasedColumnMap.put(originalName, normalized);
    }
  }

  /**
   * It returns an original column name if it is aliased column reference.
   * Otherwise, it will return NULL.
   */
  private String checkAndGetIfAliasedColumn(String name) {
    Expr expr = nameToExprMap.get(name);
    if (expr.getType() == OpType.Column) {
      ColumnReferenceExpr column = (ColumnReferenceExpr) expr;
      if (!column.getCanonicalName().equals(name)) {
        return column.getCanonicalName();
      }
    }
    return null;
  }

  public Target getTarget(Expr expr, boolean unresolved) {
    String name = exprToNameMap.get(expr);
    return getTarget(name, unresolved);
  }

  public Target getTarget(String name) {
    return getTarget(name, false);
  }

  public Target getTarget(String name, boolean unresolved) {
    String normalized = name;
    if (!unresolved && resolvedFlags.containsKey(normalized) && resolvedFlags.get(normalized)) {
      EvalNode evalNode = nameToEvalMap.get(normalized);
      EvalNode referredEval;
      if (evalNode.getType() == EvalType.CONST) {
        referredEval = evalNode;
      } else {
        referredEval = new FieldEval(normalized, evalNode.getValueType());
      }
      return new Target(referredEval, name);
    } else {
      if (nameToEvalMap.containsKey(normalized)) {
        return new Target(nameToEvalMap.get(normalized), name);
      } else {
        return null;
      }
    }
  }

  public String toString() {
    return "unresolved=" + nameToExprMap.size() + ", resolved=" + nameToEvalMap.size()
        + ", renamed=" + aliasedColumnMap.size();
  }

  /**
   * It returns an iterator for unresolved NamedExprs.
   */
  public Iterator<NamedExpr> getUnresolvedExprs() {
    return new UnresolvedIterator();
  }

  public class UnresolvedIterator implements Iterator<NamedExpr> {
    private final Iterator<NamedExpr> iterator;

    public UnresolvedIterator() {
      List<NamedExpr> unresolvedList = TUtil.newList();
      for (Entry<String,Expr> entry : nameToExprMap.entrySet()) {
        if (!isResolved(entry.getKey())) {
          unresolvedList.add(new NamedExpr(entry.getValue(), entry.getKey()));
        }
      }
      if (unresolvedList.size() == 0) {
        iterator = null;
      } else {
        iterator = unresolvedList.iterator();
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

  public void reset() {
    for (String name : resolvedFlags.keySet()) {
      resolvedFlags.put(name, false);
    }
  }
}
