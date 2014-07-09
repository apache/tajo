/**
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

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.util.TUtil;

import java.util.Set;

public class RelationList extends Expr {
  @Expose @SerializedName("Relations")
  private Expr[] relations;

  public RelationList(Expr[] relations) {
    super(OpType.RelationList);
    checkRelations(relations);
    this.relations = relations;
  }

  private void checkRelations(Expr[] relations) {
    for (Expr rel : relations) {
      Preconditions.checkArgument(
          rel.getType() == OpType.Relation ||
          rel.getType() == OpType.Join ||
          rel.getType() == OpType.TablePrimaryTableSubQuery,
          "Only Relation, Join, or TablePrimarySubQuery can be given to RelationList, but this expr "
              + " is " + rel.getType());
    }
  }

  public Expr[] getRelations() {
    return this.relations;
  }

  public int size() {
    return this.relations.length;
  }

  @Override
  public String toString() {
    return toJson();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(relations);
  }

  @Override
  boolean equalsTo(Expr expr) {
    Set<Expr> thisSet = TUtil.newHashSet(relations);
    RelationList another = (RelationList) expr;
    Set<Expr> anotherSet = TUtil.newHashSet(another.relations);
    return thisSet.equals(anotherSet);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    RelationList relationList = (RelationList) super.clone();
    relationList.relations = new Expr[relations.length];
    for (int i = 0; i < relations.length; i++) {
      relationList.relations[i] = (Expr) relations[i].clone();
    }
    return relationList;
  }
}
