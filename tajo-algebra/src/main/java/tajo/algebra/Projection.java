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

package tajo.algebra;

import tajo.util.TUtil;

public class Projection extends UnaryOperator implements Cloneable {
  private boolean project_all;
  private boolean distinct = false;

  private Target [] targets;

  public Projection() {
    super(ExprType.Projection);
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void setDistinct() {
    distinct = true;
  }

  public void setAll() {
    project_all = true;
  }

  public boolean isAllProjected() {
    return project_all;
  }
	
	public Target [] getTargets() {
	  return this.targets;
	}

  public void setTargets(Target [] targets) {
    this.targets = targets;
  }

  @Override
  boolean equalsTo(Expr expr) {
    Projection another = (Projection) expr;
    return TUtil.checkEquals(project_all, another.project_all) &&
        TUtil.checkEquals(targets, another.targets);
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }
}
