/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.catalog.function;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import tajo.catalog.Column;
import tajo.catalog.json.GsonCreator;
import tajo.datum.Datum;
import tajo.util.TUtil;

public abstract class Function<T extends Datum> implements Cloneable {
  @Expose protected Column[] definedParams;
  public final static Column [] NoArgs = new Column [] {};

  public Function(Column[] definedArgs) {
    this.definedParams = definedArgs;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Function) {
      Function other = (Function) obj;
      return TUtil.checkEquals(definedParams, other.definedParams);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(definedParams);
  }

  public Object clone() throws CloneNotSupportedException {
    Function func = (Function) super.clone();
    func.definedParams = definedParams != null ? definedParams.clone() : null;
    return func;
  }

  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, Function.class);
  }
}
