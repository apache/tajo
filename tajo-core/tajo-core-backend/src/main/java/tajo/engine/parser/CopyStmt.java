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

package tajo.engine.parser;

import org.apache.hadoop.fs.Path;
import tajo.catalog.Options;
import tajo.engine.planner.PlanningContext;

import static tajo.catalog.proto.CatalogProtos.StoreType;

public class CopyStmt extends ParseTree {
  private String tableName;
  private Path path;
  private StoreType storeType;
  private Options params = null;

  public CopyStmt(PlanningContext context) {
    super(context, StatementType.COPY);
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public Path getPath() {
    return this.path;
  }

  public void setStoreType(StoreType storeType) {
    this.storeType = storeType;
  }

  public StoreType getStoreType() {
    return this.storeType;
  }

  public boolean hasParams() {
    return this.params != null;
  }

  public void setParams(Options params) {
    this.params = params;
  }

  public Options getParams() {
    return this.params;
  }
}
