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

package tajo.engine.planner.physical;

import tajo.TaskAttemptContext;
import tajo.catalog.Schema;

import java.io.IOException;

public abstract class BinaryPhysicalExec extends PhysicalExec {
  protected final PhysicalExec outerChild;
  protected final PhysicalExec innerChild;

  public BinaryPhysicalExec(final TaskAttemptContext context,
                            final Schema inSchema, final Schema outSchema,
                            final PhysicalExec outer, final PhysicalExec inner) {
    super(context, inSchema, outSchema);
    this.outerChild = outer;
    this.innerChild = inner;
  }

  public PhysicalExec getOuterChild() {
    return outerChild;
  }

  public PhysicalExec getInnerChild() {
    return innerChild;
  }

  @Override
  public void init() throws IOException {
    outerChild.init();
    innerChild.init();
  }

  @Override
  public void rescan() throws IOException {
    outerChild.rescan();
    innerChild.rescan();
  }

  @Override
  public void close() throws IOException {
    outerChild.close();
    innerChild.close();
  }
}
