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

package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class MergeScanner implements Scanner {
  private Configuration conf;
  private TableMeta meta;
  private List<Fragment> fragments;
  private Iterator<Fragment> iterator;
  private Fragment currentFragment;
  private Scanner currentScanner;
  private Tuple tuple;

  public MergeScanner(Configuration conf, TableMeta meta, Collection<Fragment> fragments) {
    this.conf = conf;
    this.meta = meta;
    this.fragments = new ArrayList<>(fragments);
    iterator = this.fragments.iterator();
  }

  @Override
  public Tuple next() throws IOException {
    if (currentScanner != null)
      tuple = currentScanner.next();

    if (tuple != null) {
      return tuple;
    } else if (iterator.hasNext()) {
      if (currentScanner != null) {
        currentScanner.close();
      }
      currentFragment = iterator.next();
      currentScanner = StorageManager.getScanner(conf, meta, currentFragment);
      return currentScanner.next();
    } else {
      return null;
    }
  }

  @Override
  public void reset() throws IOException {
    iterator = fragments.iterator();
    if (iterator.hasNext()) {
      currentFragment = iterator.next();
      currentScanner = StorageManager.getScanner(conf, meta, currentFragment);
    }
  }

  @Override
  public void close() throws IOException {
    currentScanner.close();
    iterator = null;
    fragments.clear();
  }

  @Override
  public Schema getSchema() {
    return meta.getSchema();
  }
}
