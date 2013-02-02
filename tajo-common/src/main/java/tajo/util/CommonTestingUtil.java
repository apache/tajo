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

package tajo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.UUID;

public class CommonTestingUtil {
  /**
   *
   * @param dir a local directory to be created
   * @return  the created path
   * @throws IOException
   */
  public static Path getTestDir(String dir) throws IOException {
    Path path = new Path(dir);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    if(fs.exists(path))
      fs.delete(path, true);

    fs.mkdirs(path);

    return fs.makeQualified(path);
  }

  public static Path getTestDir() throws IOException {
    String randomStr = UUID.randomUUID().toString();
    Path path = new Path("target/test-data", randomStr);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    if(fs.exists(path))
      fs.delete(path, true);

    fs.mkdirs(path);

    return fs.makeQualified(path);
  }
}
