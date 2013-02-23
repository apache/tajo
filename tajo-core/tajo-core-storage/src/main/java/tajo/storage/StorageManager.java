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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.catalog.TableMetaImpl;
import tajo.catalog.proto.CatalogProtos.TableProto;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.util.FileUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * StorageManager
 *
 */
public class StorageManager {
	private final Log LOG = LogFactory.getLog(StorageManager.class);

	private final TajoConf conf;
	private final FileSystem fs;
	private final Path baseDir;

  /**
   * Cache of scanner handlers for each storage type.
   */
  private static final Map<String, Class<? extends FileScanner>> SCANNER_HANDLER_CACHE
      = new ConcurrentHashMap<String, Class<? extends FileScanner>>();

  /**
   * Cache of appender handlers for each storage type.
   */
  private static final Map<String, Class<? extends FileAppender>> APPENDER_HANDLER_CACHE
      = new ConcurrentHashMap<String, Class<? extends FileAppender>>();

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();


	public StorageManager(TajoConf conf) throws IOException {
		this.conf = conf;
    this.baseDir = new Path(conf.getVar(ConfVars.ROOT_DIR));
    this.fs = baseDir.getFileSystem(conf);
	}
	
	public static StorageManager get(TajoConf conf) throws IOException {
	  return new StorageManager(conf);
	}
	
	public static StorageManager get(TajoConf conf, String dataRoot)
	    throws IOException {
	  conf.setVar(ConfVars.ROOT_DIR, dataRoot);
    return new StorageManager(conf);
	}
	
	public static StorageManager get(TajoConf conf, Path dataRoot)
      throws IOException {
    conf.setVar(ConfVars.ROOT_DIR, dataRoot.toString());
    return new StorageManager(conf);
  }
	
	public FileSystem getFileSystem() {
	  return this.fs;
	}
	
	public Path getBaseDir() {
	  return this.baseDir;
	}
	
  public void delete(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    fs.delete(tablePath, true);
  }
  
  public Path getTablePath(String tableName) {
    return new Path(baseDir, tableName);
  }

  public static Scanner getScanner(Configuration conf, TableMeta meta, Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    Fragment fragment = new Fragment(path.getName(), path, meta, 0, status.getLen(), null);
    return getScanner(conf, meta, fragment);
  }

  public static Scanner getScanner(Configuration conf, TableMeta meta, Fragment fragment)
      throws IOException {
    return getScanner(conf, meta, fragment, meta.getSchema());
  }

  public static Scanner getScanner(Configuration conf, TableMeta meta, Fragment fragment,
                                   Schema target)
      throws IOException {
    Scanner scanner;

    Class<? extends FileScanner> scannerClass;

    String handlerName = meta.getStoreType().name().toLowerCase();
    scannerClass = SCANNER_HANDLER_CACHE.get(handlerName);
    if (scannerClass == null) {
        scannerClass = conf.getClass(
        String.format("tajo.storage.scanner-handler.%s.class",
            meta.getStoreType().name().toLowerCase()), null,
        FileScanner.class);
      SCANNER_HANDLER_CACHE.put(handlerName, scannerClass);
    }

    if (scannerClass == null) {
      throw new IOException("Unknown Storage Type: " + meta.getStoreType());
    }

    scanner = newScannerInstance(scannerClass, conf, meta, fragment);
    if (scanner.isProjectable()) {
      scanner.setTarget(target.toArray());
    }

    return scanner;
  }

  public static Appender getAppender(Configuration conf, TableMeta meta, Path path)
      throws IOException {
    Appender appender;

    Class<? extends FileAppender> appenderClass;

    String handlerName = meta.getStoreType().name().toLowerCase();
    appenderClass = APPENDER_HANDLER_CACHE.get(handlerName);
    if (appenderClass == null) {
      appenderClass = conf.getClass(
          String.format("tajo.storage.appender-handler.%s.class",
              meta.getStoreType().name().toLowerCase()), null,
          FileAppender.class);
      APPENDER_HANDLER_CACHE.put(handlerName, appenderClass);
    }

    if (appenderClass == null) {
      throw new IOException("Unknown Storage Type: " + meta.getStoreType());
    }

    appender = newAppenderInstance(appenderClass, conf, meta, path);

    return appender;
  }


  public TableMeta getTableMeta(Path tablePath) throws IOException {
    TableMeta meta;

    FileSystem fs = tablePath.getFileSystem(conf);
    Path tableMetaPath = new Path(tablePath, ".meta");
    if(!fs.exists(tableMetaPath)) {
      throw new FileNotFoundException(".meta file not found in "+tablePath.toString());
    }

    FSDataInputStream tableMetaIn = fs.open(tableMetaPath);

    TableProto tableProto = (TableProto) FileUtil.loadProto(tableMetaIn,
        TableProto.getDefaultInstance());
    meta = new TableMetaImpl(tableProto);

    return meta;
  }

  public Fragment[] split(String tableName) throws IOException {
    Path tablePath = new Path(baseDir, tableName);
    return split(tableName, tablePath, fs.getDefaultBlockSize());
  }

  public Fragment[] split(String tableName, long fragmentSize) throws IOException {
    Path tablePath = new Path(baseDir, tableName);
    return split(tableName, tablePath, fragmentSize);
  }

  public Fragment [] splitBroadcastTable(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    TableMeta meta = getTableMeta(tablePath);
    List<Fragment> listTablets = new ArrayList<Fragment>();
    Fragment tablet;

    FileStatus[] fileLists = fs.listStatus(new Path(tablePath, "data"));
    for (FileStatus file : fileLists) {
      tablet = new Fragment(tablePath.getName(), file.getPath(), meta, 0,
          file.getLen(), null);
      listTablets.add(tablet);
    }

    Fragment[] tablets = new Fragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  public Fragment[] split(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    return split(tablePath.getName(), tablePath, fs.getDefaultBlockSize());
  }

  public Fragment[] split(String tableName, Path tablePath) throws IOException {
    return split(tableName, tablePath, fs.getDefaultBlockSize());
  }

  private Fragment[] split(String tableName, Path tablePath, long size)
      throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);

    TableMeta meta = getTableMeta(tablePath);
    long defaultBlockSize = size;
    List<Fragment> listTablets = new ArrayList<Fragment>();
    Fragment tablet;

    FileStatus[] fileLists = fs.listStatus(new Path(tablePath, "data"));
    for (FileStatus file : fileLists) {
      long remainFileSize = file.getLen();
      long start = 0;
      if (remainFileSize > defaultBlockSize) {
        while (remainFileSize > defaultBlockSize) {
          tablet = new Fragment(tableName, file.getPath(), meta, start,
              defaultBlockSize, null);
          listTablets.add(tablet);
          start += defaultBlockSize;
          remainFileSize -= defaultBlockSize;
        }
        listTablets.add(new Fragment(tableName, file.getPath(), meta, start,
            remainFileSize, null));
      } else {
        listTablets.add(new Fragment(tableName, file.getPath(), meta, 0,
            remainFileSize, null));
      }
    }

    Fragment[] tablets = new Fragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  public static Fragment[] splitNG(Configuration conf, String tableName, TableMeta meta,
                                   Path tablePath, long size)
      throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);

    long defaultBlockSize = size;
    List<Fragment> listTablets = new ArrayList<Fragment>();
    Fragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus file : fileLists) {
      long remainFileSize = file.getLen();
      long start = 0;
      if (remainFileSize > defaultBlockSize) {
        while (remainFileSize > defaultBlockSize) {
          tablet = new Fragment(tableName, file.getPath(), meta, start,
              defaultBlockSize, null);
          listTablets.add(tablet);
          start += defaultBlockSize;
          remainFileSize -= defaultBlockSize;
        }
        listTablets.add(new Fragment(tableName, file.getPath(), meta, start,
            remainFileSize, null));
      } else {
        listTablets.add(new Fragment(tableName, file.getPath(), meta, 0,
            remainFileSize, null));
      }
    }

    Fragment[] tablets = new Fragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  public void writeTableMeta(Path tableRoot, TableMeta meta)
      throws IOException {
    FileSystem fs = tableRoot.getFileSystem(conf);
    FSDataOutputStream out = fs.create(new Path(tableRoot, ".meta"));
    FileUtil.writeProto(out, meta.getProto());
    out.flush();
    out.close();
  }

  public long calculateSize(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    long totalSize = 0;
    Path oldPath = new Path(tablePath, "data");
    Path dataPath;
    if (fs.exists(oldPath)) {
      dataPath = oldPath;
    } else {
      dataPath = tablePath;
    }

    if (fs.exists(dataPath)) {
      for (FileStatus status : fs.listStatus(dataPath)) {
        totalSize += status.getLen();
      }
    }

    return totalSize;
  }

  /////////////////////////////////////////////////////////////////////////////
  // FileInputFormat Area
  /////////////////////////////////////////////////////////////////////////////

  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Proxy PathFilter that accepts a path only if all filters given in the
   * constructor do. Used by the listPaths() to apply the built-in
   * hiddenFileFilter together with a user provided one (if any).
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression.
   *
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  protected List<FileStatus> listStatus(Path path) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    Path[] dirs = new Path[] {path};
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);

    PathFilter inputFilter = new MultiPathFilter(filters);

    for (int i=0; i < dirs.length; ++i) {
      Path p = dirs[i];

      FileSystem fs = p.getFileSystem(conf);
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            for(FileStatus stat: fs.listStatus(globStat.getPath(),
                inputFilter)) {
              result.add(stat);
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    LOG.info("Total input paths to process : " + result.size());
    return result;
  }

  /**
   * Get the lower bound on split size imposed by the format.
   * @return the number of bytes of the minimal split for this format
   */
  protected long getFormatMinSplitSize() {
    return 1;
  }

  /**
   * Is the given filename splitable? Usually, true, but if the file is
   * stream compressed, it will not be.
   *
   * <code>FileInputFormat</code> implementations can override this and return
   * <code>false</code> to ensure that individual input files are never split-up
   * so that Mappers process entire files.
   *
   * @param filename the file name to check
   * @return is this file splitable?
   */
  protected boolean isSplitable(Path filename) {
    return true;
  }

  protected long computeSplitSize(long blockSize, long minSize,
                                  long maxSize) {
    return Math.max(minSize, Math.min(maxSize, blockSize));
  }

  private static final double SPLIT_SLOP = 1.1;   // 10% slop

  protected int getBlockIndex(BlockLocation[] blkLocations,
                              long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length -1];
    long fileLength = last.getOffset() + last.getLength() -1;
    throw new IllegalArgumentException("Offset " + offset +
        " is outside of file (0.." +
        fileLength + ")");
  }

  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected Fragment makeSplit(String fragmentId, TableMeta meta, Path file, long start, long length,
                               String[] hosts) {
    return new Fragment(fragmentId, file, meta, start, length, hosts);
  }

  /**
   * Get the maximum split size.
   * @return the maximum number of bytes a split can include
   */
  public static long getMaxSplitSize() {
    // TODO - to be configurable
    return 536870912L;
  }

  /**
   * Get the minimum split size
   * @return the minimum number of bytes that can be in a split
   */
  public static long getMinSplitSize() {
    // TODO - to be configurable
    return 67108864L;
  }

  /**
   * Generate the list of files and make them into FileSplits.
   * @throws IOException
   */
  public List<Fragment> getSplits(String tableName, TableMeta meta, Path inputPath) throws IOException {
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize());
    long maxSize = getMaxSplitSize();

    // generate splits
    List<Fragment> splits = new ArrayList<Fragment>();
    List<FileStatus> files = listStatus(inputPath);
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(conf);
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
        if (isSplitable(path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(blockSize, minSize, maxSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(tableName, meta, path, length-bytesRemaining, splitSize,
                blkLocations[blkIndex].getHosts()));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(tableName, meta, path, length-bytesRemaining, bytesRemaining,
                blkLocations[blkIndex].getHosts()));
          }
        } else { // not splitable
          splits.add(makeSplit(tableName, meta, path, 0, length, blkLocations[0].getHosts()));
        }
      } else {
        //Create empty hosts array for zero length files
        splits.add(makeSplit(tableName, meta, path, 0, length, new String[0]));
      }
    }
    // Save the number of input files for metrics/loadgen
    //job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    LOG.debug("Total # of splits: " + splits.size());
    return splits;
  }

  private class InvalidInputException extends IOException {
    public InvalidInputException(
        List<IOException> errors) {
    }
  }

  private static final Class<?> [] DEFAULT_SCANNER_PARAMS = {
      Configuration.class,
      TableMeta.class,
      Fragment.class
  };

  private static final Class<?> [] DEFAULT_APPENDER_PARAMS = {
      Configuration.class,
      TableMeta.class,
      Path.class
  };

  /**
   * create a scanner instance.
   */
  public static <T> T newScannerInstance(Class<T> theClass, Configuration conf, TableMeta meta,
                                         Fragment fragment) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(DEFAULT_SCANNER_PARAMS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[] {conf, meta, fragment});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  /**
   * create a scanner instance.
   */
  public static <T> T newAppenderInstance(Class<T> theClass, Configuration conf, TableMeta meta,
                                         Path path) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(DEFAULT_APPENDER_PARAMS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[] {conf, meta, path});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }
}
