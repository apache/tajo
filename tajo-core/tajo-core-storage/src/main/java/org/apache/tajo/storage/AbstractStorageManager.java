/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.TableMetaImpl;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.FileUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractStorageManager {
  private final Log LOG = LogFactory.getLog(AbstractStorageManager.class);

  protected final TajoConf conf;
  protected final FileSystem fs;
  protected final Path baseDir;
  protected final Path tableBaseDir;
  protected final boolean blocksMetadataEnabled;

  /**
   * Cache of scanner handlers for each storage type.
   */
  protected static final Map<String, Class<? extends Scanner>> SCANNER_HANDLER_CACHE
      = new ConcurrentHashMap<String, Class<? extends Scanner>>();

  /**
   * Cache of appender handlers for each storage type.
   */
  protected static final Map<String, Class<? extends FileAppender>> APPENDER_HANDLER_CACHE
      = new ConcurrentHashMap<String, Class<? extends FileAppender>>();

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();

  public abstract Scanner getScanner(TableMeta meta, Fragment fragment,
                                   Schema target) throws IOException;

  protected AbstractStorageManager(TajoConf conf) throws IOException {
    this.conf = conf;
    this.baseDir = new Path(conf.getVar(TajoConf.ConfVars.ROOT_DIR));
    this.tableBaseDir = TajoConf.getWarehousePath(conf);
    this.fs = baseDir.getFileSystem(conf);
    this.blocksMetadataEnabled = conf.getBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);
    if (!this.blocksMetadataEnabled)
      LOG.warn("does not support block metadata. ('dfs.datanode.hdfs-blocks-metadata.enabled')");
  }

  public Scanner getScanner(TableMeta meta, Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    Fragment fragment = new Fragment(path.getName(), path, meta, 0, status.getLen());
    return getScanner(meta, fragment);
  }

  public Scanner getScanner(TableMeta meta, Fragment fragment)
      throws IOException {
    return getScanner(meta, fragment, meta.getSchema());
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public Path getBaseDir() {
    return this.baseDir;
  }

  public Path getTableBaseDir() {
    return this.tableBaseDir;
  }

  public void delete(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    fs.delete(tablePath, true);
  }

  public boolean exists(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(conf);
    return fileSystem.exists(path);
  }

  /**
   * This method deletes only data contained in the given path.
   *
   * @param path The path in which data are deleted.
   * @throws IOException
   */
  public void deleteData(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(conf);
    FileStatus[] fileLists = fileSystem.listStatus(path);
    for (FileStatus status : fileLists) {
      fileSystem.delete(status.getPath(), true);
    }
  }

  public Path getTablePath(String tableName) {
    return new Path(tableBaseDir, tableName);
  }

  public Appender getAppender(TableMeta meta, Path path)
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
    if (!fs.exists(tableMetaPath)) {
      throw new FileNotFoundException(".meta file not found in " + tablePath.toString());
    }

    FSDataInputStream tableMetaIn = fs.open(tableMetaPath);

    CatalogProtos.TableProto tableProto = (CatalogProtos.TableProto) FileUtil.loadProto(tableMetaIn,
        CatalogProtos.TableProto.getDefaultInstance());
    meta = new TableMetaImpl(tableProto);

    return meta;
  }

  public Fragment[] split(String tableName) throws IOException {
    Path tablePath = new Path(tableBaseDir, tableName);
    return split(tableName, tablePath, fs.getDefaultBlockSize());
  }

  public Fragment[] split(String tableName, long fragmentSize) throws IOException {
    Path tablePath = new Path(tableBaseDir, tableName);
    return split(tableName, tablePath, fragmentSize);
  }

  public Fragment[] splitBroadcastTable(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    TableMeta meta = getTableMeta(tablePath);
    List<Fragment> listTablets = new ArrayList<Fragment>();
    Fragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus file : fileLists) {
      tablet = new Fragment(tablePath.getName(), file.getPath(), meta, 0, file.getLen());
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

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus file : fileLists) {
      long remainFileSize = file.getLen();
      long start = 0;
      if (remainFileSize > defaultBlockSize) {
        while (remainFileSize > defaultBlockSize) {
          tablet = new Fragment(tableName, file.getPath(), meta, start, defaultBlockSize);
          listTablets.add(tablet);
          start += defaultBlockSize;
          remainFileSize -= defaultBlockSize;
        }
        listTablets.add(new Fragment(tableName, file.getPath(), meta, start, remainFileSize));
      } else {
        listTablets.add(new Fragment(tableName, file.getPath(), meta, 0, remainFileSize));
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
          tablet = new Fragment(tableName, file.getPath(), meta, start, defaultBlockSize);
          listTablets.add(tablet);
          start += defaultBlockSize;
          remainFileSize -= defaultBlockSize;
        }
        listTablets.add(new Fragment(tableName, file.getPath(), meta, start, remainFileSize));
      } else {
        listTablets.add(new Fragment(tableName, file.getPath(), meta, 0, remainFileSize));
      }
    }

    Fragment[] tablets = new Fragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  public long calculateSize(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    long totalSize = 0;

    if (fs.exists(tablePath)) {
      totalSize = fs.getContentSummary(tablePath).getLength();
    }

    return totalSize;
  }

  /////////////////////////////////////////////////////////////////////////////
  // FileInputFormat Area
  /////////////////////////////////////////////////////////////////////////////

  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p) {
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
    Path[] dirs = new Path[]{path};
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);

    PathFilter inputFilter = new MultiPathFilter(filters);

    for (int i = 0; i < dirs.length; ++i) {
      Path p = dirs[i];

      FileSystem fs = p.getFileSystem(conf);
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat : matches) {
          if (globStat.isDirectory()) {
            for (FileStatus stat : fs.listStatus(globStat.getPath(),
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
   *
   * @return the number of bytes of the minimal split for this format
   */
  protected long getFormatMinSplitSize() {
    return 1;
  }

  /**
   * Is the given filename splitable? Usually, true, but if the file is
   * stream compressed, it will not be.
   * <p/>
   * <code>FileInputFormat</code> implementations can override this and return
   * <code>false</code> to ensure that individual input files are never split-up
   * so that Mappers process entire files.
   *
   * @param filename the file name to check
   * @return is this file isSplittable?
   */
  protected boolean isSplittable(TableMeta meta, Path filename) throws IOException {
    Scanner scanner = getScanner(meta, filename);
    return scanner.isSplittable();
  }

  @Deprecated
  protected long computeSplitSize(long blockSize, long minSize,
                                  long maxSize) {
    return Math.max(minSize, Math.min(maxSize, blockSize));
  }

  @Deprecated
  private static final double SPLIT_SLOP = 1.1;   // 10% slop

  @Deprecated
  protected int getBlockIndex(BlockLocation[] blkLocations,
                              long offset) {
    for (int i = 0; i < blkLocations.length; i++) {
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())) {
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length - 1];
    long fileLength = last.getOffset() + last.getLength() - 1;
    throw new IllegalArgumentException("Offset " + offset +
        " is outside of file (0.." +
        fileLength + ")");
  }

  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected Fragment makeSplit(String fragmentId, TableMeta meta, Path file, long start, long length) {
    return new Fragment(fragmentId, file, meta, start, length);
  }

  protected Fragment makeSplit(String fragmentId, TableMeta meta, Path file, BlockLocation blockLocation,
                               int[] diskIds) throws IOException {
    return new Fragment(fragmentId, file, meta, blockLocation, diskIds);
  }

  // for Non Splittable. eg, compressed gzip TextFile
  protected Fragment makeNonSplit(String fragmentId, TableMeta meta, Path file, long start, long length,
                                  BlockLocation[] blkLocations) throws IOException {

    Map<String, Integer> hostsBlockMap = new HashMap<String, Integer>();
    for (BlockLocation blockLocation : blkLocations) {
      for (String host : blockLocation.getHosts()) {
        if (hostsBlockMap.containsKey(host)) {
          hostsBlockMap.put(host, hostsBlockMap.get(host) + 1);
        } else {
          hostsBlockMap.put(host, 1);
        }
      }
    }

    List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String, Integer>>(hostsBlockMap.entrySet());
    Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {

      @Override
      public int compare(Map.Entry<String, Integer> v1, Map.Entry<String, Integer> v2) {
        return v1.getValue().compareTo(v2.getValue());
      }
    });

    String[] hosts = new String[blkLocations[0].getHosts().length];
    int[] hostsBlockCount = new int[blkLocations[0].getHosts().length];

    for (int i = 0; i < hosts.length; i++) {
      Map.Entry<String, Integer> entry = entries.get((entries.size() - 1) - i);
      hosts[i] = entry.getKey();
      hostsBlockCount[i] = entry.getValue();
    }
    return new Fragment(fragmentId, file, meta, start, length, hosts, hostsBlockCount);
  }

  /**
   * Get the maximum split size.
   *
   * @return the maximum number of bytes a split can include
   */
  @Deprecated
  public static long getMaxSplitSize() {
    // TODO - to be configurable
    return 536870912L;
  }

  /**
   * Get the minimum split size
   *
   * @return the minimum number of bytes that can be in a split
   */
  @Deprecated
  public static long getMinSplitSize() {
    // TODO - to be configurable
    return 67108864L;
  }

  /**
   * Get Disk Ids by Volume Bytes
   */
  private int[] getDiskIds(VolumeId[] volumeIds) {
    int[] diskIds = new int[volumeIds.length];
    for (int i = 0; i < volumeIds.length; i++) {
      int diskId = -1;
      if (volumeIds[i] != null && volumeIds[i].isValid()) {
        String volumeIdString = volumeIds[i].toString();
        byte[] volumeIdBytes = Base64.decodeBase64(volumeIdString);

        if (volumeIdBytes.length == 4) {
          diskId = Bytes.toInt(volumeIdBytes);
        } else if (volumeIdBytes.length == 1) {
          diskId = (int) volumeIdBytes[0];  // support hadoop-2.0.2
        }
      }
      diskIds[i] = diskId;
    }
    return diskIds;
  }

  /**
   * Generate the map of host and make them into Volume Ids.
   *
   */
  private Map<String, Set<Integer>> getVolumeMap(List<Fragment> frags) {
    Map<String, Set<Integer>> volumeMap = new HashMap<String, Set<Integer>>();
    for (Fragment frag : frags) {
      String[] hosts = frag.getHosts();
      int[] diskIds = frag.getDiskIds();
      for (int i = 0; i < hosts.length; i++) {
        Set<Integer> volumeList = volumeMap.get(hosts[i]);
        if (volumeList == null) {
          volumeList = new HashSet<Integer>();
          volumeMap.put(hosts[i], volumeList);
        }

        if (diskIds.length > 0 && diskIds[i] > -1) {
          volumeList.add(diskIds[i]);
        }
      }
    }

    return volumeMap;
  }
  /**
   * Generate the list of files and make them into FileSplits.
   *
   * @throws IOException
   */
  public List<Fragment> getSplits(String tableName, TableMeta meta, Path inputPath) throws IOException {
    // generate splits'

    List<Fragment> splits = new ArrayList<Fragment>();
    List<FileStatus> files = listStatus(inputPath);
    FileSystem fs = inputPath.getFileSystem(conf);
    for (FileStatus file : files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length > 0) {
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
        boolean splittable = isSplittable(meta, path);
        if (blocksMetadataEnabled && fs instanceof DistributedFileSystem) {
          // supported disk volume
          BlockStorageLocation[] blockStorageLocations = ((DistributedFileSystem) fs)
              .getFileBlockStorageLocations(Arrays.asList(blkLocations));
          if (splittable) {
            for (BlockStorageLocation blockStorageLocation : blockStorageLocations) {
              splits.add(makeSplit(tableName, meta, path, blockStorageLocation, getDiskIds(blockStorageLocation
                  .getVolumeIds())));
            }
          } else { // Non splittable
            splits.add(makeNonSplit(tableName, meta, path, 0, length, blockStorageLocations));
          }

        } else {
          if (splittable) {
            for (BlockLocation blockLocation : blkLocations) {
              splits.add(makeSplit(tableName, meta, path, blockLocation, null));
            }
          } else { // Non splittable
            splits.add(makeNonSplit(tableName, meta, path, 0, length, blkLocations));
          }
        }
      } else {
        //for zero length files
        splits.add(makeSplit(tableName, meta, path, 0, length));
      }
    }

    LOG.info("Total # of splits: " + splits.size());
    return splits;
  }

  private class InvalidInputException extends IOException {
    public InvalidInputException(
        List<IOException> errors) {
    }
  }

  private static final Class<?>[] DEFAULT_SCANNER_PARAMS = {
      Configuration.class,
      TableMeta.class,
      Fragment.class
  };

  private static final Class<?>[] DEFAULT_APPENDER_PARAMS = {
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
      result = meth.newInstance(new Object[]{conf, meta, fragment});
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
      result = meth.newInstance(new Object[]{conf, meta, path});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }
}
