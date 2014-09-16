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


import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.FileUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public abstract class AbstractStorageManager {
  private final Log LOG = LogFactory.getLog(AbstractStorageManager.class);

  protected final TajoConf conf;
  protected final FileSystem fs;
  protected final Path tableBaseDir;
  protected final boolean blocksMetadataEnabled;
  private static final HdfsVolumeId zeroVolumeId = new HdfsVolumeId(Bytes.toBytes(0));

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

  public abstract Class<? extends Scanner> getScannerClass(CatalogProtos.StoreType storeType) throws IOException;

  public abstract Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment, Schema target) throws IOException;

  protected AbstractStorageManager(TajoConf conf) throws IOException {
    this.conf = conf;
    this.tableBaseDir = TajoConf.getWarehouseDir(conf);
    this.fs = tableBaseDir.getFileSystem(conf);
    this.blocksMetadataEnabled = conf.getBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);
    if (!this.blocksMetadataEnabled)
      LOG.warn("does not support block metadata. ('dfs.datanode.hdfs-blocks-metadata.enabled')");
  }

  public Scanner getFileScanner(TableMeta meta, Schema schema, Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    return getFileScanner(meta, schema, path, status);
  }

  public Scanner getFileScanner(TableMeta meta, Schema schema, Path path, FileStatus status)
      throws IOException {
    FileFragment fragment = new FileFragment(path.getName(), path, 0, status.getLen());
    return getScanner(meta, schema, fragment);
  }

  public Scanner getScanner(TableMeta meta, Schema schema, FragmentProto fragment) throws IOException {
    return getScanner(meta, schema, FragmentConvertor.convert(conf, meta.getStoreType(), fragment), schema);
  }

  public Scanner getScanner(TableMeta meta, Schema schema, FragmentProto fragment, Schema target) throws IOException {
    return getScanner(meta, schema, FragmentConvertor.convert(conf, meta.getStoreType(), fragment), target);
  }

  public Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment) throws IOException {
    return getScanner(meta, schema, fragment, schema);
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public Path getWarehouseDir() {
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

  public Appender getAppender(TableMeta meta, Schema schema, Path path)
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

    appender = newAppenderInstance(appenderClass, conf, meta, schema, path);

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
    meta = new TableMeta(tableProto);

    return meta;
  }

  public FileFragment[] split(String tableName) throws IOException {
    Path tablePath = new Path(tableBaseDir, tableName);
    return split(tableName, tablePath, fs.getDefaultBlockSize());
  }

  public FileFragment[] split(String tableName, long fragmentSize) throws IOException {
    Path tablePath = new Path(tableBaseDir, tableName);
    return split(tableName, tablePath, fragmentSize);
  }

  public FileFragment[] splitBroadcastTable(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    List<FileFragment> listTablets = new ArrayList<FileFragment>();
    FileFragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus file : fileLists) {
      tablet = new FileFragment(tablePath.getName(), file.getPath(), 0, file.getLen());
      listTablets.add(tablet);
    }

    FileFragment[] tablets = new FileFragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  public FileFragment[] split(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    return split(tablePath.getName(), tablePath, fs.getDefaultBlockSize());
  }

  public FileFragment[] split(String tableName, Path tablePath) throws IOException {
    return split(tableName, tablePath, fs.getDefaultBlockSize());
  }

  private FileFragment[] split(String tableName, Path tablePath, long size)
      throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);

    long defaultBlockSize = size;
    List<FileFragment> listTablets = new ArrayList<FileFragment>();
    FileFragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus file : fileLists) {
      long remainFileSize = file.getLen();
      long start = 0;
      if (remainFileSize > defaultBlockSize) {
        while (remainFileSize > defaultBlockSize) {
          tablet = new FileFragment(tableName, file.getPath(), start, defaultBlockSize);
          listTablets.add(tablet);
          start += defaultBlockSize;
          remainFileSize -= defaultBlockSize;
        }
        listTablets.add(new FileFragment(tableName, file.getPath(), start, remainFileSize));
      } else {
        listTablets.add(new FileFragment(tableName, file.getPath(), 0, remainFileSize));
      }
    }

    FileFragment[] tablets = new FileFragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }

  public static FileFragment[] splitNG(Configuration conf, String tableName, TableMeta meta,
                                       Path tablePath, long size)
      throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);

    long defaultBlockSize = size;
    List<FileFragment> listTablets = new ArrayList<FileFragment>();
    FileFragment tablet;

    FileStatus[] fileLists = fs.listStatus(tablePath);
    for (FileStatus file : fileLists) {
      long remainFileSize = file.getLen();
      long start = 0;
      if (remainFileSize > defaultBlockSize) {
        while (remainFileSize > defaultBlockSize) {
          tablet = new FileFragment(tableName, file.getPath(), start, defaultBlockSize);
          listTablets.add(tablet);
          start += defaultBlockSize;
          remainFileSize -= defaultBlockSize;
        }
        listTablets.add(new FileFragment(tableName, file.getPath(), start, remainFileSize));
      } else {
        listTablets.add(new FileFragment(tableName, file.getPath(), 0, remainFileSize));
      }
    }

    FileFragment[] tablets = new FileFragment[listTablets.size()];
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
  protected List<FileStatus> listStatus(Path... dirs) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
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
   * Is the given filename splitable? Usually, true, but if the file is
   * stream compressed, it will not be.
   * <p/>
   * <code>FileInputFormat</code> implementations can override this and return
   * <code>false</code> to ensure that individual input files are never split-up
   * so that Mappers process entire files.
   *
   *
   * @param path the file name to check
   * @param status get the file length
   * @return is this file isSplittable?
   */
  protected boolean isSplittable(TableMeta meta, Schema schema, Path path, FileStatus status) throws IOException {
    Scanner scanner = getFileScanner(meta, schema, path, status);
    boolean split = scanner.isSplittable();
    scanner.close();
    return split;
  }

  private static final double SPLIT_SLOP = 1.1;   // 10% slop

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
  protected FileFragment makeSplit(String fragmentId, Path file, long start, long length) {
    return new FileFragment(fragmentId, file, start, length);
  }

  protected FileFragment makeSplit(String fragmentId, Path file, long start, long length,
                                   String[] hosts) {
    return new FileFragment(fragmentId, file, start, length, hosts);
  }

  protected FileFragment makeSplit(String fragmentId, Path file, BlockLocation blockLocation)
      throws IOException {
    return new FileFragment(fragmentId, file, blockLocation);
  }

  // for Non Splittable. eg, compressed gzip TextFile
  protected FileFragment makeNonSplit(String fragmentId, Path file, long start, long length,
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

    for (int i = 0; i < hosts.length; i++) {
      Map.Entry<String, Integer> entry = entries.get((entries.size() - 1) - i);
      hosts[i] = entry.getKey();
    }
    return new FileFragment(fragmentId, file, start, length, hosts);
  }

  /**
   * Get the minimum split size
   *
   * @return the minimum number of bytes that can be in a split
   */
  public long getMinSplitSize() {
    return conf.getLongVar(TajoConf.ConfVars.MINIMUM_SPLIT_SIZE);
  }

  /**
   * Get Disk Ids by Volume Bytes
   */
  private int[] getDiskIds(VolumeId[] volumeIds) {
    int[] diskIds = new int[volumeIds.length];
    for (int i = 0; i < volumeIds.length; i++) {
      int diskId = -1;
      if (volumeIds[i] != null && volumeIds[i].hashCode() > 0) {
        diskId = volumeIds[i].hashCode() - zeroVolumeId.hashCode();
      }
      diskIds[i] = diskId;
    }
    return diskIds;
  }

  /**
   * Generate the map of host and make them into Volume Ids.
   *
   */
  private Map<String, Set<Integer>> getVolumeMap(List<FileFragment> frags) {
    Map<String, Set<Integer>> volumeMap = new HashMap<String, Set<Integer>>();
    for (FileFragment frag : frags) {
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
  public List<FileFragment> getSplits(String tableName, TableMeta meta, Schema schema, Path... inputs)
      throws IOException {
    // generate splits'

    List<FileFragment> splits = Lists.newArrayList();
    List<FileFragment> volumeSplits = Lists.newArrayList();
    List<BlockLocation> blockLocations = Lists.newArrayList();

    for (Path p : inputs) {
      FileSystem fs = p.getFileSystem(conf);
      ArrayList<FileStatus> files = Lists.newArrayList();
      if (fs.isFile(p)) {
        files.addAll(Lists.newArrayList(fs.getFileStatus(p)));
      } else {
        files.addAll(listStatus(p));
      }

      int previousSplitSize = splits.size();
      for (FileStatus file : files) {
        Path path = file.getPath();
        long length = file.getLen();
        if (length > 0) {
          // Get locations of blocks of file
          BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
          boolean splittable = isSplittable(meta, schema, path, file);
          if (blocksMetadataEnabled && fs instanceof DistributedFileSystem) {

            if (splittable) {
              for (BlockLocation blockLocation : blkLocations) {
                volumeSplits.add(makeSplit(tableName, path, blockLocation));
              }
              blockLocations.addAll(Arrays.asList(blkLocations));

            } else { // Non splittable
              long blockSize = blkLocations[0].getLength();
              if (blockSize >= length) {
                blockLocations.addAll(Arrays.asList(blkLocations));
                for (BlockLocation blockLocation : blkLocations) {
                  volumeSplits.add(makeSplit(tableName, path, blockLocation));
                }
              } else {
                splits.add(makeNonSplit(tableName, path, 0, length, blkLocations));
              }
            }

          } else {
            if (splittable) {

              long minSize = Math.max(getMinSplitSize(), 1);

              long blockSize = file.getBlockSize(); // s3n rest api contained block size but blockLocations is one
              long splitSize = Math.max(minSize, blockSize);
              long bytesRemaining = length;

              // for s3
              while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
                splits.add(makeSplit(tableName, path, length - bytesRemaining, splitSize,
                    blkLocations[blkIndex].getHosts()));
                bytesRemaining -= splitSize;
              }
              if (bytesRemaining > 0) {
                int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
                splits.add(makeSplit(tableName, path, length - bytesRemaining, bytesRemaining,
                    blkLocations[blkIndex].getHosts()));
              }
            } else { // Non splittable
              splits.add(makeNonSplit(tableName, path, 0, length, blkLocations));
            }
          }
        } else {
          //for zero length files
          splits.add(makeSplit(tableName, path, 0, length));
        }
      }
      if(LOG.isDebugEnabled()){
        LOG.debug("# of splits per partition: " + (splits.size() - previousSplitSize));
      }
    }

    // Combine original fileFragments with new VolumeId information
    setVolumeMeta(volumeSplits, blockLocations);
    splits.addAll(volumeSplits);
    LOG.info("Total # of splits: " + splits.size());
    return splits;
  }

  private void setVolumeMeta(List<FileFragment> splits, final List<BlockLocation> blockLocations)
      throws IOException {

    int locationSize = blockLocations.size();
    int splitSize = splits.size();
    if (locationSize == 0 || splitSize == 0) return;

    if (locationSize != splitSize) {
      // splits and locations don't match up
      LOG.warn("Number of block locations not equal to number of splits: "
          + "#locations=" + locationSize
          + " #splits=" + splitSize);
      return;
    }

    DistributedFileSystem fs = (DistributedFileSystem)DistributedFileSystem.get(conf);
    int lsLimit = conf.getInt(DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    int blockLocationIdx = 0;

    Iterator<FileFragment> iter = splits.iterator();
    while (locationSize > blockLocationIdx) {

      int subSize = Math.min(locationSize - blockLocationIdx, lsLimit);
      List<BlockLocation> locations = blockLocations.subList(blockLocationIdx, blockLocationIdx + subSize);
      //BlockStorageLocation containing additional volume location information for each replica of each block.
      BlockStorageLocation[] blockStorageLocations = fs.getFileBlockStorageLocations(locations);

      for (BlockStorageLocation blockStorageLocation : blockStorageLocations) {
        iter.next().setDiskIds(getDiskIds(blockStorageLocation.getVolumeIds()));
        blockLocationIdx++;
      }
    }
    LOG.info("# of splits with volumeId " + splitSize);
  }

  private static class InvalidInputException extends IOException {
    List<IOException> errors;
    public InvalidInputException(List<IOException> errors) {
      this.errors = errors;
    }

    @Override
    public String getMessage(){
      StringBuffer sb = new StringBuffer();
      int messageLimit = Math.min(errors.size(), 10);
      for (int i = 0; i < messageLimit ; i ++) {
        sb.append(errors.get(i).getMessage()).append("\n");
      }

      if(messageLimit < errors.size())
        sb.append("skipped .....").append("\n");

      return sb.toString();
    }
  }

  private static final Class<?>[] DEFAULT_SCANNER_PARAMS = {
      Configuration.class,
      Schema.class,
      TableMeta.class,
      FileFragment.class
  };

  private static final Class<?>[] DEFAULT_APPENDER_PARAMS = {
      Configuration.class,
      Schema.class,
      TableMeta.class,
      Path.class
  };

  /**
   * create a scanner instance.
   */
  public static <T> T newScannerInstance(Class<T> theClass, Configuration conf, Schema schema, TableMeta meta,
                                         Fragment fragment) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(DEFAULT_SCANNER_PARAMS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[]{conf, schema, meta, fragment});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  /**
   * create a scanner instance.
   */
  public static <T> T newAppenderInstance(Class<T> theClass, Configuration conf, TableMeta meta, Schema schema,
                                          Path path) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(DEFAULT_APPENDER_PARAMS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[]{conf, schema, meta, path});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }
}
