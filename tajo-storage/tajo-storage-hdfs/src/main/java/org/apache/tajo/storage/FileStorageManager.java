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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.Bytes;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FileStorageManager extends StorageManager {
  private final Log LOG = LogFactory.getLog(FileStorageManager.class);

  static final String OUTPUT_FILE_PREFIX="part-";
  static final ThreadLocal<NumberFormat> OUTPUT_FILE_FORMAT_STAGE =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };
  static final ThreadLocal<NumberFormat> OUTPUT_FILE_FORMAT_TASK =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  static final ThreadLocal<NumberFormat> OUTPUT_FILE_FORMAT_SEQ =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(3);
          return fmt;
        }
      };

  protected FileSystem fs;
  protected Path tableBaseDir;
  protected boolean blocksMetadataEnabled;
  private static final HdfsVolumeId zeroVolumeId = new HdfsVolumeId(Bytes.toBytes(0));

  public FileStorageManager(StoreType storeType) {
    super(storeType);
  }

  @Override
  protected void storageInit() throws IOException {
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
    Fragment fragment = new FileFragment(path.getName(), path, 0, status.getLen());
    return getScanner(meta, schema, fragment);
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

  private String partitionPath = "";
  private int currentDepth = 0;

  /**
   * Set a specific partition path for partition-column only queries
   * @param path The partition prefix path
   */
  public void setPartitionPath(String path) { partitionPath = path; }

  /**
   * Set a depth of partition path for partition-column only queries
   * @param depth Depth of partitions
   */
  public void setCurrentDepth(int depth) { currentDepth = depth; }

  @VisibleForTesting
  public Appender getAppender(TableMeta meta, Schema schema, Path filePath)
      throws IOException {
    return getAppender(null, null, meta, schema, filePath);
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
  public Path getAppenderFilePath(TaskAttemptId taskAttemptId, Path workDir) {
    if (taskAttemptId == null) {
      // For testcase
      return workDir;
    }
    // The final result of a task will be written in a file named part-ss-nnnnnnn,
    // where ss is the stage id associated with this task, and nnnnnn is the task id.
    Path outFilePath = StorageUtil.concatPath(workDir, TajoConstants.RESULT_DIR_NAME,
        OUTPUT_FILE_PREFIX +
            OUTPUT_FILE_FORMAT_STAGE.get().format(taskAttemptId.getTaskId().getExecutionBlockId().getId()) + "-" +
            OUTPUT_FILE_FORMAT_TASK.get().format(taskAttemptId.getTaskId().getId()) + "-" +
            OUTPUT_FILE_FORMAT_SEQ.get().format(0));
    LOG.info("Output File Path: " + outFilePath);

    return outFilePath;
  }

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
  public List<Fragment> getSplits(String tableName, TableMeta meta, Schema schema, Path... inputs)
      throws IOException {
    // generate splits'

    List<Fragment> splits = Lists.newArrayList();
    List<Fragment> volumeSplits = Lists.newArrayList();
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

  private void setVolumeMeta(List<Fragment> splits, final List<BlockLocation> blockLocations)
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

    Iterator<Fragment> iter = splits.iterator();
    while (locationSize > blockLocationIdx) {

      int subSize = Math.min(locationSize - blockLocationIdx, lsLimit);
      List<BlockLocation> locations = blockLocations.subList(blockLocationIdx, blockLocationIdx + subSize);
      //BlockStorageLocation containing additional volume location information for each replica of each block.
      BlockStorageLocation[] blockStorageLocations = fs.getFileBlockStorageLocations(locations);

      for (BlockStorageLocation blockStorageLocation : blockStorageLocations) {
        ((FileFragment)iter.next()).setDiskIds(getDiskIds(blockStorageLocation.getVolumeIds()));
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

  @Override
  public List<Fragment> getSplits(String tableName, TableDesc table, ScanNode scanNode) throws IOException {
    return getSplits(tableName, table.getMeta(), table.getSchema(), new Path(table.getPath()));
  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException {
    if (!tableDesc.isExternal()) {
      String [] splitted = CatalogUtil.splitFQTableName(tableDesc.getName());
      String databaseName = splitted[0];
      String simpleTableName = splitted[1];

      // create a table directory (i.e., ${WAREHOUSE_DIR}/${DATABASE_NAME}/${TABLE_NAME} )
      Path tablePath = StorageUtil.concatPath(tableBaseDir, databaseName, simpleTableName);
      tableDesc.setPath(tablePath.toUri());
    } else {
      Preconditions.checkState(tableDesc.getPath() != null, "ERROR: LOCATION must be given.");
    }

    Path path = new Path(tableDesc.getPath());

    FileSystem fs = path.getFileSystem(conf);
    TableStats stats = new TableStats();
    if (tableDesc.isExternal()) {
      if (!fs.exists(path)) {
        LOG.error(path.toUri() + " does not exist");
        throw new IOException("ERROR: " + path.toUri() + " does not exist");
      }
    } else {
      fs.mkdirs(path);
    }

    long totalSize = 0;

    try {
      totalSize = calculateSize(path);
    } catch (IOException e) {
      LOG.warn("Cannot calculate the size of the relation", e);
    }

    stats.setNumBytes(totalSize);

    if (tableDesc.isExternal()) { // if it is an external table, there is no way to know the exact row number without processing.
      stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
    }

    tableDesc.setStats(stats);
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException {
    try {
      Path path = new Path(tableDesc.getPath());
      FileSystem fs = path.getFileSystem(conf);
      LOG.info("Delete table data dir: " + path);
      fs.delete(path, true);
    } catch (IOException e) {
      throw new InternalError(e.getMessage());
    }
  }

  @Override
  public List<Fragment> getNonForwardSplit(TableDesc tableDesc, int currentPage, int numResultFragments) throws IOException {
    // Listing table data file which is not empty.
    // If the table is a partitioned table, return file list which has same partition key.
    Path tablePath = new Path(tableDesc.getPath());
    FileSystem fs = tablePath.getFileSystem(conf);

    //In the case of partitioned table, we should return same partition key data files.
    int partitionDepth = 0;
    if (tableDesc.hasPartition()) {
      partitionDepth = tableDesc.getPartitionMethod().getExpressionSchema().getColumns().size();
    }

    List<FileStatus> nonZeroLengthFiles = new ArrayList<FileStatus>();
    if (fs.exists(tablePath)) {
      if (!partitionPath.isEmpty()) {
        Path partPath = new Path(tableDesc.getPath() + partitionPath);
        if (fs.exists(partPath)) {
          getNonZeroLengthDataFiles(fs, partPath, nonZeroLengthFiles, currentPage, numResultFragments,
                  new AtomicInteger(0), tableDesc.hasPartition(), this.currentDepth, partitionDepth);
        }
      } else {
        getNonZeroLengthDataFiles(fs, tablePath, nonZeroLengthFiles, currentPage, numResultFragments,
                new AtomicInteger(0), tableDesc.hasPartition(), 0, partitionDepth);
      }
    }

    List<Fragment> fragments = new ArrayList<Fragment>();

    String[] previousPartitionPathNames = null;
    for (FileStatus eachFile: nonZeroLengthFiles) {
      FileFragment fileFragment = new FileFragment(tableDesc.getName(), eachFile.getPath(), 0, eachFile.getLen(), null);

      if (partitionDepth > 0) {
        // finding partition key;
        Path filePath = fileFragment.getPath();
        Path parentPath = filePath;
        String[] parentPathNames = new String[partitionDepth];
        for (int i = 0; i < partitionDepth; i++) {
          parentPath = parentPath.getParent();
          parentPathNames[partitionDepth - i - 1] = parentPath.getName();
        }

        // If current partitionKey == previousPartitionKey, add to result.
        if (previousPartitionPathNames == null) {
          fragments.add(fileFragment);
        } else if (previousPartitionPathNames != null && Arrays.equals(previousPartitionPathNames, parentPathNames)) {
          fragments.add(fileFragment);
        } else {
          break;
        }
        previousPartitionPathNames = parentPathNames;
      } else {
        fragments.add(fileFragment);
      }
    }

    return fragments;
  }

  /**
   *
   * @param fs
   * @param path The table path
   * @param result The final result files to be used
   * @param startFileIndex
   * @param numResultFiles
   * @param currentFileIndex
   * @param partitioned A flag to indicate if this table is partitioned
   * @param currentDepth Current visiting depth of partition directories
   * @param maxDepth The partition depth of this table
   * @throws IOException
   */
  private void getNonZeroLengthDataFiles(FileSystem fs, Path path, List<FileStatus> result,
                                                int startFileIndex, int numResultFiles,
                                                AtomicInteger currentFileIndex, boolean partitioned,
                                                int currentDepth, int maxDepth) throws IOException {
    // Intermediate directory
    if (fs.isDirectory(path)) {

      FileStatus[] files = fs.listStatus(path, StorageManager.hiddenFileFilter);

      if (files != null && files.length > 0) {

        for (FileStatus eachFile : files) {

          // checking if the enough number of files are found
          if (result.size() >= numResultFiles) {
            return;
          }
          if (eachFile.isDirectory()) {

            getNonZeroLengthDataFiles(
                fs,
                eachFile.getPath(),
                result,
                startFileIndex,
                numResultFiles,
                currentFileIndex,
                partitioned,
                currentDepth + 1, // increment a visiting depth
                maxDepth);

            // if partitioned table, we should ignore files located in the intermediate directory.
            // we can ensure that this file is in leaf directory if currentDepth == maxDepth.
          } else if (eachFile.isFile() && eachFile.getLen() > 0 && (!partitioned || currentDepth == maxDepth)) {
            if (currentFileIndex.get() >= startFileIndex) {
              result.add(eachFile);
            }
            currentFileIndex.incrementAndGet();
          }
        }
      }

      // Files located in leaf directory
    } else {
      FileStatus fileStatus = fs.getFileStatus(path);
      if (fileStatus != null && fileStatus.getLen() > 0) {
        if (currentFileIndex.get() >= startFileIndex) {
          result.add(fileStatus);
        }
        currentFileIndex.incrementAndGet();
        if (result.size() >= numResultFiles) {
          return;
        }
      }
    }
  }

  @Override
  public StorageProperty getStorageProperty() {
    StorageProperty storageProperty = new StorageProperty();
    storageProperty.setSortedInsert(false);
    if (storeType == StoreType.RAW) {
      storageProperty.setSupportsInsertInto(false);
    } else {
      storageProperty.setSupportsInsertInto(true);
    }

    return storageProperty;
  }

  @Override
  public void closeStorageManager() {
  }

  @Override
  public void beforeInsertOrCATS(LogicalNode node) throws IOException {
  }

  @Override
  public void rollbackOutputCommit(LogicalNode node) throws IOException {
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc,
                                          Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange)
      throws IOException {
    return null;
  }

  /**
   * Returns Scanner instance.
   *
   * @param conf The system property
   * @param meta The table meta
   * @param schema The input schema
   * @param path The data file path
   * @return Scanner instance
   * @throws java.io.IOException
   */
  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, Path path) throws IOException {

    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    FileFragment fragment = new FileFragment(path.getName(), path, 0, status.getLen());

    return getSeekableScanner(conf, meta, schema, fragment, schema);
  }
}
