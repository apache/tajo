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
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT;

public class FileTablespace extends Tablespace {

  public static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };
  private final Log LOG = LogFactory.getLog(FileTablespace.class);

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

  private static final StorageProperty FileStorageProperties = new StorageProperty("TEXT", true, true, true, false);
  private static final FormatProperty GeneralFileProperties = new FormatProperty(true, false, true);

  protected FileSystem fs;
  protected Path spacePath;
  protected Path stagingRootPath;
  protected boolean blocksMetadataEnabled;
  private static final HdfsVolumeId zeroVolumeId = new HdfsVolumeId(Bytes.toBytes(0));

  public FileTablespace(String spaceName, URI uri, JSONObject config) {
    super(spaceName, uri, config);
  }

  @Override
  protected void storageInit() throws IOException {
    this.spacePath = new Path(uri);
    this.fs = spacePath.getFileSystem(conf);
    this.stagingRootPath = fs.makeQualified(new Path(conf.getVar(TajoConf.ConfVars.STAGING_ROOT_DIR)));
    this.conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, fs.getUri().toString());

    this.blocksMetadataEnabled =
        conf.getBoolean(DFS_HDFS_BLOCKS_METADATA_ENABLED, DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);

    if (!this.blocksMetadataEnabled) {
      LOG.warn("does not support block metadata. ('dfs.datanode.hdfs-blocks-metadata.enabled')");
    }
  }

  @Override
  public long getTableVolume(URI uri, Optional<EvalNode> filter) throws UnsupportedException {
    Path path = new Path(uri);
    ContentSummary summary;
    try {
      summary = fs.getContentSummary(path);
    } catch (IOException e) {
      throw new TajoInternalError(e);
    }
    return summary.getLength();
  }

  @Override
  public URI getRootUri() {
    return fs.getUri();
  }

  public Scanner getFileScanner(TableMeta meta, Schema schema, Path path)
      throws IOException {
    FileStatus status = fs.getFileStatus(path);
    return getFileScanner(meta, schema, path, status);
  }

  public Scanner getFileScanner(TableMeta meta, Schema schema, Path path, FileStatus status)
      throws IOException {
    Fragment fragment = new FileFragment(path.getName(), path, 0, status.getLen());
    return getScanner(meta, schema, fragment, null);
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public void delete(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    fs.delete(tablePath, true);
  }

  public boolean exists(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(conf);
    return fileSystem.exists(path);
  }

  @Override
  public URI getTableUri(String databaseName, String tableName) {
    return StorageUtil.concatPath(spacePath, databaseName, tableName).toUri();
  }

  @VisibleForTesting
  public Appender getAppender(TableMeta meta, Schema schema, Path filePath)
      throws IOException {
    return getAppender(null, null, meta, schema, filePath);
  }

  public FileFragment[] split(String tableName, Path tablePath) throws IOException {
    return split(tableName, tablePath, fs.getDefaultBlockSize());
  }

  private FileFragment[] split(String tableName, Path tablePath, long size)
      throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);

    long defaultBlockSize = size;
    List<FileFragment> listTablets = new ArrayList<>();
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
    List<FileFragment> listTablets = new ArrayList<>();
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
    List<FileStatus> result = new ArrayList<>();
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    List<IOException> errors = new ArrayList<>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<>();
    filters.add(hiddenFileFilter);

    PathFilter inputFilter = new MultiPathFilter(filters);

    for (Path p : dirs) {
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        LOG.warn("Input path does not exist: " + p);
      } else if (matches.length == 0) {
        LOG.warn("Input Pattern " + p + " matches 0 files");
      } else {
        for (FileStatus globStat : matches) {
          if (globStat.isDirectory()) {
            for (FileStatus stat : fs.listStatus(globStat.getPath(), inputFilter)) {
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

    Map<String, Integer> hostsBlockMap = new HashMap<>();
    for (BlockLocation blockLocation : blkLocations) {
      for (String host : blockLocation.getHosts()) {
        if (hostsBlockMap.containsKey(host)) {
          hostsBlockMap.put(host, hostsBlockMap.get(host) + 1);
        } else {
          hostsBlockMap.put(host, 1);
        }
      }
    }

    List<Map.Entry<String, Integer>> entries = new ArrayList<>(hostsBlockMap.entrySet());
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

    DistributedFileSystem fs = (DistributedFileSystem) this.fs;
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
  public List<Fragment> getSplits(String inputSourceId,
                                  TableDesc table,
                                  @Nullable EvalNode filterCondition) throws IOException {
    return getSplits(inputSourceId, table.getMeta(), table.getSchema(), new Path(table.getUri()));
  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException {
    if (!tableDesc.isExternal()) {
      String [] splitted = CatalogUtil.splitFQTableName(tableDesc.getName());
      String databaseName = splitted[0];
      String simpleTableName = splitted[1];

      // create a table directory (i.e., ${WAREHOUSE_DIR}/${DATABASE_NAME}/${TABLE_NAME} )
      Path tablePath = StorageUtil.concatPath(spacePath, databaseName, simpleTableName);
      tableDesc.setUri(tablePath.toUri());
    } else {
      Preconditions.checkState(tableDesc.getUri() != null, "ERROR: LOCATION must be given.");
    }

    Path path = new Path(tableDesc.getUri());

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
      Path path = new Path(tableDesc.getUri());
      FileSystem fs = path.getFileSystem(conf);
      LOG.info("Delete table data dir: " + path);
      fs.delete(path, true);
    } catch (IOException e) {
      throw new InternalError(e.getMessage());
    }
  }

  @Override
  public StorageProperty getProperty() {
    return FileStorageProperties;
  }

  @Override
  public FormatProperty getFormatProperty(TableMeta meta) {
    return GeneralFileProperties;
  }

  @Override
  public void close() {
  }

  @Override
  public void prepareTable(LogicalNode node) throws IOException {
  }

  @Override
  public void rollbackTable(LogicalNode node) throws IOException {
  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    String outputPath = context.get(QueryVars.OUTPUT_TABLE_URI, "");

    Path stagingDir;
    // The fact that there is no output means that this query is neither CTAS or INSERT (OVERWRITE) INTO
    // So, this query results won't be materialized as a part of a table.
    // The result will be temporarily written in the staging directory.
    if (outputPath.isEmpty()) {
      // for temporarily written in the storage directory
      stagingDir = fs.makeQualified(new Path(stagingRootPath, queryId));
    } else {
      Tablespace space = TablespaceManager.get(outputPath);
      if (space.getProperty().isMovable()) { // checking if this tablespace allows MOVE operation
        // If this space allows move operation, the staging directory will be underneath the final output table uri.
        stagingDir = fs.makeQualified(StorageUtil.concatPath(outputPath, TMP_STAGING_DIR_PREFIX, queryId));
      } else {
        stagingDir = fs.makeQualified(new Path(stagingRootPath, queryId));
      }
    }

    return stagingDir.toUri();
  }

  // query submission directory is private!
  final public static FsPermission STAGING_DIR_PERMISSION = FsPermission.createImmutable((short) 0700); // rwx--------
  public static final String TMP_STAGING_DIR_PREFIX = ".staging";

  public URI prepareStagingSpace(TajoConf conf, String queryId, OverridableConf context, TableMeta meta)
      throws IOException {

    String realUser;
    String currentUser;
    UserGroupInformation ugi;
    ugi = UserGroupInformation.getLoginUser();
    realUser = ugi.getShortUserName();
    currentUser = UserGroupInformation.getCurrentUser().getShortUserName();


    Path stagingDir = new Path(getStagingUri(context, queryId, meta));

    ////////////////////////////////////////////
    // Create Output Directory
    ////////////////////////////////////////////

    if (fs.exists(stagingDir)) {
      throw new IOException("The staging directory '" + stagingDir + "' already exists");
    }
    fs.mkdirs(stagingDir, new FsPermission(STAGING_DIR_PERMISSION));
    FileStatus fsStatus = fs.getFileStatus(stagingDir);
    String owner = fsStatus.getOwner();

    if (!owner.isEmpty() && !(owner.equals(currentUser) || owner.equals(realUser))) {
      throw new IOException("The ownership on the user's query " +
          "directory " + stagingDir + " is not as expected. " +
          "It is owned by " + owner + ". The directory must " +
          "be owned by the submitter " + currentUser + " or " +
          "by " + realUser);
    }

    if (!fsStatus.getPermission().equals(STAGING_DIR_PERMISSION)) {
      LOG.info("Permissions on staging directory " + stagingDir + " are " +
          "incorrect: " + fsStatus.getPermission() + ". Fixing permissions " +
          "to correct value " + STAGING_DIR_PERMISSION);
      fs.setPermission(stagingDir, new FsPermission(STAGING_DIR_PERMISSION));
    }

    Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);
    fs.mkdirs(stagingResultDir);

    return stagingDir.toUri();
  }

  @Override
  public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) {
  }

  @Override
  public Path commitTable(OverridableConf queryContext, ExecutionBlockId finalEbId, LogicalPlan plan,
                          Schema schema, TableDesc tableDesc) throws IOException {
    return commitOutputData(queryContext, true);
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc,
                                          Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange)
      throws IOException {
    return null;
  }

  /**
   * Finalizes result data. Tajo stores result data in the staging directory.
   * If the query fails, clean up the staging directory.
   * Otherwise the query is successful, move to the final directory from the staging directory.
   *
   * @param queryContext The query property
   * @param changeFileSeq If true change result file name with max sequence.
   * @return Saved path
   * @throws java.io.IOException
   */
  protected Path commitOutputData(OverridableConf queryContext, boolean changeFileSeq) throws IOException {
    Path stagingDir = new Path(queryContext.get(QueryVars.STAGING_DIR));
    Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);

    Path finalOutputDir = null;
    if (!queryContext.get(QueryVars.OUTPUT_TABLE_URI, "").isEmpty()) {
      finalOutputDir = new Path(queryContext.get(QueryVars.OUTPUT_TABLE_URI));

      boolean hasPartition = !queryContext.get(QueryVars.OUTPUT_PARTITIONS, "").isEmpty() ? true : false;

      try {
        if (queryContext.getBool(QueryVars.OUTPUT_OVERWRITE, false)) { // INSERT OVERWRITE INTO
          Path oldTableDir = new Path(stagingDir, TajoConstants.INSERT_OVERWIRTE_OLD_TABLE_NAME);

          // When inserting empty data into a partitioned table, check if keep existing data need to be remove or not.
          boolean overwriteEnabled = queryContext.getBool(SessionVars.PARTITION_NO_RESULT_OVERWRITE_ENABLED);
          if (hasPartition && !overwriteEnabled) {
            commitInsertOverwriteWihProtectivePartition(stagingResultDir, finalOutputDir, oldTableDir);
          } else { 
            commitInsertOverwrite(stagingResultDir, finalOutputDir, oldTableDir);
          }
        } else {
          String queryType = queryContext.get(QueryVars.COMMAND_TYPE);
          Preconditions.checkNotNull(queryContext);
          if (queryType.equals(NodeType.INSERT.name())) { // INSERT INTO an existing table
            commitInsert(stagingResultDir, finalOutputDir, hasPartition, changeFileSeq);
          } else if (queryType.equals(NodeType.CREATE_TABLE.name())){ // CREATE TABLE AS SELECT (CTAS)
            commitCreate(stagingResultDir, finalOutputDir);
          } else {
            throw new IOException("Cannot handle query type:" + queryType);
          }
        }
        // remove the staging directory if the final output dir is given.
        Path stagingDirRoot = stagingDir.getParent();
        fs.delete(stagingDirRoot, true);
      } catch (Throwable t) {
        LOG.error(t);
        throw new IOException(t);
      }
    } else {
      finalOutputDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);
    }
    return finalOutputDir;
  }

  private void commitInsertOverwriteWihProtectivePartition(Path stagingResultDir, Path finalOutputDir,
                                                            Path oldTableDir) throws IOException {
    // This is a map for existing non-leaf directory to rename. A key is current directory and a value is
    // renaming directory.
    Map<Path, Path> renameDirs = new HashMap<>();
    // This is a map for recovering existing partition directory. A key is current directory and a value is
    // temporary directory to back up.
    Map<Path, Path> recoveryDirs = new HashMap<>();

    try {
      if (!fs.exists(finalOutputDir)) {
        fs.mkdirs(finalOutputDir);
      }

      visitPartitionedDirectory(fs, stagingResultDir, finalOutputDir, stagingResultDir.toString(),
        renameDirs, oldTableDir);

      // Rename target partition directories
      for(Map.Entry<Path, Path> entry : renameDirs.entrySet()) {
        // Backup existing data files for recovering
        if (fs.exists(entry.getValue())) {
          String recoveryPathString = entry.getValue().toString().replaceAll(finalOutputDir.toString(),
            oldTableDir.toString());
          Path recoveryPath = new Path(recoveryPathString);
          fs.rename(entry.getValue(), recoveryPath);
          fs.exists(recoveryPath);
          recoveryDirs.put(entry.getValue(), recoveryPath);
        }
        // Delete existing directory
        fs.delete(entry.getValue(), true);
        // Rename staging directory to final output directory
        fs.rename(entry.getKey(), entry.getValue());
      }

    } catch (IOException ioe) {
      // Remove created dirs
      for(Map.Entry<Path, Path> entry : renameDirs.entrySet()) {
        fs.delete(entry.getValue(), true);
      }

      // Recovery renamed dirs
      for(Map.Entry<Path, Path> entry : recoveryDirs.entrySet()) {
        fs.delete(entry.getValue(), true);
        fs.rename(entry.getValue(), entry.getKey());
      }

      throw new IOException(ioe.getMessage());
    }
  }

  private void commitInsertOverwrite(Path stagingResultDir, Path finalOutputDir, Path oldTableDir) throws IOException {
    // It moves the original table into the temporary location.
    // Then it moves the new result table into the original table location.
    // Upon failed, it recovers the original table if possible.
    boolean movedToOldTable = false;
    boolean committed = false;

    try {
      // if the final output dir exists, move all contents to the temporary table dir.
      // Otherwise, just make the final output dir. As a result, the final output dir will be empty.
      if (fs.exists(finalOutputDir)) {
        fs.mkdirs(oldTableDir);

        for (FileStatus status : fs.listStatus(finalOutputDir, hiddenFileFilter)) {
          fs.rename(status.getPath(), oldTableDir);
        }

        movedToOldTable = fs.exists(oldTableDir);
      } else { // if the parent does not exist, make its parent directory.
        fs.mkdirs(finalOutputDir);
      }

      // Move the results to the final output dir.
      for (FileStatus status : fs.listStatus(stagingResultDir)) {
        fs.rename(status.getPath(), finalOutputDir);
      }

      // Check the final output dir
      committed = fs.exists(finalOutputDir);

    } catch (IOException ioe) {
      // recover the old table
      if (movedToOldTable && !committed) {

        // if commit is failed, recover the old data
        for (FileStatus status : fs.listStatus(finalOutputDir, hiddenFileFilter)) {
          fs.delete(status.getPath(), true);
        }

        for (FileStatus status : fs.listStatus(oldTableDir)) {
          fs.rename(status.getPath(), finalOutputDir);
        }
      }

      throw new IOException(ioe.getMessage());
    }
  }

  private void commitInsert(Path stagingResultDir, Path finalOutputDir, boolean hasPartition, boolean changeFileSeq)
    throws IOException {
    NumberFormat fmt = NumberFormat.getInstance();
    fmt.setGroupingUsed(false);
    fmt.setMinimumIntegerDigits(3);

    if (hasPartition) {
      for(FileStatus eachFile: fs.listStatus(stagingResultDir)) {
        if (eachFile.isFile()) {
          LOG.warn("Partition table can't have file in a staging dir: " + eachFile.getPath());
          continue;
        }
        moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputDir, fmt, -1, changeFileSeq);
      }
    } else {
      int maxSeq = StorageUtil.getMaxFileSequence(fs, finalOutputDir, false) + 1;
      for(FileStatus eachFile: fs.listStatus(stagingResultDir)) {
        if (eachFile.getPath().getName().startsWith("_")) {
          continue;
        }
        moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputDir, fmt, maxSeq++, changeFileSeq);
      }
    }
    // checking all file moved and remove empty dir
    verifyAllFileMoved(fs, stagingResultDir);
    FileStatus[] files = fs.listStatus(stagingResultDir);
    if (files != null && files.length != 0) {
      for (FileStatus eachFile: files) {
        LOG.error("There are some unmoved files in staging dir:" + eachFile.getPath());
      }
    }
  }

  private void commitCreate(Path stagingResultDir, Path finalOutputDir) throws IOException {
    if (fs.exists(finalOutputDir)) {
      for (FileStatus status : fs.listStatus(stagingResultDir)) {
        fs.rename(status.getPath(), finalOutputDir);
      }
    } else {
      fs.rename(stagingResultDir, finalOutputDir);
    }
    LOG.info("Moved from the staging dir to the output directory '" + finalOutputDir);
  }

  /**
   * Attach the sequence number to the output file name and than move the file into the final result path.
   *
   * @param fs FileSystem
   * @param stagingResultDir The staging result dir
   * @param fileStatus The file status
   * @param finalOutputPath Final output path
   * @param nf Number format
   * @param fileSeq The sequence number
   * @throws java.io.IOException
   */
  private void moveResultFromStageToFinal(FileSystem fs, Path stagingResultDir,
                                          FileStatus fileStatus, Path finalOutputPath,
                                          NumberFormat nf,
                                          int fileSeq, boolean changeFileSeq) throws IOException {
    if (fileStatus.isDirectory()) {
      String subPath = extractSubPath(stagingResultDir, fileStatus.getPath());
      if (subPath != null) {
        Path finalSubPath = new Path(finalOutputPath, subPath);
        if (!fs.exists(finalSubPath)) {
          fs.mkdirs(finalSubPath);
        }
        int maxSeq = StorageUtil.getMaxFileSequence(fs, finalSubPath, false);
        for (FileStatus eachFile : fs.listStatus(fileStatus.getPath())) {
          if (eachFile.getPath().getName().startsWith("_")) {
            continue;
          }
          moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputPath, nf, ++maxSeq, changeFileSeq);
        }
      } else {
        throw new IOException("Wrong staging dir:" + stagingResultDir + "," + fileStatus.getPath());
      }
    } else {
      String subPath = extractSubPath(stagingResultDir, fileStatus.getPath());
      if (subPath != null) {
        Path finalSubPath = new Path(finalOutputPath, subPath);
        if (changeFileSeq) {
          finalSubPath = new Path(finalSubPath.getParent(), replaceFileNameSeq(finalSubPath, fileSeq, nf));
        }
        if (!fs.exists(finalSubPath.getParent())) {
          fs.mkdirs(finalSubPath.getParent());
        }
        if (fs.exists(finalSubPath)) {
          throw new IOException("Already exists data file:" + finalSubPath);
        }
        boolean success = fs.rename(fileStatus.getPath(), finalSubPath);
        if (success) {
          LOG.info("Moving staging file[" + fileStatus.getPath() + "] + " +
              "to final output[" + finalSubPath + "]");
        } else {
          LOG.error("Can't move staging file[" + fileStatus.getPath() + "] + " +
              "to final output[" + finalSubPath + "]");
        }
      }
    }
  }

  /**
   * Removes the path of the parent.
   * @param parentPath
   * @param childPath
   * @return
   */
  private String extractSubPath(Path parentPath, Path childPath) {
    String parentPathStr = parentPath.toUri().getPath();
    String childPathStr = childPath.toUri().getPath();

    if (parentPathStr.length() > childPathStr.length()) {
      return null;
    }

    int index = childPathStr.indexOf(parentPathStr);
    if (index != 0) {
      return null;
    }

    return childPathStr.substring(parentPathStr.length() + 1);
  }

  /**
   * Attach the sequence number to a path.
   *
   * @param path Path
   * @param seq sequence number
   * @param nf Number format
   * @return New path attached with sequence number
   * @throws java.io.IOException
   */
  private String replaceFileNameSeq(Path path, int seq, NumberFormat nf) throws IOException {
    String[] tokens = path.getName().split("-");
    if (tokens.length != 4) {
      throw new IOException("Wrong result file name:" + path);
    }
    return tokens[0] + "-" + tokens[1] + "-" + tokens[2] + "-" + nf.format(seq);
  }

  /**
   * Make sure all files are moved.
   * @param fs FileSystem
   * @param stagingPath The stagind directory
   * @return
   * @throws java.io.IOException
   */
  private boolean verifyAllFileMoved(FileSystem fs, Path stagingPath) throws IOException {
    FileStatus[] files = fs.listStatus(stagingPath);
    if (files != null && files.length != 0) {
      for (FileStatus eachFile: files) {
        if (eachFile.isFile()) {
          LOG.error("There are some unmoved files in staging dir:" + eachFile.getPath());
          return false;
        } else {
          if (verifyAllFileMoved(fs, eachFile.getPath())) {
            fs.delete(eachFile.getPath(), false);
          } else {
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * This method sets a rename map which includes renamed staging directory to final output directory recursively.
   * If there exists some data files, this delete it for duplicate data.
   *
   *
   * @param fs
   * @param stagingPath
   * @param outputPath
   * @param stagingParentPathString
   * @throws java.io.IOException
   */
  private void visitPartitionedDirectory(FileSystem fs, Path stagingPath, Path outputPath,
                                         String stagingParentPathString,
                                         Map<Path, Path> renameDirs, Path oldTableDir) throws IOException {
    FileStatus[] files = fs.listStatus(stagingPath);

    for(FileStatus eachFile : files) {
      if (eachFile.isDirectory()) {
        Path oldPath = eachFile.getPath();

        // Make recover directory.
        String recoverPathString = oldPath.toString().replaceAll(stagingParentPathString,
            oldTableDir.toString());
        Path recoveryPath = new Path(recoverPathString);
        if (!fs.exists(recoveryPath)) {
          fs.mkdirs(recoveryPath);
        }

        visitPartitionedDirectory(fs, eachFile.getPath(), outputPath, stagingParentPathString,
            renameDirs, oldTableDir);
        // Find last order partition for renaming
        String newPathString = oldPath.toString().replaceAll(stagingParentPathString,
            outputPath.toString());
        Path newPath = new Path(newPathString);
        if (!isLeafDirectory(fs, eachFile.getPath())) {
          renameDirs.put(eachFile.getPath(), newPath);
        } else {
          if (!fs.exists(newPath)) {
            fs.mkdirs(newPath);
          }
        }
      }
    }
  }

  private boolean isLeafDirectory(FileSystem fs, Path path) throws IOException {
    boolean retValue = false;

    FileStatus[] files = fs.listStatus(path);
    for (FileStatus file : files) {
      if (fs.isDirectory(file.getPath())) {
        retValue = true;
        break;
      }
    }

    return retValue;
  }


}
