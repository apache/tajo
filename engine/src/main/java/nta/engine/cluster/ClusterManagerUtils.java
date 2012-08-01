package nta.engine.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.ipc.protocolrecords.Fragment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * @author jihoon
 */
public class ClusterManagerUtils {
  private static Log LOG = LogFactory.getLog(ClusterManagerUtils.class);

  public static Map<String, StoredBlockInfo> makeStoredBlockInfoForHosts(
      FileSystem fs, Path path) throws IOException {
    int fileIdx, blockIdx;
    BlockLocation[] blocks;

    // get files of the table
    FileStatus[] files = fs.listStatus(new Path(path + "/data"));
    if (files == null || files.length == 0) {
      throw new FileNotFoundException(path.toString() + "/data");
    }

    Map<String, StoredBlockInfo> storedBlockInfoMap = Maps.newHashMap();
    StoredBlockInfo sbl;
    for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
      // get blocks of each file
      blocks = fs.getFileBlockLocations(files[fileIdx], 0,
          files[fileIdx].getLen());
      for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
        for (String host : blocks[blockIdx].getHosts()) {
          if (storedBlockInfoMap.containsKey(host)) {
            sbl = storedBlockInfoMap.get(host);
          } else {
            sbl = new StoredBlockInfo(host);
          }
          sbl.addBlock(files[fileIdx].getPath(), blocks[blockIdx]);
          storedBlockInfoMap.put(host, sbl);
        }
      }
    }
    return storedBlockInfoMap;
  }

  public static Map<Fragment, FragmentServingInfo> assignFragments(
      TableDescProto td, Collection<StoredBlockInfo> storedBlockInfos)
      throws IOException {
    StoredBlockInfo[] arrBlockInfo =
        new StoredBlockInfo[storedBlockInfos.size()];
    arrBlockInfo = storedBlockInfos.toArray(arrBlockInfo);
    Arrays.sort(arrBlockInfo);
    List<StoredBlockInfo> storedBlockInfoList =
        Lists.newArrayList(arrBlockInfo);
    List<StoredBlockInfo> tobeRemoved = Lists.newArrayList();
    Map<BlockLocation, FragmentServingInfo> assignInfo =
        Maps.newHashMap();

    Path currentPath;
    BlockLocation bl;
    FragmentServingInfo fsi;

    int i, j;
    while (!storedBlockInfoList.isEmpty()) {
      tobeRemoved.clear();
      for (i = 0; i < storedBlockInfoList.size(); i++) {
        StoredBlockInfo info = storedBlockInfoList.get(i);
        if (tobeRemoved.contains(info)) {
          continue;
        }
        bl = info.nextBlock();
        currentPath = info.getPathOfCurrentBlock();

        // assign the fragment
        if (assignInfo.containsKey(bl)) {
          fsi = assignInfo.get(bl);
        } else {
          fsi = new FragmentServingInfo(new Fragment(td.getId(),
              currentPath, new TableMetaImpl(td.getMeta()),
              bl.getOffset(), bl.getLength()));
        }
        fsi.addPrimaryHost(info.getHost());
        for (String h : bl.getHosts()) {
          if (h.equals(info.getHost())) {
            continue;
          }
          fsi.addHost(h);
        }
        assignInfo.put(bl, fsi);

        for (j = 0; j < storedBlockInfoList.size(); j++) {
          StoredBlockInfo cand = storedBlockInfoList.get(j);
          for (String h : bl.getHosts()) {
            if (cand.getHost().equals(h)) {
              cand.removeBlock(currentPath, bl);
              cand.resetIteration();
            }
          }
          if (cand.getBlockNum() == 0) {
            tobeRemoved.add(cand);
          }
        }
      }
      storedBlockInfoList.removeAll(tobeRemoved);
    }

    Map<Fragment, FragmentServingInfo> outMap = Maps.newHashMap();
    for (FragmentServingInfo info : assignInfo.values()) {
      outMap.put(info.getFragment(), info);
    }
    return outMap;
  }
}
