package nta.engine.cluster;

import com.google.common.collect.Maps;
import nta.catalog.TableDesc;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos;
import nta.engine.ipc.protocolrecords.Fragment;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * @author jihoon
 */
public class ClusterManagerUtils {

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
      CatalogProtos.TableDescProto td, Collection<StoredBlockInfo> storedBlockInfos)
      throws IOException {
    PriorityQueue<StoredBlockInfo> in = new PriorityQueue<StoredBlockInfo>();
    PriorityQueue<StoredBlockInfo> out = new PriorityQueue<StoredBlockInfo>();
    PriorityQueue<StoredBlockInfo> tmp;
    Map<BlockLocation, FragmentServingInfo> assignInfo =
        Maps.newHashMap();

    for (StoredBlockInfo sbi : storedBlockInfos) {
      in.add(sbi);
    }

    BlockLocation bl;
    StoredBlockInfo sbi;
    FragmentServingInfo fsi;
    Path currentPath;
    while (!in.isEmpty()) {
      sbi = in.peek();
      bl = sbi.nextBlock();
      currentPath = sbi.getPathOfCurrentBlock();

      // assign the fragment
      if (assignInfo.containsKey(bl)) {
        fsi = assignInfo.get(bl);
      } else {
        fsi = new FragmentServingInfo(new Fragment(td.getId(),
            currentPath, new TableMetaImpl(td.getMeta()),
            bl.getOffset(), bl.getLength()));
      }
      fsi.addHost(sbi.getHost());
      for (String h : bl.getHosts()) {
        if (h.equals(sbi.getHost())) {
          continue;
        }
        fsi.addHost(h);
      }
      assignInfo.put(bl, fsi);

      // update queue
      while (!in.isEmpty()) {
        sbi = in.peek();
        in.remove(sbi);
        for (String h : bl.getHosts()) {
          if (sbi.getHost().equals(h)) {
            sbi.removeBlock(currentPath, bl);
            sbi.initIteration();
          }
        }
        if (sbi.getBlockNum() > 0) {
          out.add(sbi);
        }
      }
      tmp = out;
      out = in;
      in = tmp;
    }

    Map<Fragment, FragmentServingInfo> outMap = Maps.newHashMap();
    for (FragmentServingInfo info : assignInfo.values()) {
      outMap.put(info.getFragment(), info);
    }
    return outMap;
  }
}
