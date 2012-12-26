package tajo.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * This class is based on ZKUtil of HBase.
 * 
 */
public class ZkUtil {
  public static final Log LOG = LogFactory.getLog(ZkUtil.class);

  private static final char ZNODE_PATH_SEPARATOR = '/';

  /**
   * Creates the specified node, if the node does not exist. Does not set a
   * watch and fails silently if the node already exists.
   * 
   * The node created is persistent and init access.
   * 
   * @param zk
   *          zk reference
   * @param znode
   *          path of node
   * @throws KeeperException
   *           if unexpected zookeeper exception
   * @throws InterruptedException
   */
  public static void createAndFailSilent(ZkClient zk, String znode)
      throws KeeperException, InterruptedException {
    if (zk.exists(znode, true) == null) {
      zk.createPersistent(znode);
    }

  }

  public static String concat(String parent, String znode) {
    return parent + ZNODE_PATH_SEPARATOR + znode;
  }

  /**
   * Check if the specified node exists. Sets no watches.
   * 
   * Returns true if node exists, false if not. Returns an exception if there is
   * an unexpected zookeeper exception.
   * 
   * @param zk
   *          zk reference
   * @param znode
   *          path of node to watch
   * @return version of the node if it exists, -1 if does not exist
   * @throws KeeperException
   *           if unexpected zookeeper exception
   */
  public static int checkExists(ZkClient zk, String znode)
      throws KeeperException {
    try {
      Stat s = zk.exists(znode);
      return s != null ? s.getVersion() : -1;
    } catch (KeeperException e) {
      LOG.warn("Unable to set watcher on znode (" + znode + ")", e);
      return -1;
    } catch (InterruptedException e) {
      LOG.warn("Unable to set watcher on znode (" + znode + ")", e);
      return -1;
    }
  }

  /**
   * Returns the full path of the immediate parent of the specified node.
   * 
   * @param node
   *          path to get parent of
   * @return parent of path, null if passed the root node or an invalid node
   */
  public static String getParent(String node) {
    int idx = node.lastIndexOf(ZNODE_PATH_SEPARATOR);
    return idx <= 0 ? null : node.substring(0, idx);
  }

  /**
   * Get the name of the current node from the specified fully-qualified path.
   * 
   * @param path
   *          fully-qualified path
   * @return name of the current node
   */
  public static String getNodeName(String path) {
    return path.substring(path.lastIndexOf("/") + 1);
  }

  /**
   * Get the data at the specified znode and set a watch.
   * 
   * Returns the data and sets a watch if the node exists. Returns null and no
   * watch is set if the node does not exist or there is an exception.
   * 
   * @param zk
   *          zk reference
   * @param znode
   *          path of node
   * @return data of the specified znode, or null
   * @throws KeeperException
   *           if unexpected zookeeper exception
   */
  public static byte[] getDataAndWatch(ZkClient zk, String znode)
      throws KeeperException {
    try {
      byte[] data = zk.getData(znode, zk, null);
      return data;
    } catch (KeeperException.NoNodeException e) {
      LOG.debug("Unable to get data of znode " + znode + " "
          + "because node does not exist (not an error)");
      return null;
    } catch (KeeperException e) {
      LOG.warn("Unable to get data of znode " + znode, e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn("Unable to get data of znode " + znode, e);
      return null;
    }
  }

  /**
   * Watch the specified znode for delete/create/change events. The watcher is
   * set whether or not the node exists. If the node already exists, the method
   * returns true. If the node does not exist, the method returns false.
   * 
   * @param zk
   *          zk reference
   * @param znode
   *          path of node to watch
   * @return true if znode exists, false if does not exist or error
   * @throws KeeperException
   *           if unexpected zookeeper exception
   */
  public static boolean watchAndCheckExists(ZkClient zk, String znode)
      throws KeeperException {
    try {
      Stat s = zk.exists(znode);
      boolean exists = s != null ? true : false;
      if (exists) {
        LOG.debug("Set watcher on existing znode " + znode);
      } else {
        LOG.debug(znode + " does not exist. Watcher is set.");
      }
      return exists;
    } catch (KeeperException e) {
      LOG.warn("Unable to set watcher on znode " + znode, e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn("Unable to set watcher on znode " + znode, e);
      return false;
    }
  }

  /**
   * Lists the children znodes of the specified znode. Also sets a watch on the
   * specified znode which will capture a NodeDeleted event on the specified
   * znode as well as NodeChildrenChanged if any children of the specified znode
   * are created or deleted.
   * 
   * Returns null if the specified node does not exist. Otherwise returns a list
   * of children of the specified node. If the node exists but it has no
   * children, an empty list will be returned.
   * 
   * @param zk
   *          zk reference
   * @param znode
   *          path of node to list and watch children of
   * @return list of children of the specified node, an empty list if the node
   *         exists but has no children, and null if the node does not exist
   * @throws KeeperException
   *           if unexpected zookeeper exception
   */
  public static List<String> listChildrenAndWatchForNewChildren(ZkClient zk,
      String znode) throws KeeperException {
    try {
      List<String> children = zk.getChildren(znode);
      return children;
    } catch (KeeperException.NoNodeException ke) {
      LOG.debug("Unable to list children of znode " + znode + " "
          + "because node does not exist (not an error)");
      return null;
    } catch (KeeperException e) {
      LOG.warn("Unable to list children of znode " + znode + " ", e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn("Unable to list children of znode " + znode + " ", e);
      return null;
    }
  }

  /**
   * List all the children of the specified znode, setting a watch for children
   * changes and also setting a watch on every individual child in order to get
   * the NodeCreated and NodeDeleted events.
   * 
   * @param zk
   *          zookeeper reference
   * @param znode
   *          node to get children of and watch
   * @return list of znode names, null if the node doesn't exist
   * @throws KeeperException
   */
  public static List<String> listChildrenAndWatchThem(ZkClient zk, String znode)
      throws KeeperException {
    List<String> children = listChildrenAndWatchForNewChildren(zk, znode);
    if (children == null) {
      return null;
    }
    for (String child : children) {
      watchAndCheckExists(zk, concat(znode, child));
    }
    return children;
  }

  public static void upsertEphemeralNode(ZkClient zk, String znode)
      throws KeeperException, InterruptedException {
    if (zk.exists(znode) != null) {
      zk.delete(znode);
    }
    zk.createEphemeral(znode);
  }

  public static void upsertEphemeralNode(ZkClient zk, String znode, byte[] data)
      throws KeeperException, InterruptedException {
    if (zk.exists(znode) != null) {
      zk.delete(znode);
    }
    zk.createEphemeral(znode, data);
  }

  public static void createPersistentNodeIfNotExist(ZkClient zk, String znode)
      throws KeeperException, InterruptedException {
    if (zk.exists(znode) == null) {
      zk.createPersistent(znode);
    }
  }

  public static void createPersistentNodeIfNotExist(ZkClient zk, String znode,
      byte[] data) throws KeeperException, InterruptedException {
    if (zk.exists(znode) == null) {
      zk.createPersistent(znode, data);
    }
  }
}
