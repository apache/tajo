package nta.engine.cluster;

import com.google.common.collect.Lists;
import nta.engine.ipc.protocolrecords.Fragment;

import java.util.List;

/**
 * @author jihoon
 */
public class FragmentServingInfo {
  private Fragment fragment;
  private List<String> hosts;
  private int nextHost;

  public FragmentServingInfo(Fragment fragment) {
    this.fragment = fragment;
    hosts = Lists.newArrayList();
    this.nextHost = 0;
  }

  public void addHost(String host) {
    hosts.add(host);
  }

  public Fragment getFragment() {
    return this.fragment;
  }

  public int getHostNum() {
    return this.hosts.size();
  }

  public String getPrimaryHost() {
    return this.hosts.get(0);
  }

  public String getNextBackupHost() {
    if (++nextHost < hosts.size()) {
      return this.hosts.get(nextHost);
    } else {
      return null;
    }
  }

  public String[] getAllHosts() {
    return this.hosts.toArray(new String[hosts.size()]);
  }

  @Override
  public String toString() {
    return "< fragment: " + fragment +
        " hosts: " + hosts +
        " next host: " + hosts.get(nextHost) + " >";
  }
}
