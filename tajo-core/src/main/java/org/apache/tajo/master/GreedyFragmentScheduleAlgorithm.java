/*
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

package org.apache.tajo.master;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.master.DefaultFragmentScheduleAlgorithm.FragmentsPerDisk;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * GreedyFragmentScheduleAlgorithm selects a fragment considering the number of fragments that are not scheduled yet.
 * Disks of hosts have the priorities which are represented by the remaining number of fragments.
 * This algorithm selects a fragment with trying minimizing the maximum priority.
 */
public class GreedyFragmentScheduleAlgorithm implements FragmentScheduleAlgorithm {
  private final static Log LOG = LogFactory.getLog(GreedyFragmentScheduleAlgorithm.class);
  private final HostPriorityComparator hostComparator = new HostPriorityComparator();
  private Map<String, Map<Integer, FragmentsPerDisk>> fragmentHostMapping =
      new HashMap<String, Map<Integer, FragmentsPerDisk>>();
  private Map<HostAndDisk, PrioritizedHost> totalHostPriority = new HashMap<HostAndDisk, PrioritizedHost>();
  private Map<String, Set<PrioritizedHost>> hostPriorityPerRack = new HashMap<String, Set<PrioritizedHost>>();
  private TopologyCache topologyCache = new TopologyCache();
  private int totalFragmentNum = 0;

  private FragmentsPerDisk getHostFragmentSet(String host, Integer diskId) {
    Map<Integer, FragmentsPerDisk> fragmentsPerDiskMap;
    FragmentsPerDisk fragmentsPerDisk;
    if (fragmentHostMapping.containsKey(host)) {
      fragmentsPerDiskMap = fragmentHostMapping.get(host);
    } else {
      fragmentsPerDiskMap = new HashMap<Integer, FragmentsPerDisk>();
      fragmentHostMapping.put(host, fragmentsPerDiskMap);
    }
    if (fragmentsPerDiskMap.containsKey(diskId)) {
      fragmentsPerDisk = fragmentsPerDiskMap.get(diskId);
    } else {
      fragmentsPerDisk = new FragmentsPerDisk(diskId);
      fragmentsPerDiskMap.put(diskId, fragmentsPerDisk);
    }
    return fragmentsPerDisk;
  }

  private void updateHostPriority(HostAndDisk hostAndDisk, int priority) {
    if (priority > 0) {
      // update the priority among the total hosts
      PrioritizedHost prioritizedHost;
      if (totalHostPriority.containsKey(hostAndDisk)) {
        prioritizedHost = totalHostPriority.get(hostAndDisk);
        prioritizedHost.priority = priority;
      } else {
        prioritizedHost = new PrioritizedHost(hostAndDisk, priority);
        totalHostPriority.put(hostAndDisk, prioritizedHost);
      }

      // update the priority among the hosts in a rack
      String rack = topologyCache.resolve(hostAndDisk.host);
      Set<PrioritizedHost> hostsOfRack;
      if (!hostPriorityPerRack.containsKey(rack)) {
        hostsOfRack = new HashSet<PrioritizedHost>();
        hostsOfRack.add(prioritizedHost);
        hostPriorityPerRack.put(rack, hostsOfRack);
      }
    } else {
      if (totalHostPriority.containsKey(hostAndDisk)) {
        PrioritizedHost prioritizedHost = totalHostPriority.remove(hostAndDisk);

        String rack = topologyCache.resolve(hostAndDisk.host);
        if (hostPriorityPerRack.containsKey(rack)) {
          Set<PrioritizedHost> hostsOfRack = hostPriorityPerRack.get(rack);
          hostsOfRack.remove(prioritizedHost);
          if (hostsOfRack.size() == 0){
            hostPriorityPerRack.remove(rack);
          }
        }
      }
    }
  }

  @Override
  public void addFragment(FragmentPair fragmentPair) {
    String[] hosts = fragmentPair.getLeftFragment().getHosts();
    int[] diskIds = null;
    if (fragmentPair.getLeftFragment() instanceof FileFragment) {
      diskIds = ((FileFragment)fragmentPair.getLeftFragment()).getDiskIds();
    }
    for (int i = 0; i < hosts.length; i++) {
      addFragment(hosts[i], diskIds != null ? diskIds[i] : -1, fragmentPair);
    }
    totalFragmentNum++;
  }

  private void addFragment(String host, Integer diskId, FragmentPair fragmentPair) {
    host = topologyCache.normalize(host);
    FragmentsPerDisk fragmentsPerDisk = getHostFragmentSet(host, diskId);
    fragmentsPerDisk.addFragmentPair(fragmentPair);

    int priority;
    HostAndDisk hostAndDisk = new HostAndDisk(host, diskId);
    if (totalHostPriority.containsKey(hostAndDisk)) {
      priority = totalHostPriority.get(hostAndDisk).priority;
    } else {
      priority = 0;
    }
    updateHostPriority(hostAndDisk, priority+1);
  }

  public int size() {
    return totalFragmentNum;
  }

  /**
   * Selects a fragment that is stored in the given host, and replicated at the disk of the maximum
   * priority.
   * @param host
   * @return If there are fragments stored in the host, returns a fragment. Otherwise, return null.
   */
  @Override
  public FragmentPair getHostLocalFragment(String host) {
    String normalizedHost = topologyCache.normalize(host);
    if (!fragmentHostMapping.containsKey(normalizedHost)) {
      return null;
    }

    Map<Integer, FragmentsPerDisk> fragmentsPerDiskMap = fragmentHostMapping.get(normalizedHost);
    List<Integer> disks = Lists.newArrayList(fragmentsPerDiskMap.keySet());
    Collections.shuffle(disks);
    FragmentsPerDisk fragmentsPerDisk = null;
    FragmentPair fragmentPair = null;

    for (Integer diskId : disks) {
      fragmentsPerDisk = fragmentsPerDiskMap.get(diskId);
      if (fragmentsPerDisk != null && !fragmentsPerDisk.isEmpty()) {
        fragmentPair = getBestFragment(fragmentsPerDisk);
      }
      if (fragmentPair != null) {
        return fragmentPair;
      }
    }

    return null;
  }

  /**
   * Selects a fragment that is stored at the given disk of the given host, and replicated at the disk of the maximum
   * priority.
   * @param host
   * @param diskId
   * @return If there are fragments stored at the disk of the host, returns a fragment. Otherwise, return null.
   */
  @Override
  public FragmentPair getHostLocalFragment(String host, Integer diskId) {
    String normalizedHost = NetUtils.normalizeHost(host);
    if (fragmentHostMapping.containsKey(normalizedHost)) {
      Map<Integer, FragmentsPerDisk> fragmentsPerDiskMap = fragmentHostMapping.get(normalizedHost);
      if (fragmentsPerDiskMap.containsKey(diskId)) {
        FragmentsPerDisk fragmentsPerDisk = fragmentsPerDiskMap.get(diskId);
        if (!fragmentsPerDisk.isEmpty()) {
          return getBestFragment(fragmentsPerDisk);
        }
      }
    }
    return null;
  }

  /**
   * In the descending order of priority, find a fragment that is shared by the given fragment set and the fragment set
   * of the maximal priority.
   * @param fragmentsPerDisk a fragment set
   * @return a fragment that is shared by the given fragment set and the fragment set of the maximal priority
   */
  private FragmentPair getBestFragment(FragmentsPerDisk fragmentsPerDisk) {
    // Select a fragment that is shared by host and another hostAndDisk that has the most fragments
    Collection<PrioritizedHost> prioritizedHosts = totalHostPriority.values();
    PrioritizedHost[] sortedHosts = prioritizedHosts.toArray(new PrioritizedHost[prioritizedHosts.size()]);
    Arrays.sort(sortedHosts, hostComparator);

    for (PrioritizedHost nextHost : sortedHosts) {
      if (fragmentHostMapping.containsKey(nextHost.hostAndDisk.host)) {
        Map<Integer, FragmentsPerDisk> diskFragmentsMap = fragmentHostMapping.get(nextHost.hostAndDisk.host);
        if (diskFragmentsMap.containsKey(nextHost.hostAndDisk.diskId)) {
          Set<FragmentPair> largeFragmentPairSet = diskFragmentsMap.get(nextHost.hostAndDisk.diskId).getFragmentPairSet();
          Iterator<FragmentPair> smallFragmentSetIterator = fragmentsPerDisk.getFragmentPairIterator();
          while (smallFragmentSetIterator.hasNext()) {
            FragmentPair eachFragmentOfSmallSet = smallFragmentSetIterator.next();
            if (largeFragmentPairSet.contains(eachFragmentOfSmallSet)) {
              return eachFragmentOfSmallSet;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Selects a fragment that is stored at the same rack of the given host, and replicated at the disk of the maximum
   * priority.
   * @param host
   * @return If there are fragments stored at the same rack of the given host, returns a fragment. Otherwise, return null.
   */
  public FragmentPair getRackLocalFragment(String host) {
    host = topologyCache.normalize(host);
    // Select a fragment from a host that has the most fragments in the rack
    String rack = topologyCache.resolve(host);
    Set<PrioritizedHost> hostsOfRack = hostPriorityPerRack.get(rack);
    if (hostsOfRack != null && hostsOfRack.size() > 0) {
      PrioritizedHost[] sortedHosts = hostsOfRack.toArray(new PrioritizedHost[hostsOfRack.size()]);
      Arrays.sort(sortedHosts, hostComparator);
      for (PrioritizedHost nextHost : sortedHosts) {
        if (fragmentHostMapping.containsKey(nextHost.hostAndDisk.host)) {
          List<FragmentsPerDisk> disks = Lists.newArrayList(fragmentHostMapping.get(nextHost.hostAndDisk.host).values());
          Collections.shuffle(disks);

          for (FragmentsPerDisk fragmentsPerDisk : disks) {
            if (!fragmentsPerDisk.isEmpty()) {
              return fragmentsPerDisk.getFragmentPairIterator().next();
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Selects a fragment from the disk of the maximum priority.
   * @return If there are remaining fragments, it returns a fragment. Otherwise, it returns null.
   */
  public FragmentPair getRandomFragment() {
    // Select a fragment from a host that has the most fragments
    Collection<PrioritizedHost> prioritizedHosts = totalHostPriority.values();
    PrioritizedHost[] sortedHosts = prioritizedHosts.toArray(new PrioritizedHost[prioritizedHosts.size()]);
    Arrays.sort(sortedHosts, hostComparator);
    PrioritizedHost randomHost = sortedHosts[0];
    if (fragmentHostMapping.containsKey(randomHost.hostAndDisk.host)) {
      Iterator<FragmentsPerDisk> fragmentsPerDiskIterator = fragmentHostMapping.get(randomHost.hostAndDisk.host).values().iterator();
      if (fragmentsPerDiskIterator.hasNext()) {
        Iterator<FragmentPair> fragmentPairIterator = fragmentsPerDiskIterator.next().getFragmentPairIterator();
        if (fragmentPairIterator.hasNext()) {
          return fragmentPairIterator.next();
        }
      }
    }
    return null;
  }

  public FragmentPair[] getAllFragments() {
    List<FragmentPair> fragmentPairs = new ArrayList<FragmentPair>();
    for (Map<Integer, FragmentsPerDisk> eachValue : fragmentHostMapping.values()) {
      for (FragmentsPerDisk fragmentsPerDisk : eachValue.values()) {
        Set<FragmentPair> pairSet = fragmentsPerDisk.getFragmentPairSet();
        fragmentPairs.addAll(pairSet);
      }
    }
    return fragmentPairs.toArray(new FragmentPair[fragmentPairs.size()]);
  }

  public void removeFragment(FragmentPair fragmentPair) {
    String [] hosts = fragmentPair.getLeftFragment().getHosts();
    int[] diskIds = null;
    if (fragmentPair.getLeftFragment() instanceof FileFragment) {
      diskIds = ((FileFragment)fragmentPair.getLeftFragment()).getDiskIds();
    }
    for (int i = 0; i < hosts.length; i++) {
      int diskId = diskIds == null ? -1 : diskIds[i];
      String normalizedHost = NetUtils.normalizeHost(hosts[i]);
      Map<Integer, FragmentsPerDisk> diskFragmentMap = fragmentHostMapping.get(normalizedHost);

      if (diskFragmentMap != null) {
        FragmentsPerDisk fragmentsPerDisk = diskFragmentMap.get(diskId);
        if (fragmentsPerDisk != null) {
          boolean isRemoved = fragmentsPerDisk.removeFragmentPair(fragmentPair);
          if (isRemoved) {
            if (fragmentsPerDisk.size() == 0) {
              diskFragmentMap.remove(diskId);
              if (diskFragmentMap.size() == 0) {
                fragmentHostMapping.remove(normalizedHost);
              }
            }
            HostAndDisk hostAndDisk = new HostAndDisk(normalizedHost, diskId);
            if (totalHostPriority.containsKey(hostAndDisk)) {
              PrioritizedHost prioritizedHost = totalHostPriority.get(hostAndDisk);
              updateHostPriority(prioritizedHost.hostAndDisk, prioritizedHost.priority-1);
            }
          }
        }
      }
    }

    totalFragmentNum--;
  }

  private static class HostAndDisk {
    private String host;
    private Integer diskId;

    public HostAndDisk(String host, Integer diskId) {
      this.host = host;
      this.diskId = diskId;
    }

    public String getHost() {
      return host;
    }

    public int getDiskId() {
      return diskId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(host, diskId);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof HostAndDisk) {
        HostAndDisk other = (HostAndDisk) o;
        return this.host.equals(other.host) &&
            TUtil.checkEquals(this.diskId, other.diskId);
      }
      return false;
    }
  }

  public static class PrioritizedHost {
    private HostAndDisk hostAndDisk;
    private int priority;

    public PrioritizedHost(HostAndDisk hostAndDisk, int priority) {
      this.hostAndDisk = hostAndDisk;
      this.priority = priority;
    }

    public PrioritizedHost(String host, Integer diskId, int priority) {
      this.hostAndDisk = new HostAndDisk(host, diskId);
      this.priority = priority;
    }

    public String getHost() {
      return hostAndDisk.host;
    }

    public Integer getDiskId() {
      return hostAndDisk.diskId;
    }

    public Integer getPriority() {
      return priority;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof PrioritizedHost) {
        PrioritizedHost other = (PrioritizedHost) o;
        return this.hostAndDisk.equals(other.hostAndDisk);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return hostAndDisk.hashCode();
    }

    @Override
    public String toString() {
      return "host: " + hostAndDisk.host + " disk: " + hostAndDisk.diskId + " priority: " + priority;
    }
  }


  public static class HostPriorityComparator implements Comparator<PrioritizedHost> {

    @Override
    public int compare(PrioritizedHost prioritizedHost, PrioritizedHost prioritizedHost2) {
      return prioritizedHost2.priority - prioritizedHost.priority;
    }
  }


  public static class TopologyCache {
    private Map<String, String> hostRackMap = new HashMap<String, String>();
    private Map<String, String> normalizedHostMap = new HashMap<String, String>();

    public String normalize(String host) {
      if (normalizedHostMap.containsKey(host)) {
        return normalizedHostMap.get(host);
      } else {
        String normalized = NetUtils.normalizeHost(host);
        normalizedHostMap.put(host, normalized);
        return normalized;
      }
    }

    public String resolve(String host) {
      if (hostRackMap.containsKey(host)) {
        return hostRackMap.get(host);
      } else {
        String rack = RackResolver.resolve(host).getNetworkLocation();
        hostRackMap.put(host, rack);
        return rack;
      }
    }
  }
}
