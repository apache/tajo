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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.NetUtils;

import java.util.*;
import java.util.Map.Entry;

/**
 * DefaultFragmentScheduleAlgorithm selects a fragment randomly for the given argument.
 * For example, when getHostLocalFragment(host, disk) is called, this algorithm randomly selects a fragment among
 * the fragments which are stored at the disk of the host specified by the arguments.
 */
public class DefaultFragmentScheduleAlgorithm implements FragmentScheduleAlgorithm {
  private final static Log LOG = LogFactory.getLog(DefaultFragmentScheduleAlgorithm.class);
  private Map<String, Map<Integer, FragmentsPerDisk>> fragmentHostMapping =
      new HashMap<String, Map<Integer, FragmentsPerDisk>>();
  private Map<String, Set<FragmentPair>> rackFragmentMapping =
      new HashMap<String, Set<FragmentPair>>();
  private int fragmentNum = 0;
  private Random random = new Random(System.currentTimeMillis());

  public static class FragmentsPerDisk {
    private Integer diskId;
    private Set<FragmentPair> fragmentPairSet;

    public FragmentsPerDisk(Integer diskId) {
      this.diskId = diskId;
      this.fragmentPairSet = Collections.newSetFromMap(new HashMap<FragmentPair, Boolean>());
    }

    public Integer getDiskId() {
      return diskId;
    }

    public Set<FragmentPair> getFragmentPairSet() {
      return fragmentPairSet;
    }

    public void addFragmentPair(FragmentPair fragmentPair) {
      fragmentPairSet.add(fragmentPair);
    }

    public boolean removeFragmentPair(FragmentPair fragmentPair) {
      return fragmentPairSet.remove(fragmentPair);
    }

    public int size() {
      return fragmentPairSet.size();
    }

    public Iterator<FragmentPair> getFragmentPairIterator() {
      return fragmentPairSet.iterator();
    }

    public boolean isEmpty() {
      return fragmentPairSet.isEmpty();
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
    fragmentNum++;
  }

  private void addFragment(String host, Integer diskId, FragmentPair fragmentPair) {
    // update the fragment maps per host
    String normalizeHost = NetUtils.normalizeHost(host);
    Map<Integer, FragmentsPerDisk> diskFragmentMap;
    if (fragmentHostMapping.containsKey(normalizeHost)) {
      diskFragmentMap = fragmentHostMapping.get(normalizeHost);
    } else {
      diskFragmentMap = new HashMap<Integer, FragmentsPerDisk>();
      fragmentHostMapping.put(normalizeHost, diskFragmentMap);
    }
    FragmentsPerDisk fragmentsPerDisk;
    if (diskFragmentMap.containsKey(diskId)) {
      fragmentsPerDisk = diskFragmentMap.get(diskId);
    } else {
      fragmentsPerDisk = new FragmentsPerDisk(diskId);
      diskFragmentMap.put(diskId, fragmentsPerDisk);
    }
    fragmentsPerDisk.addFragmentPair(fragmentPair);

    // update the fragment maps per rack
    String rack = RackResolver.resolve(normalizeHost).getNetworkLocation();
    Set<FragmentPair> fragmentPairList;
    if (rackFragmentMapping.containsKey(rack)) {
      fragmentPairList = rackFragmentMapping.get(rack);
    } else {
      fragmentPairList = Collections.newSetFromMap(new HashMap<FragmentPair, Boolean>());
      rackFragmentMapping.put(rack, fragmentPairList);
    }
    fragmentPairList.add(fragmentPair);
  }

  @Override
  public void removeFragment(FragmentPair fragmentPair) {
    boolean removed = false;
    for (String eachHost : fragmentPair.getLeftFragment().getHosts()) {
      String normalizedHost = NetUtils.normalizeHost(eachHost);
      Map<Integer, FragmentsPerDisk> diskFragmentMap = fragmentHostMapping.get(normalizedHost);
      for (Entry<Integer, FragmentsPerDisk> entry : diskFragmentMap.entrySet()) {
        FragmentsPerDisk fragmentsPerDisk = entry.getValue();
        removed = fragmentsPerDisk.removeFragmentPair(fragmentPair);
        if (removed) {
          if (fragmentsPerDisk.size() == 0) {
            diskFragmentMap.remove(entry.getKey());
          }
          if (diskFragmentMap.size() == 0) {
            fragmentHostMapping.remove(normalizedHost);
          }
          break;
        }
      }
      String rack = RackResolver.resolve(normalizedHost).getNetworkLocation();
      if (rackFragmentMapping.containsKey(rack)) {
        Set<FragmentPair> fragmentPairs = rackFragmentMapping.get(rack);
        fragmentPairs.remove(fragmentPair);
        if (fragmentPairs.size() == 0) {
          rackFragmentMapping.remove(rack);
        }
      }
    }
    if (removed) {
      fragmentNum--;
    }
  }

  /**
   * Randomly select a fragment among the fragments stored on the host.
   * @param host
   * @return a randomly selected fragment
   */
  @Override
  public FragmentPair getHostLocalFragment(String host) {
    String normalizedHost = NetUtils.normalizeHost(host);
    if (fragmentHostMapping.containsKey(normalizedHost)) {
      Collection<FragmentsPerDisk> disks = fragmentHostMapping.get(normalizedHost).values();
      Iterator<FragmentsPerDisk> diskIterator = disks.iterator();
      int randomIndex = random.nextInt(disks.size());
      FragmentsPerDisk fragmentsPerDisk = null;
      for (int i = 0; i < randomIndex; i++) {
        fragmentsPerDisk = diskIterator.next();
      }

      if (fragmentsPerDisk != null) {
        Iterator<FragmentPair> fragmentIterator = fragmentsPerDisk.getFragmentPairIterator();
        if (fragmentIterator.hasNext()) {
          return fragmentIterator.next();
        }
      }
    }
    return null;
  }

  /**
   * Randomly select a fragment among the fragments stored at the disk of the host.
   * @param host
   * @param diskId
   * @return a randomly selected fragment
   */
  @Override
  public FragmentPair getHostLocalFragment(String host, Integer diskId) {
    String normalizedHost = NetUtils.normalizeHost(host);
    if (fragmentHostMapping.containsKey(normalizedHost)) {
      Map<Integer, FragmentsPerDisk> fragmentsPerDiskMap = fragmentHostMapping.get(normalizedHost);
      if (fragmentsPerDiskMap.containsKey(diskId)) {
        FragmentsPerDisk fragmentsPerDisk = fragmentsPerDiskMap.get(diskId);
        if (!fragmentsPerDisk.isEmpty()) {
          return fragmentsPerDisk.getFragmentPairIterator().next();
        }
      }
    }
    return null;
  }

  /**
   * Randomly select a fragment among the fragments stored on nodes of the same rack with the host.
   * @param host
   * @return a randomly selected fragment
   */
  @Override
  public FragmentPair getRackLocalFragment(String host) {
    String rack = RackResolver.resolve(host).getNetworkLocation();
    if (rackFragmentMapping.containsKey(rack)) {
      Set<FragmentPair> fragmentPairs = rackFragmentMapping.get(rack);
      if (!fragmentPairs.isEmpty()) {
        return fragmentPairs.iterator().next();
      }
    }
    return null;
  }

  /**
   * Randomly select a fragment among the total fragments.
   * @return a randomly selected fragment
   */
  @Override
  public FragmentPair getRandomFragment() {
    if (!fragmentHostMapping.isEmpty()) {
      return fragmentHostMapping.values().iterator().next().values().iterator().next().getFragmentPairIterator().next();
    }
    return null;
  }

  @Override
  public FragmentPair[] getAllFragments() {
    List<FragmentPair> fragmentPairs = new ArrayList<FragmentPair>();
    for (Map<Integer, FragmentsPerDisk> eachDiskFragmentMap : fragmentHostMapping.values()) {
      for (FragmentsPerDisk fragmentsPerDisk : eachDiskFragmentMap.values()) {
        fragmentPairs.addAll(fragmentsPerDisk.fragmentPairSet);
      }
    }
    return fragmentPairs.toArray(new FragmentPair[fragmentPairs.size()]);
  }

  @Override
  public int size() {
    return fragmentNum;
  }
}
