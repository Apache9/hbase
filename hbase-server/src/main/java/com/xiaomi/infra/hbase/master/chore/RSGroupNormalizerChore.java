package com.xiaomi.infra.hbase.master.chore;

import static org.apache.hadoop.hbase.rsgroup.RSGroupInfo.DEFAULT_GROUP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManagerImpl;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RSGroupNormalizerChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(RSGroupNormalizerChore.class);

  public static final String NORMALIZER_CHORE_PERIOD_KEY = "hbase.rsgroup.normalizer.chore.period";
  public static final int NORMALIZER_CHORE_PERIOD_DEFAULT = (int) TimeUnit.MINUTES.toMillis(5);

  public static final String CONSECUTIVE_TIMES = "hbase.rsgroup.normalizer.chore.consecutive.times";
  public static final Integer CONSECUTIVE_TIMES_DEFAULT = 2;

  public static final String REQUIRED_SERVER_NUM = "hbase.rsgroup.required.server.num";

  private Configuration conf;

  private HMaster master;

  private RSGroupInfoManagerImpl rsGroupInfoManager;

  private int maxConsecutiveHitTimes;

  private Map<String, Integer> consecutiveBadGroups = new HashMap<>();

  public RSGroupNormalizerChore(HMaster master, int period) {
    super("RSGroupNormalizerChore", master, period);
    this.conf = master.getConfiguration();
    maxConsecutiveHitTimes = conf.getInt(CONSECUTIVE_TIMES, CONSECUTIVE_TIMES_DEFAULT);
    this.master = master;
    if (master.getRSGroupInfoManager() instanceof RSGroupInfoManagerImpl) {
      this.rsGroupInfoManager = (RSGroupInfoManagerImpl) master.getRSGroupInfoManager();
    }
  }

  @Override
  protected void chore() {
    if (!RSGroupUtil.isRSGroupEnabled(this.conf)) {
      return;
    }
    if (rsGroupInfoManager == null) {
      LOG.warn("RSGroup is enabled, but rsGroupInfoManager is null");
      return;
    }
    Pair<List<Plan>, List<Plan>> plans = findUnqualifiedGroups();
    reduceGroupServerNum(filterConsecutiveHitPlans(plans.getFirst()));
    increaseGroupServerNum(filterConsecutiveHitPlans(plans.getSecond()));
  }

  private Pair<List<Plan>, List<Plan>> findUnqualifiedGroups() {
    List<Plan> toReduce = new ArrayList<>();
    List<Plan> toIncrease = new ArrayList<>();
    Set<Address> onlineServers = getOnlineServers();

    rsGroupInfoManager.listRSGroups().stream()
        .filter(group -> !DEFAULT_GROUP.equals(group.getName()))
        .forEach(group -> {
          Integer requiredServerNum = getRequiredServerNum(group);
          if (null != requiredServerNum) {
            HashSet<Address> serversInGroup = new HashSet<>(group.getServers());
            serversInGroup.retainAll(onlineServers);
            if (serversInGroup.size() > requiredServerNum) {
              toReduce.add(new Plan(group, serversInGroup.size() - requiredServerNum));
            } else if (serversInGroup.size() < requiredServerNum) {
              toIncrease.add(new Plan(group, requiredServerNum - serversInGroup.size()));
            }
          }
        });

    Set<String> groupNames = Stream.concat(toReduce.stream(), toIncrease.stream())
        .map(p -> p.rsGroupInfo.getName()).collect(Collectors.toSet());
    consecutiveBadGroups.entrySet().removeIf(group -> !groupNames.contains(group));
    groupNames.forEach(group ->
        consecutiveBadGroups.compute(group, (k, v) -> null == v ? 1 : v + 1));
    return Pair.newPair(toReduce, toIncrease);
  }

  private List<Plan> filterConsecutiveHitPlans(List<Plan> plans) {
    if (plans == null) {
      return Collections.EMPTY_LIST;
    }
    return plans.stream().filter(plan ->
        consecutiveBadGroups.get(plan.rsGroupInfo.getName()) >= maxConsecutiveHitTimes)
        .collect(Collectors.toList());
  }

  private void reduceGroupServerNum(List<Plan> plans) {
    if (plans == null || plans.isEmpty()) {
      return;
    }
    Set<Address> onlineServers = getOnlineServers();
    for (Plan plan : plans) {
      LOG.info("{} servers will be removed from group {}", plan.num, plan.rsGroupInfo.getName());
      String groupName = plan.rsGroupInfo.getName();
      RSGroupInfo rsGroup = rsGroupInfoManager.getRSGroup(groupName);
      Set<Address> serversInGroup = getOnlineServersInGroup(rsGroup, onlineServers);
      Integer requiredServerNum = getRequiredServerNum(rsGroup);
      // check again
      if (serversInGroup.size() <= requiredServerNum) {
        continue;
      }
      moveServers(serversInGroup, groupName, DEFAULT_GROUP,
          serversInGroup.size() - requiredServerNum);

      consecutiveBadGroups.remove(groupName);
    }
  }

  private void increaseGroupServerNum(List<Plan> plans) {
    if (plans == null || plans.isEmpty()) {
      return;
    }
    Set<Address> onlineServers = getOnlineServers();
    for (Plan plan : plans) {
      LOG.info("{} servers will be added to group {}", plan.num, plan.rsGroupInfo.getName());
      String groupName = plan.rsGroupInfo.getName();
      RSGroupInfo rsGroup = rsGroupInfoManager.getRSGroup(groupName);
      Integer requiredServerNum = getRequiredServerNum(rsGroup);
      Set<Address> serversInGroup = getOnlineServersInGroup(rsGroup, onlineServers);
      // check again
      if (serversInGroup.size() >= requiredServerNum) {
        continue;
      }

      RSGroupInfo defaultGroup = rsGroupInfoManager.getRSGroup(DEFAULT_GROUP);
      Set<Address> serversInDefaultGroup = getOnlineServersInGroup(defaultGroup, onlineServers);

      moveServers(serversInDefaultGroup, DEFAULT_GROUP, groupName,
          requiredServerNum - serversInGroup.size());

      consecutiveBadGroups.remove(groupName);
    }
  }

  private void moveServers(Set<Address> srcServers, String src, String dst, int num) {
    Set<Address> leastLoadServers = selectLeastLoadServers(srcServers, num);
    try {
      rsGroupInfoManager.moveServers(leastLoadServers, src, dst);
    } catch (IOException e) {
      LOG.error("Failed to move servers from {} to {}", src, dst, e);
    }
  }

  private Set<Address> selectLeastLoadServers(Set<Address> servers, int num) {
    Map<ServerName, ServerMetrics> onlineServers = master.getServerManager().getOnlineServers();
    return onlineServers.keySet().stream()
        .filter(s -> servers.contains(s.getAddress()))
        .sorted(Comparator.comparingLong(o -> onlineServers.get(o).getRequestCount()))
        .limit(num)
        .map(ServerName::getAddress)
        .collect(Collectors.toSet());
  }

  public static Integer getRequiredServerNum(RSGroupInfo rsGroupInfo) {
    String value = rsGroupInfo.getConfiguration().get(REQUIRED_SERVER_NUM);
    if (value == null) {
      return null;
    }
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      LOG.warn("Illegal format for properties {} valued {} in group {}", REQUIRED_SERVER_NUM,
          value, rsGroupInfo.getName(), e);
      return null;
    }
  }

  private Set<Address> getOnlineServers() {
    return master.getServerManager().getOnlineServers().keySet().stream()
        .map(ServerName::getAddress).collect(Collectors.toSet());
  }

  private Set<Address> getOnlineServersInGroup(RSGroupInfo groupInfo, Set<Address> onlineServers) {
    HashSet<Address> serversInGroup = new HashSet<>(groupInfo.getServers());
    serversInGroup.retainAll(onlineServers);
    return serversInGroup;
  }

  private class Plan {
    private RSGroupInfo rsGroupInfo;
    private int num;

    public Plan(RSGroupInfo rsGroupInfo, int num) {
      this.rsGroupInfo = rsGroupInfo;
      this.num = num;
    }
  }

}
