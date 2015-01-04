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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.StochasticLoadBalancer.Cluster.Action;
import org.apache.hadoop.hbase.master.StochasticLoadBalancer.Cluster.Action.Type;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

// back StochasticLoadBalancer in trunk to 0.94
public class StochasticLoadBalancer extends DefaultLoadBalancer {
  private static final int MIN_SERVER_BALANCE = 2;
  private static final List<HRegionInfo> EMPTY_REGION_LIST = new ArrayList<HRegionInfo>(0);

  protected static final String STEPS_PER_REGION_KEY =
      "hbase.master.balancer.stochastic.stepsPerRegion";
  protected static final String MAX_STEPS_KEY =
      "hbase.master.balancer.stochastic.maxSteps";
  protected static final String MAX_RUNNING_TIME_KEY =
      "hbase.master.balancer.stochastic.maxRunningTime";
  protected static final String KEEP_REGION_LOADS =
      "hbase.master.balancer.stochastic.numRegionLoadsToRemember";

  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final Log LOG = LogFactory.getLog(StochasticLoadBalancer.class);

  Map<String, Deque<RegionLoad>> loads = new HashMap<String, Deque<RegionLoad>>();

  private int maxSteps = 1000000;
  private int stepsPerRegion = 800;
  private long maxRunningTime = 30 * 1000 * 1; // 30 seconds.
  private int numRegionLoadsToRemember = 15;

  private CandidateGenerator[] candidateGenerators;
  private CostFromRegionLoadFunction[] regionLoadFunctions;
  private CostFunction[] costFunctions;
  private LocalityCostFunction localityCost;
  private LocalityBasedCandidateGenerator localityCandidateGenerator;

  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);

    maxSteps = conf.getInt(MAX_STEPS_KEY, maxSteps);

    stepsPerRegion = conf.getInt(STEPS_PER_REGION_KEY, stepsPerRegion);
    maxRunningTime = conf.getLong(MAX_RUNNING_TIME_KEY, maxRunningTime);

    numRegionLoadsToRemember = conf.getInt(KEEP_REGION_LOADS, numRegionLoadsToRemember);

    LOG.info("loading config, maxSteps=" + maxSteps + ", stepsPerRegion=" + stepsPerRegion
        + ", maxRunningTime=" + maxRunningTime + ", numRegionLoadsToRemember="
        + numRegionLoadsToRemember);

    if (localityCandidateGenerator == null) {
      localityCandidateGenerator = new LocalityBasedCandidateGenerator(services);
    }
    if (candidateGenerators == null) {
      candidateGenerators = new CandidateGenerator[] {
          new RandomCandidateGenerator(),
          new LoadCandidateGenerator(),
          localityCandidateGenerator
      };
    }

    regionLoadFunctions = new CostFromRegionLoadFunction[] {
      new ReadRequestCostFunction(conf),
      new WriteRequestCostFunction(conf)
    };
    localityCost = new LocalityCostFunction(conf, services);
    costFunctions = new CostFunction[]{
      new RegionCountSkewCostFunction(conf),
      new MoveCostFunction(conf),
      new TableSkewCostFunction(conf),
      regionLoadFunctions[0],
      regionLoadFunctions[1],
      localityCost
    };
  }

  @Override
  public synchronized void setClusterStatus(ClusterStatus st) {
    super.setClusterStatus(st);
    updateRegionLoad();
    for(CostFromRegionLoadFunction cost : regionLoadFunctions) {
      cost.setClusterStatus(st);
    }
  }

  @Override
  public synchronized void setMasterServices(MasterServices masterServices) {
    super.setMasterServices(masterServices);
    this.localityCost.setServices(masterServices);
    this.localityCandidateGenerator.setServices(masterServices);
  }

  protected boolean needsBalance(Cluster c) {
    ClusterLoadState cs = new ClusterLoadState(c.clusterState);
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not running balancer because only " + cs.getNumServers()
            + " active regionserver(s)");
      }
      return false;
    }
    
    // Check if we even need to do any load balancing
    // HBASE-3681 check sloppiness first
    float average = cs.getLoadAverage(); // for logging
    int floor = (int) Math.floor(average * (1 - slop));
    int ceiling = (int) Math.ceil(average * (1 + slop));
    if (!(cs.getMaxLoad() > ceiling || cs.getMinLoad() < floor)) {
      NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad = cs.getServersByLoad();
      // If nothing to balance, then don't say anything unless trace-level logging.
      LOG.info("Skipping load balancing because balanced cluster; " + "servers="
          + cs.getNumServers() + " regions=" + cs.getNumRegions() + " average=" + average
          + " mostloaded=" + serversByLoad.lastKey().getLoad() + " leastloaded="
          + serversByLoad.firstKey().getLoad() + ", slop=" + slop);
      return false;
    }
    return true;
  }
  
  /**
   * Given the cluster state this will try and approach an optimal balance. This
   * should always approach the optimal state given enough steps.
   */
  @Override
  public synchronized List<RegionPlan> balanceCluster(Map<ServerName,
    List<HRegionInfo>> clusterState) {

    //The clusterState that is given to this method contains the state
    //of all the regions in the table(s) (that's true today)
    // Keep track of servers to iterate through them.
    Cluster cluster = null;
    try {
      cluster = new Cluster(getConf(), this.services, clusterState, loads);
    } catch (IOException e) {
      LOG.error("construct cluster state fail", e);
      return new ArrayList<RegionPlan>();
    }
    
    // should not decide balance only from region count skew
    /*
    if (!needsBalance(cluster)) {
      return null;
    }
    */

    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    initCosts(cluster);

    double currentCost = computeCost(cluster, Double.MAX_VALUE);
    LOG.info("start StochasticLoadBalancer.balaner, initCost=" + currentCost + ", functionCost="
        + functionCost() + ", costDetail:");
    LOG.info(functionCostDetail());
    
    double initCost = currentCost;
    double newCost = currentCost;

    long computedMaxSteps = Math.min(this.maxSteps,
        ((long)cluster.numRegions * (long)this.stepsPerRegion * (long)cluster.numServers));
    // Perform a stochastic walk to see if we can get a good fit.
    long step;

    for (step = 0; step < computedMaxSteps; step++) {
      int generatorIdx = RANDOM.nextInt(candidateGenerators.length);
      CandidateGenerator p = candidateGenerators[generatorIdx];
      Cluster.Action action = p.generate(cluster);

      if (action.type == Type.NULL) {
        continue;
      }

      cluster.doAction(action);
      updateCostsWithAction(cluster, action);
      newCost = computeCost(cluster, currentCost);
      
      if (newCost < currentCost) {
        currentCost = newCost;
        LOG.info("cost decreased, action=" + action.toString(cluster) + ", oldCost=" + currentCost
            + ", newCost=" + newCost + ", functionCost=" + functionCost());
      } else {
        // Put things back the way they were before.
        // TODO: undo by remembering old values
        Action undoAction = action.undoAction();
        cluster.doAction(undoAction);
        updateCostsWithAction(cluster, undoAction);
      }

      if (EnvironmentEdgeManager.currentTimeMillis() - startTime >
          maxRunningTime) {
        break;
      }
    }

    long endTime = EnvironmentEdgeManager.currentTimeMillis();

    if (initCost > currentCost) {
      List<RegionPlan> plans = createRegionPlans(cluster);
      LOG.info("Finished computing new load balance plan.  Computation took "
          + (endTime - startTime) + "ms to try " + step
          + " different iterations.  Found a solution that moves " + plans.size()
          + " regions; Going from a computed cost of " + initCost + " to a new cost of "
          + currentCost + ", functionCost=" + functionCost() + ", costDetail:");
      LOG.info(functionCostDetail());
      return plans;
    }
    LOG.info("Finished computing new load balance plan, Could not find a better "
        + "load balance plan.  Tried " + step + " different configurations in " + (endTime - startTime)
        + "ms, and did not find anything with a computed cost less than " + initCost);
    return null;
  }

  /**
   * Create all of the RegionPlan's needed to move from the initial cluster state to the desired
   * state.
   *
   * @param cluster The state of the cluster
   * @return List of RegionPlan's that represent the moves needed to get to desired final state.
   */
  private List<RegionPlan> createRegionPlans(Cluster cluster) {
    List<RegionPlan> plans = new LinkedList<RegionPlan>();
    for (int regionIndex = 0;
         regionIndex < cluster.regionIndexToServerIndex.length; regionIndex++) {
      int initialServerIndex = cluster.initialRegionIndexToServerIndex[regionIndex];
      int newServerIndex = cluster.regionIndexToServerIndex[regionIndex];

      if (initialServerIndex != newServerIndex) {
        HRegionInfo region = cluster.regions[regionIndex];
        ServerName initialServer = cluster.servers[initialServerIndex];
        ServerName newServer = cluster.servers[newServerIndex];

        if (LOG.isTraceEnabled()) {
          LOG.trace("Moving Region " + region.getEncodedName() + " from server "
              + initialServer.getHostname() + " to " + newServer.getHostname());
        }
        RegionPlan rp = new RegionPlan(region, initialServer, newServer);
        plans.add(rp);
      }
    }
    return plans;
  }

  /**
   * Store the current region loads.
   */
  private synchronized void updateRegionLoad() {
    // We create a new hashmap so that regions that are no longer there are removed.
    // However we temporarily need the old loads so we can use them to keep the rolling average.
    Map<String, Deque<RegionLoad>> oldLoads = loads;
    loads = new HashMap<String, Deque<RegionLoad>>();

    for (ServerName sn : status.getServers()) {
      HServerLoad sl = status.getLoad(sn);
      if (sl == null) {
        continue;
      }
      for (Entry<byte[], RegionLoad> entry : sl.getRegionsLoad().entrySet()) {
        Deque<RegionLoad> rLoads = oldLoads.get(Bytes.toString(entry.getKey()));
        if (rLoads == null) {
          // There was nothing there
          rLoads = new ArrayDeque<RegionLoad>();
        } else if (rLoads.size() >= numRegionLoadsToRemember) {
          rLoads.remove();
        }
        rLoads.add(entry.getValue());
        loads.put(Bytes.toString(entry.getKey()), rLoads);

      }
    }

    for(CostFromRegionLoadFunction cost : regionLoadFunctions) {
      cost.setLoads(loads);
    }
  }

  protected void initCosts(Cluster cluster) {
    for (CostFunction c:costFunctions) {
      c.init(cluster);
    }
  }

  protected void updateCostsWithAction(Cluster cluster, Action action) {
    for (CostFunction c : costFunctions) {
      c.postAction(action);
    }
  }

  /**
   * This is the main cost function.  It will compute a cost associated with a proposed cluster
   * state.  All different costs will be combined with their multipliers to produce a double cost.
   *
   * @param cluster The state of the cluster
   * @param previousCost the previous cost. This is used as an early out.
   * @return a double of a cost associated with the proposed cluster state.  This cost is an
   *         aggregate of all individual cost functions.
   */
  protected double computeCost(Cluster cluster, double previousCost) {
    double total = 0;

    for (CostFunction c:costFunctions) {
      if (c.getMultiplier() <= 0) {
        continue;
      }

      total += c.getMultiplier() * c.cost();

      if (total > previousCost) {
        return total;
      }
    }
    return total;
  }
  
  protected String functionCost() {
    StringBuilder builder = new StringBuilder();
    for (CostFunction c:costFunctions) {
      builder.append(c.getClass().getSimpleName());
      builder.append(" : (");
      builder.append(c.getMultiplier());
      builder.append(", ");
      builder.append(c.cost());
      builder.append("); ");
    }
    return builder.toString();
  }
  
  protected String functionCostDetail() {
    StringBuilder builder = new StringBuilder();
    for (CostFunction c:costFunctions) {
      builder.append(c.costDetail());
    }
    return builder.toString();
  }

  // Action generators
  /** Generates a candidate action to be applied to the cluster for cost function search */
  abstract static class CandidateGenerator {
    abstract Cluster.Action generate(Cluster cluster);

    /**
     * From a list of regions pick a random one. Null can be returned which
     * {@link StochasticLoadBalancer#balanceCluster(Map)} recognize as signal to try a region move
     * rather than swap.
     *
     * @param cluster        The state of the cluster
     * @param server         index of the server
     * @param chanceOfNoSwap Chance that this will decide to try a move rather
     *                       than a swap.
     * @return a random {@link HRegionInfo} or null if an asymmetrical move is
     *         suggested.
     */
    protected int pickRandomRegion(Cluster cluster, int server, double chanceOfNoSwap) {
      // Check to see if this is just a move.
      if (cluster.regionsPerServer[server].length == 0 || RANDOM.nextFloat() < chanceOfNoSwap) {
        // signal a move only.
        return -1;
      }
      int rand = RANDOM.nextInt(cluster.regionsPerServer[server].length);
      return cluster.regionsPerServer[server][rand];

    }
    protected int pickRandomServer(Cluster cluster) {
      if (cluster.numServers < 1) {
        return -1;
      }

      return RANDOM.nextInt(cluster.numServers);
    }

    protected int pickOtherRandomServer(Cluster cluster, int serverIndex) {
      if (cluster.numServers < 2) {
        return -1;
      }
      while (true) {
        int otherServerIndex = pickRandomServer(cluster);
        if (otherServerIndex != serverIndex) {
          return otherServerIndex;
        }
      }
    }

    protected Cluster.Action pickRandomRegions(Cluster cluster,
                                                       int thisServer,
                                                       int otherServer) {
      if (thisServer < 0 || otherServer < 0) {
        return Cluster.NullAction;
      }

      // Decide who is most likely to need another region
      int thisRegionCount = cluster.getNumRegions(thisServer);
      int otherRegionCount = cluster.getNumRegions(otherServer);

      // Assign the chance based upon the above
      double thisChance = (thisRegionCount > otherRegionCount) ? 0 : 0.5;
      double otherChance = (thisRegionCount <= otherRegionCount) ? 0 : 0.5;

      int thisRegion = pickRandomRegion(cluster, thisServer, thisChance);
      int otherRegion = pickRandomRegion(cluster, otherServer, otherChance);

      return getAction(thisServer, thisRegion, otherServer, otherRegion);
    }

    protected Cluster.Action getAction (int fromServer, int fromRegion,
        int toServer, int toRegion) {
      if (fromServer < 0 || toServer < 0) {
        return Cluster.NullAction;
      }
      if (fromRegion > 0 && toRegion > 0) {
        return new Cluster.SwapRegionsAction(fromServer, fromRegion,
          toServer, toRegion);
      } else if (fromRegion > 0) {
        return new Cluster.MoveRegionAction(fromRegion, fromServer, toServer);
      } else if (toRegion > 0) {
        return new Cluster.MoveRegionAction(toRegion, toServer, fromServer);
      } else {
        return Cluster.NullAction;
      }
    }
  }

  static class RandomCandidateGenerator extends CandidateGenerator {

    @Override
    Cluster.Action generate(Cluster cluster) {

      int thisServer = pickRandomServer(cluster);

      // Pick the other server
      int otherServer = pickOtherRandomServer(cluster, thisServer);

      return pickRandomRegions(cluster, thisServer, otherServer);
    }
  }

  static class LoadCandidateGenerator extends CandidateGenerator {

    @Override
    Cluster.Action generate(Cluster cluster) {
      cluster.sortServersByRegionCount();
      int thisServer = pickMostLoadedServer(cluster, -1);
      int otherServer = pickLeastLoadedServer(cluster, thisServer);

      return pickRandomRegions(cluster, thisServer, otherServer);
    }

    private int pickLeastLoadedServer(final Cluster cluster, int thisServer) {
      Integer[] servers = cluster.serverIndicesSortedByRegionCount;

      int index = 0;
      while (servers[index] == null || servers[index] == thisServer) {
        index++;
        if (index == servers.length) {
          return -1;
        }
      }
      return servers[index];
    }

    private int pickMostLoadedServer(final Cluster cluster, int thisServer) {
      Integer[] servers = cluster.serverIndicesSortedByRegionCount;

      int index = servers.length - 1;
      while (servers[index] == null || servers[index] == thisServer) {
        index--;
        if (index < 0) {
          return -1;
        }
      }
      return servers[index];
    }
  }

  
  static class LocalityBasedCandidateGenerator extends CandidateGenerator {

    private MasterServices masterServices;

    LocalityBasedCandidateGenerator(MasterServices masterServices) {
      this.masterServices = masterServices;
    }

    @Override
    Cluster.Action generate(Cluster cluster) {
      if (this.masterServices == null) {
        return Cluster.NullAction;
      }
      // Pick a random region server
      int thisServer = pickRandomServer(cluster);

      // Pick a random region on this server
      int thisRegion = pickRandomRegion(cluster, thisServer, 0.0f);

      if (thisRegion == -1) {
        return Cluster.NullAction;
      }

      // Pick the server with the highest locality
      int otherServer = pickHighestLocalityServer(cluster, thisServer, thisRegion);

      if (otherServer == -1) {
        return Cluster.NullAction;
      }

      // pick an region on the other server to potentially swap
      int otherRegion = this.pickRandomRegion(cluster, otherServer, 0.5f);

      return getAction(thisServer, thisRegion, otherServer, otherRegion);
    }

    private int pickHighestLocalityServer(Cluster cluster, int thisServer, int thisRegion) {
      int[] regionLocations = cluster.regionLocations[thisRegion];

      if (regionLocations == null || regionLocations.length <= 1) {
        return pickOtherRandomServer(cluster, thisServer);
      }

      for (int loc : regionLocations) {
        if (loc == cluster.serverIndexToHostIndex[thisServer]) {
          return -1;
        }
        if (loc >= 0) { // find the first suitable host
          return cluster.hostServerSelector[loc].select();
        }
      }

      // no location found
      return pickOtherRandomServer(cluster, thisServer);
    }

    void setServices(MasterServices services) {
      this.masterServices = services;
    }
  }
  
  // Cost Function classes
  /**
   * Base class of StochasticLoadBalancer's Cost Functions.
   */
  abstract static class CostFunction {

    private float multiplier = 0;

    protected Cluster cluster;

    CostFunction(Configuration c) {

    }

    float getMultiplier() {
      return multiplier;
    }

    void setMultiplier(float m) {
      this.multiplier = m;
    }

    /** Called once per LB invocation to give the cost function
     * to initialize it's state, and perform any costly calculation.
     */
    void init(Cluster cluster) {
      this.cluster = cluster;
    }

    // do post action when necessary
    void postAction(Action action) {
    }

    abstract double cost();
    
    String costDetail() {
      return this.getClass().getSimpleName() + " : null";
    }

    /**
     * Function to compute a scaled cost using DescriptiveStatistics. It
     * assumes that this is a zero sum set of costs.  It assumes that the worst case
     * possible is all of the elements in one region server and the rest having 0.
     *
     * @param stats the costs
     * @return a scaled set of costs.
     */
    protected double costFromArray(double[] stats) {
      double totalCost = 0;
      double total = getSum(stats);

      double count = stats.length;
      double mean = total/count;

      // Compute max as if all region servers had 0 and one had the sum of all costs.  This must be
      // a zero sum cost for this to make sense.
      double max = ((count - 1) * mean) + (total - mean);

      // It's possible that there aren't enough regions to go around
      double min;
      if (count > total) {
        min = ((count - total) * mean) + ((1 - mean) * total);
      } else {
        // Some will have 1 more than everything else.
        int numHigh = (int) (total - (Math.floor(mean) * count));
        int numLow = (int) (count - numHigh);

        min = (numHigh * (Math.ceil(mean) - mean)) + (numLow * (mean - Math.floor(mean)));

      }
      min = Math.max(0, min);
      for (int i=0; i<stats.length; i++) {
        double n = stats[i];
        double diff = Math.abs(mean - n);
        totalCost += diff;
      }

      return scale(min, max, totalCost);
    }

    private double getSum(double[] stats) {
      double total = 0;
      for(double s:stats) {
        total += s;
      }
      return total;
    }
    
    protected String serverStats(ServerName[] servers, double[] stats) {
      StringBuilder builder = new StringBuilder();
      builder.append(this.getClass().getSimpleName());
      builder.append(" :\n");
      for (int i = 0; i < servers.length; ++i) {
        builder.append(servers[i]);
        builder.append(" : ");
        builder.append(stats[i]);
        builder.append("\n");
      }
      return builder.toString();
    }
    
    /**
     * Scale the value between 0 and 1.
     *
     * @param min   Min value
     * @param max   The Max value
     * @param value The value to be scaled.
     * @return The scaled value.
     */
    protected double scale(double min, double max, double value) {
      if (max <= min || value <= min) {
        return 0;
      }
      if ((max - min) == 0) return 0;

      return Math.max(0d, Math.min(1d, (value - min) / (max - min)));
    }
  }

  /**
   * Given the starting state of the regions and a potential ending state
   * compute cost based upon the number of regions that have moved.
   */
  static class MoveCostFunction extends CostFunction {
    private static final String MOVE_COST_KEY = "hbase.master.balancer.stochastic.moveCost";
    private static final String MAX_MOVES_PERCENT_KEY =
        "hbase.master.balancer.stochastic.maxMovePercent";
    private static final float DEFAULT_MOVE_COST = 100;
    private static final int DEFAULT_MAX_MOVES = 600;
    private static final float DEFAULT_MAX_MOVE_PERCENT = 0.25f;

    private final float maxMovesPercent;

    MoveCostFunction(Configuration conf) {
      super(conf);

      // Move cost multiplier should be the same cost or higher than the rest of the costs to ensure
      // that large benefits are need to overcome the cost of a move.
      this.setMultiplier(conf.getFloat(MOVE_COST_KEY, DEFAULT_MOVE_COST));
      // What percent of the number of regions a single run of the balancer can move.
      maxMovesPercent = conf.getFloat(MAX_MOVES_PERCENT_KEY, DEFAULT_MAX_MOVE_PERCENT);
    }

    @Override
    double cost() {
      // Try and size the max number of Moves, but always be prepared to move some.
      int maxMoves = Math.max((int) (cluster.numRegions * maxMovesPercent),
          DEFAULT_MAX_MOVES);

      double moveCost = cluster.numMovedRegions;
      
      // Don't let this single balance move more than the max moves.
      // This allows better scaling to accurately represent the actual cost of a move.
      if (moveCost > maxMoves) {
        return 1000000;   // return a number much greater than any of the other cost
      }

      return scale(0, cluster.numRegions, moveCost);
    }
    
    @Override
    String costDetail() {
      return this.getClass().getSimpleName() + " : " + cluster.numMovedRegions + " moves";
    }
  }

  /**
   * Compute the cost of a potential cluster state from skew in number of
   * regions on a cluster.
   */
  static class RegionCountSkewCostFunction extends CostFunction {
    private static final String REGION_COUNT_SKEW_COST_KEY =
        "hbase.master.balancer.stochastic.regionCountCost";
    private static final float DEFAULT_REGION_COUNT_SKEW_COST = 500;

    private double[] stats = null;

    RegionCountSkewCostFunction(Configuration conf) {
      super(conf);
      // Load multiplier should be the greatest as it is the most general way to balance data.
      this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));
    }

    @Override
    double cost() {
      if (stats == null || stats.length != cluster.numServers) {
        stats = new double[cluster.numServers];
      }

      for (int i =0; i < cluster.numServers; i++) {
        stats[i] = cluster.regionsPerServer[i].length;
      }

      return costFromArray(stats);
    }
    
    @Override
    String costDetail() {
      return serverStats(cluster.servers, stats);
    }
  }
  
  /**
   * Compute the cost of a potential cluster configuration based upon how evenly
   * distributed tables are.
   */
  static class TableSkewCostFunction extends CostFunction {

    private static final String TABLE_SKEW_COST_KEY =
        "hbase.master.balancer.stochastic.tableSkewCost";
    private static final float DEFAULT_TABLE_SKEW_COST = 35;

    TableSkewCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(TABLE_SKEW_COST_KEY, DEFAULT_TABLE_SKEW_COST));
    }

    @Override
    double cost() {
      double max = cluster.numRegions;
      double min = ((double) cluster.numRegions) / cluster.numServers;
      double value = 0;

      for (int i = 0; i < cluster.numMaxRegionsPerTable.length; i++) {
        value += cluster.numMaxRegionsPerTable[i];
      }

      return scale(min, max, value);
    }
    
    @Override
    String costDetail() {
      StringBuilder builder = new StringBuilder();
      builder.append(this.getClass().getSimpleName());
      builder.append(" :\n");
      for (int i = 0; i < cluster.numMaxRegionsPerTable.length; ++i) {
        builder.append(cluster.tables.get(i));
        builder.append(" : ");
        builder.append(cluster.numMaxRegionsPerTable[i]);
        builder.append("\n");
      }
      return builder.toString();
    }
  }


  /**
   * Base class the allows writing costs functions from rolling average of some
   * number from RegionLoad.
   */
  abstract static class CostFromRegionLoadFunction extends CostFunction {

    private ClusterStatus clusterStatus = null;
    private Map<String, Deque<RegionLoad>> loads = null;
    private double[] stats = null;
    CostFromRegionLoadFunction(Configuration conf) {
      super(conf);
    }

    void setClusterStatus(ClusterStatus status) {
      this.clusterStatus = status;
    }

    void setLoads(Map<String, Deque<RegionLoad>> l) {
      this.loads = l;
    }

    @Override
    double cost() {
      if (clusterStatus == null || loads == null) {
        return 0;
      }

      if (stats == null || stats.length != cluster.numServers) {
        stats = new double[cluster.numServers];
      }

      for (int i =0; i < stats.length; i++) {
        //Cost this server has from RegionLoad
        long cost = 0;

        // for every region on this server get the rl
        for(int regionIndex:cluster.regionsPerServer[i]) {
          Collection<RegionLoad> regionLoadList =  cluster.regionLoads[regionIndex];

          // Now if we found a region load get the type of cost that was requested.
          if (regionLoadList != null) {
            cost += getRegionLoadCost(regionLoadList);
          }
        }

        // Add the total cost to the stats.
        stats[i] = cost;
      }

      // Now return the scaled cost from data held in the stats object.
      return costFromArray(stats);
    }

    protected double getRegionLoadCost(Collection<RegionLoad> regionLoadList) {
      double cost = 0;

      RegionLoad lastLoad = null;
      for (RegionLoad rl : regionLoadList) {
        if (lastLoad == null) {
          lastLoad = rl;
          continue;
        }
        
        double toAdd = getCostFromRl(rl, lastLoad);
        lastLoad = rl;
        
        if (cost == 0) {
          cost = toAdd;
        } else {
          cost = (.5 * cost) + (.5 * toAdd);
        }
      }

      return cost;
    }
    
    @Override
    String costDetail() {
      return serverStats(cluster.servers, stats);
    }

    protected abstract double getCostFromRl(RegionLoad rl, RegionLoad lastLoad);
  }

  /**
   * Compute the cost of total number of read requests  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */

  static class ReadRequestCostFunction extends CostFromRegionLoadFunction {

    private static final String READ_REQUEST_COST_KEY =
        "hbase.master.balancer.stochastic.readRequestCost";
    private static final float DEFAULT_READ_REQUEST_COST = 5;

    ReadRequestCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(READ_REQUEST_COST_KEY, DEFAULT_READ_REQUEST_COST));
    }


    @Override
    protected double getCostFromRl(RegionLoad rl, RegionLoad lastLoad) {
      // the region must be reopened. This is not accurate enough to compute real-time qps.
      // TODO: report read/write qps in RegionLoad
      if (rl.getReadRequestsCount() < lastLoad.getReadRequestsCount()) {
        return rl.getReadRequestsCount();
      } else {
        return rl.getReadRequestsCount() - lastLoad.getReadRequestsCount();
      }
    }
  }

  /**
   * Compute the cost of total number of write requests.  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */
  static class WriteRequestCostFunction extends CostFromRegionLoadFunction {

    private static final String WRITE_REQUEST_COST_KEY =
        "hbase.master.balancer.stochastic.writeRequestCost";
    private static final float DEFAULT_WRITE_REQUEST_COST = 5;

    WriteRequestCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(WRITE_REQUEST_COST_KEY, DEFAULT_WRITE_REQUEST_COST));
    }

    @Override
    protected double getCostFromRl(RegionLoad rl, RegionLoad lastLoad) {
      if (rl.getWriteRequestsCount() < lastLoad.getWriteRequestsCount()) {
        return rl.getWriteRequestsCount();
      } else {
        return rl.getWriteRequestsCount() - lastLoad.getWriteRequestsCount();
      }
    }
  }

  /**
   * Compute a cost of a potential cluster configuration based upon where
   * {@link org.apache.hadoop.hbase.regionserver.StoreFile}s are located.
   */
  static class LocalityCostFunction extends CostFunction {

    private static final String LOCALITY_COST_KEY = "hbase.master.balancer.stochastic.localityCost";
    private static final float DEFAULT_LOCALITY_COST = 25;

    private MasterServices services;

    LocalityCostFunction(Configuration conf, MasterServices srv) {
      super(conf);
      this.setMultiplier(conf.getFloat(LOCALITY_COST_KEY, DEFAULT_LOCALITY_COST));
      this.services = srv;
    }

    void setServices(MasterServices srvc) {
      this.services = srvc;
    }

    @Override
    double cost() {
      double max = 0;
      double cost = 0;

      // If there's no master so there's no way anything else works.
      if (this.services == null) {
        return cost;
      }

      for (int i = 0; i < cluster.regionLocations.length; i++) {
        max += 1;
        int hostIndex = cluster.serverIndexToHostIndex[cluster.regionIndexToServerIndex[i]];
        int[] regionLocations = cluster.regionLocations[i];

        // If we can't find where the data is getTopBlock returns null.
        // so count that as being the best possible.
        if (regionLocations == null) {
          continue;
        }

        int index = -1;
        for (int j = 0; j < regionLocations.length; j++) {
          if (regionLocations[j] >= 0 && regionLocations[j] == hostIndex) {
            index = j;
            break;
          }
        }

        if (index < 0) {
          if (regionLocations.length > 0) {
            cost += 1;
          }
        } else {
          cost += (double) index / (double) regionLocations.length;
        }
      }
      return scale(0, max, cost);
    }    
  }
  
  protected static class RoundRobinSelector<R> {
    private ArrayList<R> candidates;
    private int next;

    public RoundRobinSelector() {
      this.candidates = new ArrayList<R>();
      this.next = 0;
    }

    public void add(R candidate) {
      this.candidates.add(candidate);
    }

    public R select() {
      if (candidates.size() == 0) return null;
      next = (next) % candidates.size();
      return candidates.get(next++);
    }
  }

  // cluster statistics when balancing
  protected static class Cluster extends Configured {
    ServerName[] servers;
    String[] hosts;
    HRegionInfo[] regions;
    Deque<RegionLoad>[] regionLoads;
    ArrayList<String> tables;
    MasterServices services;
    
    int[]   serverIndexToHostIndex;      //serverIndex -> host index
    int[][] regionsPerServer;            //serverIndex -> region list
    int[]   regionIndexToServerIndex;    //regionIndex -> serverIndex
    int[]   initialRegionIndexToServerIndex;    //regionIndex -> serverIndex (initial cluster state)
    Integer[] serverIndicesSortedByRegionCount;
    int[][] regionLocations; //regionIndex -> list of serverIndex sorted by locality
    int[][] numRegionsPerServerPerTable; //serverIndex -> tableIndex -> # regions
    int[]   numMaxRegionsPerTable;       //tableIndex -> max number of regions in a single RS
    int[]   regionIndexToTableIndex;     //regionIndex -> tableIndex
    
    Map<String, Integer> serversToIndex;
    Map<String, Integer> hostsToIndex;
    Map<String, Integer> tablesToIndex;
    Map<HRegionInfo, Integer> regionsToIndex;
    RoundRobinSelector<Integer>[] hostServerSelector;

    int numServers;
    int numHosts;
    int numTables;
    int numRegions;

    int numMovedRegions = 0; //num moved regions from the initial configuration
    // num of moved regions away from master that should be on the master
    int numMovedMetaRegions = 0;       //num of moved regions that are META
    Map<ServerName, List<HRegionInfo>> clusterState;

    protected Cluster(Configuration conf, MasterServices masterService,
        Map<ServerName, List<HRegionInfo>> clusterState, Map<String, Deque<RegionLoad>> loads)
        throws IOException {
      this(conf, masterService, null, clusterState, loads);
    }

    @SuppressWarnings("unchecked")
    protected Cluster(Configuration conf,
        MasterServices masterService,
        Collection<HRegionInfo> unassignedRegions,
        Map<ServerName, List<HRegionInfo>> clusterState,
        Map<String, Deque<RegionLoad>> loads) throws IOException {
      super(conf);
      this.services = masterService;
      if (unassignedRegions == null) {
        unassignedRegions = EMPTY_REGION_LIST;
      }

      serversToIndex = new HashMap<String, Integer>();
      hostsToIndex = new HashMap<String, Integer>();
      tablesToIndex = new HashMap<String, Integer>();
      
      this.clusterState = clusterState;

      // Use servername and port as there can be dead servers in this list. We want everything with
      // a matching hostname and port to have the same index.
      for (ServerName sn : clusterState.keySet()) {
        if (serversToIndex.get(sn.getHostAndPort()) == null) {
          serversToIndex.put(sn.getHostAndPort(), numServers++);
        }
        if (!hostsToIndex.containsKey(sn.getHostname())) {
          hostsToIndex.put(sn.getHostname(), numHosts++);
        }
      }
      
      hostServerSelector = new RoundRobinSelector[numHosts];
      for (int i = 0; i < numHosts; ++i) {
        hostServerSelector[i] = new RoundRobinSelector<Integer>();
      }

      // Count how many regions there are.
      numRegions = 0;
      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        numRegions += entry.getValue().size();
      }
      numRegions += unassignedRegions.size();

      regionsToIndex = new HashMap<HRegionInfo, Integer>(numRegions);
      servers = new ServerName[numServers];
      regions = new HRegionInfo[numRegions];
      regionIndexToServerIndex = new int[numRegions];
      regionIndexToTableIndex = new int[numRegions];
      initialRegionIndexToServerIndex = new int[numRegions];
      regionLoads = new Deque[numRegions];
      regionLocations = new int[numRegions][];
      serverIndicesSortedByRegionCount = new Integer[numServers];
      serverIndexToHostIndex = new int[numServers];
      regionsPerServer = new int[numServers][];

      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        int serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());

        // keep the servername if this is the first server name for this hostname
        // or this servername has the newest startcode.
        if (servers[serverIndex] == null ||
            servers[serverIndex].getStartcode() < entry.getKey().getStartcode()) {
          servers[serverIndex] = entry.getKey();
        }

        if (regionsPerServer[serverIndex] != null) {
          // there is another server with the same hostAndPort in ClusterState.
          // allocate the array for the total size
          regionsPerServer[serverIndex] = new int[entry.getValue().size() + regionsPerServer[serverIndex].length];
        } else {
          regionsPerServer[serverIndex] = new int[entry.getValue().size()];
        }
        serverIndicesSortedByRegionCount[serverIndex] = serverIndex;
      }

      tables = new ArrayList<String>();
      int regionIndex = 0, regionPerServerIndex = 0;
      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        int serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());
        regionPerServerIndex = 0;

        for (HRegionInfo region : entry.getValue()) {
          registerRegion(region, regionIndex, serverIndex, loads);

          regionsPerServer[serverIndex][regionPerServerIndex++] = regionIndex;
          regionIndex++;
        }
        
        int hostIndex = hostsToIndex.get(entry.getKey().getHostname());
        serverIndexToHostIndex[serverIndex] = hostIndex;
        hostServerSelector[hostIndex].add(serverIndex);
      }
      for (HRegionInfo region : unassignedRegions) {
        registerRegion(region, regionIndex, -1, loads);
        regionIndex++;
      }
      
      numTables = tables.size();
      numRegionsPerServerPerTable = new int[numServers][numTables];

      for (int i = 0; i < numServers; i++) {
        for (int j = 0; j < numTables; j++) {
          numRegionsPerServerPerTable[i][j] = 0;
        }
      }

      for (int i=0; i < regionIndexToServerIndex.length; i++) {
        if (regionIndexToServerIndex[i] >= 0) {
          numRegionsPerServerPerTable[regionIndexToServerIndex[i]][regionIndexToTableIndex[i]]++;
        }
      }

      numMaxRegionsPerTable = new int[numTables];
      for (int serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
        for (int tableIndex = 0 ; tableIndex < numRegionsPerServerPerTable[serverIndex].length; tableIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
        }
      }
    }

    /** Helper for Cluster constructor to handle a region */
    private void registerRegion(HRegionInfo region, int regionIndex, int serverIndex,
        Map<String, Deque<RegionLoad>> loads) throws IOException {
      String tableName = region.getTableNameAsString();
      if (!tablesToIndex.containsKey(tableName)) {
        tables.add(tableName);
        tablesToIndex.put(tableName, tablesToIndex.size());
      }
      int tableIndex = tablesToIndex.get(tableName);
      
      regionsToIndex.put(region, regionIndex);
      regions[regionIndex] = region;
      regionIndexToServerIndex[regionIndex] = serverIndex;
      initialRegionIndexToServerIndex[regionIndex] = serverIndex;
      regionIndexToTableIndex[regionIndex] = tableIndex;
      
      // region load
      if (loads != null) {
        Deque<RegionLoad> rl = loads.get(region.getRegionNameAsString());
        // That could have failed if the RegionLoad is using the other regionName
        if (rl == null) {
          // Try getting the region load using encoded name.
          rl = loads.get(region.getEncodedName());
        }
        regionLoads[regionIndex] = rl;
      }
      
      // compute region locality
      List<String> loc = getTopHosts(getConf(), region.getTableName(), region.getEncodedName());
      regionLocations[regionIndex] = new int[loc.size()];
      for (int i = 0; i < loc.size(); i++) {
        regionLocations[regionIndex][i] = loc.get(i) == null ? -1
            : (hostsToIndex.get(loc.get(i)) == null ? -1 : hostsToIndex.get(loc.get(i)));
      }
    }
    
    protected HTableDescriptor getTableDescriptor(byte[] tableName) throws IOException {
      if (this.services != null && this.services.getTableDescriptors() != null) {
        return this.services.getTableDescriptors().get(tableName);
      }
      return null;
    }
    
    protected List<String> getTopHosts(Configuration conf, byte[] tableName, String regionEncodeName)
        throws IOException {
      HTableDescriptor desc = getTableDescriptor(tableName);
      if (desc == null) {
        return new ArrayList<String>();
      }
      return HRegion.computeHDFSBlocksDistribution(conf, desc, regionEncodeName).getTopHosts();
    }

    /** An action to move or swap a region */
    public static class Action {
      public static enum Type {
        ASSIGN_REGION,
        MOVE_REGION,
        SWAP_REGIONS,
        NULL,
      }

      public Type type;
      public Action (Type type) {this.type = type;}
      /** Returns an Action which would undo this action */
      public Action undoAction() { return this; }
      public String toString(Cluster cluster) { return type + ":";}
    }

    public static class MoveRegionAction extends Action {
      public int region;
      public int fromServer;
      public int toServer;

      public MoveRegionAction(int region, int fromServer, int toServer) {
        super(Type.MOVE_REGION);
        this.fromServer = fromServer;
        this.region = region;
        this.toServer = toServer;
      }
      @Override
      public Action undoAction() {
        return new MoveRegionAction (region, toServer, fromServer);
      }
      public String toString(Cluster cluster) {
        return type + ": " + cluster.regions[region].getEncodedName() + ":"
            + cluster.servers[fromServer] + " -> " + cluster.servers[toServer];
      }
    }

    public static class SwapRegionsAction extends Action {
      public int fromServer;
      public int fromRegion;
      public int toServer;
      public int toRegion;
      public SwapRegionsAction(int fromServer, int fromRegion, int toServer, int toRegion) {
        super(Type.SWAP_REGIONS);
        this.fromServer = fromServer;
        this.fromRegion = fromRegion;
        this.toServer = toServer;
        this.toRegion = toRegion;
      }
      @Override
      public Action undoAction() {
        return new SwapRegionsAction (fromServer, toRegion, toServer, fromRegion);
      }
      @Override
      public String toString(Cluster cluster) {
        return type + ": " + cluster.regions[fromRegion].getEncodedName() + ":"
            + cluster.servers[fromServer] + " <-> " + cluster.regions[toRegion].getEncodedName()
            + ":" + cluster.servers[toServer];
      }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NM_FIELD_NAMING_CONVENTION",
        justification="Mistake. Too disruptive to change now")
    public static final Action NullAction = new Action(Type.NULL);

    public void doAction(Action action) {
      switch (action.type) {
      case NULL: break;
      case MOVE_REGION:
        MoveRegionAction mra = (MoveRegionAction) action;
        regionsPerServer[mra.fromServer] = removeRegion(regionsPerServer[mra.fromServer], mra.region);
        regionsPerServer[mra.toServer] = addRegion(regionsPerServer[mra.toServer], mra.region);
        regionMoved(mra.region, mra.fromServer, mra.toServer);
        break;
      case SWAP_REGIONS:
        SwapRegionsAction a = (SwapRegionsAction) action;
        regionsPerServer[a.fromServer] = replaceRegion(regionsPerServer[a.fromServer], a.fromRegion, a.toRegion);
        regionsPerServer[a.toServer] = replaceRegion(regionsPerServer[a.toServer], a.toRegion, a.fromRegion);
        regionMoved(a.fromRegion, a.fromServer, a.toServer);
        regionMoved(a.toRegion, a.toServer, a.fromServer);
        break;
      default:
        throw new RuntimeException("Uknown action:" + action.type);
      }
    }

    void regionMoved(int region, int oldServer, int newServer) {
      regionIndexToServerIndex[region] = newServer;
      if (initialRegionIndexToServerIndex[region] == newServer) {
        numMovedRegions--; //region moved back to original location
      } else if (oldServer >= 0 && initialRegionIndexToServerIndex[region] == oldServer) {
        numMovedRegions++; //region moved from original location
      }
      
      int tableIndex = regionIndexToTableIndex[region];
      if (oldServer >= 0) {
        numRegionsPerServerPerTable[oldServer][tableIndex]--;
      }
      numRegionsPerServerPerTable[newServer][tableIndex]++;

      //check whether this caused maxRegionsPerTable in the new Server to be updated
      if (numRegionsPerServerPerTable[newServer][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
        numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[newServer][tableIndex];
      } else if (oldServer >= 0 && (numRegionsPerServerPerTable[oldServer][tableIndex] + 1)
          == numMaxRegionsPerTable[tableIndex]) {
        numMaxRegionsPerTable[tableIndex] = 0;
        //recompute maxRegionsPerTable since the previous value was coming from the old server
        for (int serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
        }
      }
    }

    int[] removeRegion(int[] regions, int regionIndex) {
      //TODO: this maybe costly. Consider using linked lists
      int[] newRegions = new int[regions.length - 1];
      int i = 0;
      for (i = 0; i < regions.length; i++) {
        if (regions[i] == regionIndex) {
          break;
        }
        newRegions[i] = regions[i];
      }
      System.arraycopy(regions, i+1, newRegions, i, newRegions.length - i);
      return newRegions;
    }

    int[] addRegion(int[] regions, int regionIndex) {
      int[] newRegions = new int[regions.length + 1];
      System.arraycopy(regions, 0, newRegions, 0, regions.length);
      newRegions[newRegions.length - 1] = regionIndex;
      return newRegions;
    }

    int[] replaceRegion(int[] regions, int regionIndex, int newRegionIndex) {
      int i = 0;
      for (i = 0; i < regions.length; i++) {
        if (regions[i] == regionIndex) {
          regions[i] = newRegionIndex;
          break;
        }
      }
      return regions;
    }

    void sortServersByRegionCount() {
      Arrays.sort(serverIndicesSortedByRegionCount, numRegionsComparator);
    }

    int getNumRegions(int server) {
      return regionsPerServer[server].length;
    }

    private Comparator<Integer> numRegionsComparator = new Comparator<Integer>() {
      @Override
      public int compare(Integer integer, Integer integer2) {
        return Integer.valueOf(getNumRegions(integer)).compareTo(getNumRegions(integer2));
      }
    };

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="SBSC_USE_STRINGBUFFER_CONCATENATION",
        justification="Not important but should be fixed")
    @Override
    public String toString() {
      String desc = "Cluster{" +
          "servers=[";
          for(ServerName sn:servers) {
             desc += sn.getHostAndPort() + ", ";
          }
          desc +=
          ", serverIndicesSortedByRegionCount="+
          Arrays.toString(serverIndicesSortedByRegionCount) +
          ", regionsPerServer=[";

          for (int[]r:regionsPerServer) {
            desc += Arrays.toString(r);
          }
          desc += "]" +
          ", numRegions=" +
          numRegions +
          ", numServers=" +
          numServers +
          ", numMovedRegions=" +
          numMovedRegions +
          '}';
      return desc;
    }
  }
}
