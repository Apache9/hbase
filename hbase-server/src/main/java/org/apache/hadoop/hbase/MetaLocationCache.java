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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.hash.Hasher;
import org.apache.hbase.thirdparty.com.google.common.hash.Hashing;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.MetaLocationUpdate;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.MetaLocationUpdateType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.SyncMetaLocationResponse;

/**
 * Cache the meta locations.
 * <p/>
 * The meta locations are now stored in the master region. So at active master side, when
 * restarting, we will load the locations into this cache, when reassigning meta regions, we will
 * also update this cache.
 * <p/>
 * At backup master and region server side, we will fetch the content of this cache periodically
 * from active master, and active master will also send us the newest update of this cache.
 */
@InterfaceAudience.Private
public final class MetaLocationCache {

  private static final Logger LOG = LoggerFactory.getLogger(MetaLocationCache.class);

  public static final String PERIODIC_SYNC_INTERVAL_SECS =
    "hbase.server.meta-location-cache.sync_interval_secs";

  private static final int PERIODIC_SYNC_INTERVAL_SECS_DEFAULT = 60;

  public static final String MIN_SECS_BETWEEN_SYNCS =
    "hbase.server.meta-location-cache.min_secs_between_syncs";

  private static final int MIN_SECS_BETWEEN_SYNCS_DEFAULT = 5;

  private final Server server;

  private final long periodicSyncMs;

  private final long minTimeBetweenSyncsMs;

  // indicate whether this cache is act as primary cache, i.e, need to publish updates to other
  // caches.
  private boolean primary;

  private boolean stopped;

  private boolean syncNow;

  private ArrayDeque<MetaLocationUpdate> updates;

  private Supplier<List<ServerName>> getServersToPublish;

  private Thread syncThread;

  private Thread publishThread;

  private static final class HashAndLocations {
    private final long hash;

    private final RegionLocations metaLocations;

    public HashAndLocations(RegionLocations metaLocations) {
      this.hash = hash(metaLocations);
      this.metaLocations = metaLocations;
    }
  }

  private volatile HashAndLocations hashAndLocations = new HashAndLocations(new RegionLocations());

  public MetaLocationCache(Server server) {
    this.server = server;
    Configuration conf = server.getConfiguration();
    periodicSyncMs = TimeUnit.SECONDS
      .toMillis(conf.getLong(PERIODIC_SYNC_INTERVAL_SECS, PERIODIC_SYNC_INTERVAL_SECS_DEFAULT));
    minTimeBetweenSyncsMs = TimeUnit.SECONDS
      .toMillis(conf.getLong(MIN_SECS_BETWEEN_SYNCS, MIN_SECS_BETWEEN_SYNCS_DEFAULT));
    Preconditions.checkArgument(periodicSyncMs > 0);
    Preconditions.checkArgument(minTimeBetweenSyncsMs < periodicSyncMs);
    syncThread = new Thread(this::sync, "MetaLocationCache-Sync");
    syncThread.start();
  }

  public List<HRegionLocation> getMetaLocations() {
    HashAndLocations hal = this.hashAndLocations;
    return hal != null ? Arrays.asList(hal.metaLocations.getRegionLocations()) :
      Collections.emptyList();
  }

  public SyncMetaLocationResponse syncMetaLocation() {
    HashAndLocations hal = this.hashAndLocations;
    SyncMetaLocationResponse.Builder builder =
      SyncMetaLocationResponse.newBuilder().setHash(hal.hash);
    hal.metaLocations.forEach(loc -> builder.addLocation(ProtobufUtil.toRegionLocation(loc)));
    return builder.build();
  }

  private HRegionLocation[] ensureSize(HRegionLocation[] locs, int replicaId) {
    if (locs.length <= replicaId) {
      return Arrays.copyOf(locs, replicaId + 1);
    } else {
      return locs;
    }
  }

  public void applyMetaLocationUpdate(long hash, List<MetaLocationUpdate> updates) {
    LOG.debug("apply meta location update: hash={}, updates={}", hash, updates);
    synchronized (this) {
      Preconditions.checkState(!primary);
      HashAndLocations hal = this.hashAndLocations;
      HRegionLocation[] locs = hal.metaLocations.getRegionLocations();
      // apply the updates
      for (MetaLocationUpdate update : updates) {
        switch (update.getType()) {
          case UPDATE:
            RegionInfo regionInfo = ProtobufUtil.toRegionInfo(update.getRegionInfo());
            locs = ensureSize(locs, regionInfo.getReplicaId());
            locs[regionInfo.getReplicaId()] = new HRegionLocation(regionInfo,
              ProtobufUtil.toServerName(update.getServerName()), update.getSeqNum());
            break;
          case REMOVE:
            if (locs.length > update.getReplicaCount()) {
              locs = Arrays.copyOf(locs, update.getReplicaCount());
            }
            break;
          default:
            throw new IllegalArgumentException("Unknown update type: " + update.getType());
        }
      }
      RegionLocations newLocs = new RegionLocations(locs);
      HashAndLocations newHal = new HashAndLocations(newLocs);
      this.hashAndLocations = newHal;
      if (newHal.hash != hash) {
        // after applying the updates, if the hash does not match, usually this means we missed some
        // previous updates, so let's trigger a full sync.
        syncNow = true;
        notifyAll();
      }
    }
  }

  private void sync() {
    long lastRefreshTime = 0;
    for (;;) {
      synchronized (this) {
        for (;;) {
          // we have been switched to active masters, skip syncing
          if (primary || stopped) {
            return;
          }
          // if syncNow is true, then we will wait until minTimeBetweenSyncsMs elapsed,
          // otherwise wait until periodicSyncMs elapsed
          long waitTime = (syncNow ? minTimeBetweenSyncsMs : periodicSyncMs)
            - (EnvironmentEdgeManager.currentTime() - lastRefreshTime);
          if (waitTime <= 0) {
            // we are going to refresh, reset this flag
            syncNow = false;
            break;
          }
          try {
            wait(waitTime);
          } catch (InterruptedException e) {
            LOG.warn("Interrupted during wait", e);
            Thread.currentThread().interrupt();
            continue;
          }
        }
      }
      AsyncClusterConnection conn = server.getAsyncClusterConnection();
      if (conn == null) {
        continue;
      }
      HashAndLocations hal = this.hashAndLocations;
      long hash = hal.hash;
      SyncMetaLocationResponse resp;
      try {
        resp = FutureUtils.get(conn.syncMetaLocation(hash));
      } catch (IOException e) {
        LOG.warn("Failed to sync meta location", e);
        continue;
      }
      LOG.debug("sync meta location: {}", resp);
      lastRefreshTime = EnvironmentEdgeManager.currentTime();
      if (resp.getHash() == hash) {
        continue;
      }
      RegionLocations locs = new RegionLocations(resp.getLocationList().stream()
        .map(ProtobufUtil::toRegionLocation).collect(Collectors.toList()));
      HashAndLocations newHal = new HashAndLocations(locs);
      synchronized (this) {
        if (primary || stopped) {
          return;
        }
        if (this.hashAndLocations.hash != hash) {
          // this hash has been changed, which means we have received an update request, skip this
          // update to avoid possible resetting the cached data to a stale one, and set syncNow to
          // true to start a new sync soon, to check whether we already have the newest data.
          syncNow = true;
          continue;
        }
        this.hashAndLocations = newHal;
      }
    }
  }

  private static final int PUBLISH_WAIT_MS = 30000;

  private void publish() {
    for (;;) {
      long hash = 0;
      List<MetaLocationUpdate> toPublish = new ArrayList<>();
      synchronized (this) {
        for (;;) {
          if (stopped) {
            return;
          }
          if (updates.isEmpty()) {
            try {
              wait(PUBLISH_WAIT_MS);
            } catch (InterruptedException e) {
              LOG.warn("Interrupted during wait", e);
              Thread.currentThread().interrupt();
            }
            continue;
          }
          while (!updates.isEmpty()) {
            toPublish.add(updates.pollFirst());
          }
          hash = hashAndLocations.hash;
          break;
        }
      }
      server.getAsyncClusterConnection().publishMetaLocationUpdate(getServersToPublish.get(), hash,
        toPublish);
    }
  }

  private static long hash(RegionLocations locs) {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    for (HRegionLocation loc : locs) {
      if (loc == null) {
        continue;
      }
      hasher.putObject(loc, (l, sink) -> {
        sink.putBytes(RegionInfo.toByteArray(l.getRegion()));
        ServerName sn = l.getServerName();
        sink.putString(sn.getHostname(), StandardCharsets.UTF_8);
        sink.putInt(sn.getPort());
        sink.putLong(sn.getStartcode());
        sink.putLong(l.getSeqNum());
      });
    }
    return hasher.hash().asLong();
  }

  /**
   * We will always create this cache for both masters and region servers, and it will be initialize
   * as secondary replica, to fetch the content from active master. And once we become the active
   * master, we will stop the background tasks, reset the meta locations, and then start to publish
   * updates to others.
   */
  public void upgrade(RegionLocations metaLocations,
    Supplier<List<ServerName>> getServersToPublish) {
    HashAndLocations hal = new HashAndLocations(metaLocations);
    synchronized (this) {
      Preconditions.checkState(!primary, "already upgraded");
      primary = true;
      hashAndLocations = hal;
      updates = new ArrayDeque<>();
      this.getServersToPublish = getServersToPublish;
      syncThread = null;
      publishThread = new Thread(this::publish, "MetaLocationCache-Publish");
      publishThread.start();
      notifyAll();
    }
  }

  private MetaLocationUpdate toMetaLocationUpdate(HRegionLocation loc) {
    return MetaLocationUpdate.newBuilder().setType(MetaLocationUpdateType.UPDATE)
      .setRegionInfo(ProtobufUtil.toRegionInfo(loc.getRegion()))
      .setServerName(ProtobufUtil.toServerName(loc.getServerName())).setSeqNum(loc.getSeqNum())
      .build();
  }

  private MetaLocationUpdate toMetaLocationUpdate(RegionInfo region, int newReplicaCount) {
    return MetaLocationUpdate.newBuilder().setType(MetaLocationUpdateType.UPDATE)
      .setRegionInfo(ProtobufUtil.toRegionInfo(region)).setReplicaCount(newReplicaCount).build();
  }

  private void updateAndPublish(RegionLocations newLocs, MetaLocationUpdate update) {
    this.hashAndLocations = new HashAndLocations(newLocs);
    updates.add(update);
    notifyAll();
  }

  public void updateRegionLocation(HRegionLocation loc) {
    synchronized (this) {
      Preconditions.checkState(primary);
      HashAndLocations hal = this.hashAndLocations;
      RegionLocations newLocs = hal.metaLocations.updateLocation(loc, false, true);
      updateAndPublish(newLocs, toMetaLocationUpdate(loc));
    }
  }

  public void removeRegionReplica(RegionInfo region, int newReplicaCount) {
    synchronized (this) {
      Preconditions.checkState(primary);
      HashAndLocations hal = this.hashAndLocations;
      RegionLocations newLocs =
        new RegionLocations(Stream.of(hal.metaLocations.getRegionLocations())
          .filter(l -> l != null && l.getRegion().getReplicaId() < newReplicaCount)
          .toArray(HRegionLocation[]::new));
      updateAndPublish(newLocs, toMetaLocationUpdate(region, newReplicaCount));
    }
  }
}
