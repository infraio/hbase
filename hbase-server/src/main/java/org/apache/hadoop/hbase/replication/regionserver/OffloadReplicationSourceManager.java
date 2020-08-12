package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationListener;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Used when offload replication enabled.
 *
 * Notice: this is not compatible with bulkload replication and synchronous replication.
 */
@InterfaceAudience.Private
public class OffloadReplicationSourceManager implements ReplicationListener {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadReplicationSourceManager.class);
  private final ConcurrentMap<String, String> replicationQueues;
  // List of all the sources we got from died RSs
  private final List<ReplicationSourceInterface> oldsources;
  private final ReplicationQueueStorage queueStorage;
  private final ReplicationTracker replicationTracker;
  private final ReplicationPeers replicationPeers;
  // UUID for this cluster
  private final UUID clusterId;
  // All about stopping
  private final Server server;

  private final SyncReplicationPeerMappingManager syncReplicationPeerMappingManager;

  private final Configuration conf;
  private final FileSystem fs;
  // The paths to the latest log of each wal group, for new coming peers
  private final Map<String, Path> latestPaths;
  // Path to the wals directories
  private final Path logDir;
  // Path to the wal archive
  private final Path oldLogDir;
  private final WALFileLengthProvider walFileLengthProvider;

  private final boolean replicationForBulkLoadDataEnabled;

  private AtomicLong totalBufferUsed = new AtomicLong();

  // How long should we sleep for each retry when deleting remote wal files for sync replication
  // peer.
  private final long sleepForRetries;
  // Maximum number of retries before taking bold actions when deleting remote wal files for sync
  // replication peer.
  private final int maxRetriesMultiplier;
  // Total buffer size on this RegionServer for holding batched edits to be shipped.
  private final long totalBufferLimit;
  private final MetricsReplicationGlobalSourceSource globalMetrics;

  private final Map<String, MetricsSource> sourceMetrics = new HashMap<>();

  /**
   * Creates a replication manager and sets the watch on all the other registered region servers
   * @param queueStorage the interface for manipulating replication queues
   * @param replicationPeers
   * @param replicationTracker
   * @param conf the configuration to use
   * @param server the server for this region server
   * @param fs the file system to use
   * @param logDir the directory that contains all wal directories of live RSs
   * @param oldLogDir the directory where old logs are archived
   * @param clusterId
   */
  public OffloadReplicationSourceManager(ReplicationQueueStorage queueStorage,
    ReplicationPeers replicationPeers, ReplicationTracker replicationTracker, Configuration conf,
    Server server, FileSystem fs, Path logDir, Path oldLogDir, UUID clusterId,
    WALFileLengthProvider walFileLengthProvider,
    SyncReplicationPeerMappingManager syncReplicationPeerMappingManager,
    MetricsReplicationGlobalSourceSource globalMetrics) throws IOException {
    this.replicationQueues = new ConcurrentHashMap<>();
    this.queueStorage = queueStorage;
    this.replicationPeers = replicationPeers;
    this.replicationTracker = replicationTracker;
    this.server = server;
    this.oldsources = new ArrayList<>();
    this.conf = conf;
    this.fs = fs;
    this.logDir = logDir;
    this.oldLogDir = oldLogDir;
    this.clusterId = clusterId;
    this.walFileLengthProvider = walFileLengthProvider;
    this.syncReplicationPeerMappingManager = syncReplicationPeerMappingManager;
    this.replicationTracker.registerListener(this);
    this.latestPaths = new HashMap<>();
    this.replicationForBulkLoadDataEnabled = conf.getBoolean(
      HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    this.sleepForRetries = this.conf.getLong("replication.source.sync.sleepforretries", 1000);
    this.maxRetriesMultiplier =
      this.conf.getInt("replication.source.sync.maxretriesmultiplier", 60);
    this.totalBufferLimit = conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
      HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    this.globalMetrics = globalMetrics;
  }

  /**
   * Adds a normal source per registered peer cluster and tries to process all old region server wal
   * queues
   * <p>
   * The returned future is for adoptAbandonedQueues task.
   */
  Future<?> init() throws IOException {
    for (String id : this.replicationPeers.getAllPeerIds()) {
      addSource(id);
    }
  }

  /**
   * <ol>
   * <li>Add peer to replicationPeers</li>
   * <li>Add the normal source and related replication queue</li>
   * <li>Add HFile Refs</li>
   * </ol>
   * @param peerId the id of replication peer
   */
  public void addPeer(String peerId) throws IOException {
    boolean added = false;
    try {
      added = this.replicationPeers.addPeer(peerId);
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
    if (added) {
      addSource(peerId);
    }
  }

  /**
   * <ol>
   * <li>Remove peer for replicationPeers</li>
   * <li>Remove all the recovered sources for the specified id and related replication queues</li>
   * <li>Remove the normal source and related replication queue</li>
   * <li>Remove HFile Refs</li>
   * </ol>
   * @param peerId the id of the replication peer
   */
  public void removePeer(String peerId) {
    deleteQueue(peerId);
  }

  /**
   * Add a normal source for the given peer on this region server. Meanwhile, add new replication
   * queue to storage. For the newly added peer, we only need to enqueue the latest log of each wal
   * group and do replication
   * @param peerId the id of the replication peer
   * @return the source that was created
   */
  private void addSource(String peerId) throws IOException {
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      // For normal replication source, the replication queue id is same with peer id.
      this.replicationQueues.put(peerId, peerId);
      // Add the latest wal to that source's queue
      if (!latestPaths.isEmpty()) {
        for (Map.Entry<String, Path> walPrefixAndPath : latestPaths.entrySet()) {
          Path walPath = walPrefixAndPath.getValue();
          // Abort RS and throw exception to make add peer failed
          abortAndThrowIOExceptionWhenFail(
            () -> this.queueStorage.addWAL(server.getServerName(), peerId, walPath.getName()));
        }
      }
    }
  }

  public void drainSources(String peerId) throws IOException, ReplicationException {

  }

  public void refreshSources(String peerId) throws ReplicationException, IOException {

  }

  /**
   * Delete a complete queue of wals associated with a replication source
   * @param queueId the id of replication queue to delete
   */
  private void deleteQueue(String queueId) {
    abortWhenFail(() -> this.queueStorage.removeQueue(server.getServerName(), queueId));
  }

  @FunctionalInterface
  private interface ReplicationQueueOperation {
    void exec() throws ReplicationException;
  }

  private void abortWhenFail(ReplicationQueueOperation op) {
    try {
      op.exec();
    } catch (ReplicationException e) {
      server.abort("Failed to operate on replication queue", e);
    }
  }

  private void throwIOExceptionWhenFail(ReplicationQueueOperation op) throws IOException {
    try {
      op.exec();
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
  }

  private void abortAndThrowIOExceptionWhenFail(ReplicationQueueOperation op) throws IOException {
    try {
      op.exec();
    } catch (ReplicationException e) {
      server.abort("Failed to operate on replication queue", e);
      throw new IOException(e);
    }
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
  @VisibleForTesting
  public void preLogRoll(Path newLog) throws IOException {
    String logName = newLog.getName();
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(logName);
    // synchronized on latestPaths to avoid the new replication peer miss the new log
    synchronized (this.latestPaths) {
      // Add log to queue storage
      for (String queueId : this.replicationQueues.values()) {
        // If record log to queue storage failed, abort RS and throw exception to make log roll
        // failed
        abortAndThrowIOExceptionWhenFail(
          () -> this.queueStorage.addWAL(server.getServerName(), queueId, logName));
      }
      // Add to latestPaths
      latestPaths.put(logPrefix, newLog);
    }
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
  @VisibleForTesting
  public void postLogRoll(Path newLog) throws IOException {

  }

  @Override
  public void regionServerRemoved(String regionserver) {

  }

  /**
   * Terminate the replication on this region server
   */
  public void join() {
    this.executor.shutdown();
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.terminate("Region server is closing");
    }
  }

  /**
   * Get a copy of the wals of the normal sources on this rs
   * @return a sorted set of wal names
   */
  @VisibleForTesting
  public Map<String, Map<String, NavigableSet<String>>> getWALs()
    throws ReplicationException {
    Map<String, Map<String, NavigableSet<String>>> walsById = new HashMap<>();
    for (ReplicationSourceInterface source : sources.values()) {
      String queueId = source.getQueueId();
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      walsById.put(queueId, walsByGroup);
      for (String wal : this.queueStorage.getWALsInQueue(this.server.getServerName(), queueId)) {
        String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
        walsByGroup.computeIfAbsent(walPrefix, p -> new TreeSet<>()).add(wal);
      }
    }
    return Collections.unmodifiableMap(walsById);
  }

  /**
   * Get a copy of the wals of the recovered sources on this rs
   * @return a sorted set of wal names
   */
  @VisibleForTesting
  Map<String, Map<String, NavigableSet<String>>> getWalsByIdRecoveredQueues()
    throws ReplicationException {
    Map<String, Map<String, NavigableSet<String>>> walsByIdRecoveredQueues = new HashMap<>();
    for (ReplicationSourceInterface source : oldsources) {
      String queueId = source.getQueueId();
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      walsByIdRecoveredQueues.put(queueId, walsByGroup);
      for (String wal : this.queueStorage.getWALsInQueue(this.server.getServerName(), queueId)) {
        String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
        walsByGroup.computeIfAbsent(walPrefix, p -> new TreeSet<>()).add(wal);
      }
    }
    return Collections.unmodifiableMap(walsByIdRecoveredQueues);
  }

  /**
   * Get a list of all the normal sources of this rs
   * @return list of all normal sources
   */
  public List<ReplicationSourceInterface> getSources() {
    return new ArrayList<>(this.sources.values());
  }

  /**
   * Get a list of all the recovered sources of this rs
   * @return list of all recovered sources
   */
  public List<ReplicationSourceInterface> getOldSources() {
    return this.oldsources;
  }

  /**
   * Get the normal source for a given peer
   * @return the normal source for the give peer if it exists, otherwise null.
   */
  @VisibleForTesting
  public ReplicationSourceInterface getSource(String peerId) {
    return this.sources.get(peerId);
  }

  @VisibleForTesting
  List<String> getAllQueues() throws IOException {
    List<String> allQueues = Collections.emptyList();
    try {
      allQueues = queueStorage.getAllQueues(server.getServerName());
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
    return allQueues;
  }

  @VisibleForTesting
  int getSizeOfLatestPath() {
    synchronized (latestPaths) {
      return latestPaths.size();
    }
  }

  @VisibleForTesting
  Set<Path> getLastestPath() {
    synchronized (latestPaths) {
      return Sets.newHashSet(latestPaths.values());
    }
  }

  @VisibleForTesting
  public AtomicLong getTotalBufferUsed() {
    return totalBufferUsed;
  }

  /**
   * Returns the maximum size in bytes of edits held in memory which are pending replication
   * across all sources inside this RegionServer.
   */
  public long getTotalBufferLimit() {
    return totalBufferLimit;
  }

  /**
   * Get the directory where wals are archived
   * @return the directory where wals are archived
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /**
   * Get the directory where wals are stored by their RSs
   * @return the directory where wals are stored by their RSs
   */
  public Path getLogDir() {
    return this.logDir;
  }

  /**
   * Get the handle on the local file system
   * @return Handle on the local file system
   */
  public FileSystem getFs() {
    return this.fs;
  }

  /**
   * Get the ReplicationPeers used by this ReplicationSourceManager
   * @return the ReplicationPeers used by this ReplicationSourceManager
   */
  public ReplicationPeers getReplicationPeers() {
    return this.replicationPeers;
  }

  /**
   * Get a string representation of all the sources' metrics
   */
  public String getStats() {
    StringBuilder stats = new StringBuilder();
    // Print stats that apply across all Replication Sources
    stats.append("Global stats: ");
    stats.append("WAL Edits Buffer Used=").append(getTotalBufferUsed().get()).append("B, Limit=")
      .append(getTotalBufferLimit()).append("B\n");
    for (ReplicationSourceInterface source : this.sources.values()) {
      stats.append("Normal source for cluster " + source.getPeerId() + ": ");
      stats.append(source.getStats() + "\n");
    }
    for (ReplicationSourceInterface oldSource : oldsources) {
      stats.append("Recovered source for cluster/machine(s) " + oldSource.getPeerId() + ": ");
      stats.append(oldSource.getStats() + "\n");
    }
    return stats.toString();
  }

  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
    throws IOException {
    for (ReplicationSourceInterface source : this.sources.values()) {
      throwIOExceptionWhenFail(() -> addHFileRefs(source.getPeerId(), tableName, family, pairs));
    }
  }

  /**
   * Add hfile names to the queue to be replicated.
   * @param peerId the replication peer id
   * @param tableName Name of the table these files belongs to
   * @param family Name of the family these files belong to
   * @param pairs list of pairs of { HFile location in staging dir, HFile path in region dir which
   *          will be added in the queue for replication}
   * @throws ReplicationException If failed to add hfile references
   */
  private void addHFileRefs(String peerId, TableName tableName, byte[] family,
    List<Pair<Path, Path>> pairs) throws ReplicationException {
    // Only the normal replication source update here, its peerId is equals to queueId.
    MetricsSource metrics = sourceMetrics.get(peerId);
    ReplicationPeer replicationPeer = replicationPeers.getPeer(peerId);
    Set<String> namespaces = replicationPeer.getNamespaces();
    Map<TableName, List<String>> tableCFMap = replicationPeer.getTableCFs();
    if (tableCFMap != null) { // All peers with TableCFs
      List<String> tableCfs = tableCFMap.get(tableName);
      if (tableCFMap.containsKey(tableName)
        && (tableCfs == null || tableCfs.contains(Bytes.toString(family)))) {
        this.queueStorage.addHFileRefs(peerId, pairs);
        metrics.incrSizeOfHFileRefsQueue(pairs.size());
      } else {
        LOG.debug("HFiles will not be replicated belonging to the table {} family {} to peer id {}",
          tableName, Bytes.toString(family), peerId);
      }
    } else if (namespaces != null) { // Only for set NAMESPACES peers
      if (namespaces.contains(tableName.getNamespaceAsString())) {
        this.queueStorage.addHFileRefs(peerId, pairs);
        metrics.incrSizeOfHFileRefsQueue(pairs.size());
      } else {
        LOG.debug("HFiles will not be replicated belonging to the table {} family {} to peer id {}",
          tableName, Bytes.toString(family), peerId);
      }
    } else {
      // user has explicitly not defined any table cfs for replication, means replicate all the
      // data
      this.queueStorage.addHFileRefs(peerId, pairs);
      metrics.incrSizeOfHFileRefsQueue(pairs.size());
    }
  }

  int activeFailoverTaskCount() {
    return executor.getActiveCount();
  }

  MetricsReplicationGlobalSourceSource getGlobalMetrics() {
    return this.globalMetrics;
  }

  @InterfaceAudience.Private
  Server getServer() {
    return this.server;
  }

  @InterfaceAudience.Private
  ReplicationQueueStorage getQueueStorage() {
    return this.queueStorage;
  }
}
