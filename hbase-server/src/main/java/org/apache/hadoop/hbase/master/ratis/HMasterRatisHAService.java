package org.apache.hadoop.hbase.master.ratis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.ActiveMasterManager;
import org.apache.hadoop.hbase.master.HMasterHAService;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.ratis.proto.RaftProtos;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@InterfaceAudience.Private
public class HMasterRatisHAService implements HMasterHAService {
  private static final Logger LOG = LoggerFactory.getLogger(HMasterRatisHAService.class);

  private final ServerName serverName;
  private final Server master;
  private final HMasterRatisServer ratisServer;
  private final ZKWatcher watcher;
  private volatile ServerName activeMasterServerName;

  HMasterRatisHAService(Configuration conf, ZKWatcher watcher, ServerName serverName, Server master) throws IOException {
    ratisServer = new HMasterRatisServer(conf);
    this.watcher = watcher;
    this.serverName = serverName;
    this.master = master;
  }

  @Override
  public boolean blockUntilBecomingActiveMaster(int checkInterval, MonitoredTask startupStatus) {
    while (!(master.isAborted() || master.isStopped())) {
      startupStatus.setStatus("Checking if is active");
      Optional<RaftProtos.RaftPeerRole> role = ratisServer.getServerRole();
      if (role.isPresent() && role.get().equals(RaftProtos.RaftPeerRole.LEADER)) {
        // We are the master, return
        startupStatus.setStatus("Successfully registered as active master.");
        activeMasterServerName = serverName;
        LOG.info("Registered as active master=" + this.serverName);
        return true;
      }
      try {
        TimeUnit.SECONDS.sleep(checkInterval);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted when sleep", e);
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }

  @Override
  public Optional<ServerName> getActiveMasterServerName() {
    if (activeMasterServerName == null) {
      LOG.debug("Attempting to fetch active master sn from zk");
      try {
        activeMasterServerName = MasterAddressTracker.getMasterAddress(watcher);
      } catch (IOException | KeeperException e) {
        // Log and ignore for now and re-fetch later if needed.
        LOG.error("Error fetching active master information", e);
      }
    }
    // It could still be null, but return whatever we have.
    return Optional.ofNullable(activeMasterServerName);
  }

  @Override
  public boolean hasActiveMaster() {
    try {
      if (ZKUtil.checkExists(watcher, watcher.getZNodePaths().masterAddressZNode) >= 0) {
        return true;
      }
    }
    catch (KeeperException ke) {
      LOG.info("Received an unexpected KeeperException when checking " +
        "hasActiveMaster : "+ ke);
    }
    return false;
  }

  @Override
  public void stop() {
    try {
      // If our address is in ZK, delete it on our way out
      ServerName activeMaster = null;
      try {
        activeMaster = MasterAddressTracker.getMasterAddress(this.watcher);
      } catch (IOException e) {
        LOG.warn("Failed get of master address: " + e.toString());
      }
      if (activeMaster != null &&  activeMaster.equals(this.serverName)) {
        ZKUtil.deleteNode(watcher, watcher.getZNodePaths().masterAddressZNode);
        // We may have failed to delete the znode at the previous step, but
        //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
        ZNodeClearer.deleteMyEphemeralNodeOnDisk();
      }
    } catch (KeeperException e) {
      LOG.debug(this.watcher.prefix("Failed delete of our master address node; " +
        e.getMessage()));
    }
  }
}
