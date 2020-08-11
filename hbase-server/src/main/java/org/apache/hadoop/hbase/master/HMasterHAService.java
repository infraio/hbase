package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;

import java.util.Optional;

public interface HMasterHAService {

  /**
   * Block until becoming the active master.
   *
   * Method blocks until there is not another active master and our attempt
   * to become the new active master is successful.
   *
   * This also makes sure that we are watching the master znode so will be
   * notified if another master dies.
   * @param checkInterval the interval to check if the master is stopped
   * @param startupStatus the monitor status to track the progress
   * @return True if no issue becoming active master else false if another
   *   master was running or if some other problem (zookeeper, stop flag has been
   *   set on this Master)
   */
  boolean blockUntilBecomingActiveMaster(
    int checkInterval, MonitoredTask startupStatus);


  Optional<ServerName> getActiveMasterServerName();

  /**
   * @return True if cluster has an active master.
   */
  boolean hasActiveMaster();

  void stop();
}
