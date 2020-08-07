package org.apache.hadoop.hbase.master.ratis;

import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.yetus.audience.InterfaceAudience;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@InterfaceAudience.Private
public class HMasterRatisServer {

  public static final String RATIS_RPC_PORT = "hbase.master.ratis.rpc.port";
  public static final int DEFAULT_RATIS_RPC_PORT = 22333;
  public static final String RATIS_STORAGE_DIR = "hbase.master.ratis.storage.dir";


  private final RaftServer raftServer;
  private final RaftGroupId raftGroupId;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;
  private final HMasterRatisStateMachine stateMachine;

  public HMasterRatisServer(String raftPeerIdStr, String raftGroupIdStr, List<RaftPeer> raftPeers)
    throws Exception {
    raftPeerId = RaftPeerId.valueOf(raftPeerIdStr);
    raftGroupId =
      RaftGroupId.valueOf(UUID.nameUUIDFromBytes(raftGroupIdStr.getBytes(StandardCharsets.UTF_8)));
    raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers);
    stateMachine = new HMasterRatisStateMachine();

    RaftProperties properties = new RaftProperties();
    raftServer = RaftServer.newBuilder().setServerId(this.raftPeerId).setGroup(this.raftGroup)
      .setProperties(properties).setStateMachine(stateMachine).build();
    raftServer.start();
  }

  private RaftProperties newRaftProperties(Configuration conf) {
    RaftProperties properties = new RaftProperties();
    // Use Netty Rpc as default
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
    NettyConfigKeys.Server.setPort(properties, conf.getInt(RATIS_RPC_PORT, DEFAULT_RATIS_RPC_PORT));

    return properties;
  }
}
