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
package org.apache.hadoop.hbase.master.ratis;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.NetUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class HMasterRatisServer {
  private static final Logger LOG = LoggerFactory.getLogger(HMasterRatisHAService.class);

  public static final String RATIS_RAFT_GROUP_ID = "master";
  public static final String RATIS_RAFT_PEERS = "hbase.master.ratis.raft.peers";
  public static final String RATIS_RAFT_PEER_ID = "hbase.master.ratis.raft.peer.id";
  public static final String RATIS_STORAGE_DIR = "hbase.master.ratis.storage.dir";

  private final RaftGroupId raftGroupId;
  private final Map<RaftPeerId, RaftPeer> raftPeers;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;
  private final RaftPeer raftPeer;
  private final HMasterRatisStateMachine stateMachine;
  private final RaftServer raftServer;

  private final ClientId clientId = ClientId.randomId();
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  public HMasterRatisServer(Configuration conf) throws IOException {
    raftGroupId = RaftGroupId
      .valueOf(UUID.nameUUIDFromBytes(RATIS_RAFT_GROUP_ID.getBytes(StandardCharsets.UTF_8)));
    raftPeers = getRaftPeers(conf);
    raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers.values());
    raftPeerId = getRatisRaftPeerId(conf);
    raftPeer = raftPeers.get(raftPeerId);
    stateMachine = new HMasterRatisStateMachine();

    RaftProperties properties = newRaftProperties(conf);
    raftServer = RaftServer.newBuilder().setServerId(this.raftPeerId).setGroup(this.raftGroup)
      .setProperties(properties).setStateMachine(stateMachine).build();
    raftServer.start();
  }

  public Optional<RaftProtos.RaftPeerRole> getServerRole() {
    try {
      GroupInfoReply groupInfo = getGroupInfo();
      RaftProtos.RoleInfoProto roleInfoProto = groupInfo.getRoleInfoProto();
      return Optional.of(roleInfoProto.getRole());
    } catch (IOException e) {
      LOG.error("Failed to retrieve RaftPeerRole", e);
      return Optional.empty();
    }
  }

  private GroupInfoReply getGroupInfo() throws IOException {
    GroupInfoRequest groupInfoRequest = new GroupInfoRequest(clientId,
      raftPeerId, raftGroupId, nextCallId());
    GroupInfoReply groupInfo = raftServer.getGroupInfo(groupInfoRequest);
    return groupInfo;
  }

  private RaftProperties newRaftProperties(Configuration conf) {
    RaftProperties properties = new RaftProperties();
    // Use Netty Rpc as default
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
    NettyConfigKeys.Server
      .setPort(properties, NetUtils.createSocketAddr(raftPeer.getAddress()).getPort());

    RaftServerConfigKeys
      .setStorageDir(properties, Collections.singletonList(new File(getRatisStorageDir(conf))));

    return properties;
  }

  private Map<RaftPeerId, RaftPeer> getRaftPeers(Configuration conf) {
    String peers = conf.get(RATIS_RAFT_PEERS);
    if (Strings.isNullOrEmpty(peers)) {
      throw new IllegalArgumentException(RATIS_RAFT_PEERS + " must be defined");
    }
    return Stream.of(peers.split(",")).map(address -> {
      String[] addressParts = address.split(":");
      return new RaftPeer(RaftPeerId.valueOf(addressParts[0]),
        addressParts[1] + ":" + addressParts[2]);
    }).collect(Collectors.toMap(RaftPeer::getId, Function.identity()));
  }

  private RaftPeerId getRatisRaftPeerId(Configuration conf) {
    String peerIdStr = conf.get(RATIS_RAFT_PEER_ID);
    if (Strings.isNullOrEmpty(peerIdStr)) {
      throw new IllegalArgumentException(RATIS_RAFT_PEER_ID + " must be defined");
    }
    RaftPeerId peerId = RaftPeerId.valueOf(peerIdStr);
    // raft peer id must match with raft peers config
    if (!raftPeers.containsKey(peerId)) {
      throw new IllegalArgumentException(RATIS_RAFT_PEER_ID + " must match with " + RATIS_RAFT_PEERS);
    }
    return peerId;
  }

  private String getRatisStorageDir(Configuration conf) {
    String storageDir = conf.get(RATIS_STORAGE_DIR);
    if (Strings.isNullOrEmpty(storageDir)) {
      throw new IllegalArgumentException(RATIS_STORAGE_DIR + " must be defined");
    }
    return storageDir;
  }

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }
}
