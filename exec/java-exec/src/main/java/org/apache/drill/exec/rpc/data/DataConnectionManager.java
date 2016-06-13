/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.data;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData.BitClientHandshake;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.RpcChannel;
import org.apache.drill.exec.rpc.ReconnectingConnection;
import org.apache.drill.exec.server.BootStrapContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataConnectionManager extends ReconnectingConnection<DataClientConnection, BitClientHandshake>{

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataConnectionManager.class);

  private final DrillbitEndpoint endpoint;
  private final BootStrapContext context;
  private final Map<FragmentContext, ChannelWritabilityListener> cwls = new ConcurrentHashMap<>();

  private final static BitClientHandshake HANDSHAKE = BitClientHandshake //
      .newBuilder() //
      .setRpcVersion(DataRpcConfig.RPC_VERSION) //
      .setChannel(RpcChannel.BIT_DATA) //
      .build();

  private volatile boolean channelWritable = true; // this must be initially true

  public DataConnectionManager(DrillbitEndpoint endpoint, BootStrapContext context) {
    super(HANDSHAKE, endpoint.getAddress(), endpoint.getDataPort());
    this.endpoint = endpoint;
    this.context = context;
  }

  public void addChannelWritabilityListener(FragmentContext context, ChannelWritabilityListener cwl) {
    cwls.put(context, cwl);
  }

  public void removeChannelWritabilityListener(FragmentContext context) {
    cwls.remove(context);
  }

  @Override
  protected DataClient getNewClient() {
    return new DataClient(endpoint, context, new CloseHandlerCreator());
  }

  @Override
  protected void connectionUpdated(DataClientConnection connection) {
    connection.getChannel().pipeline().addLast(new ChannelInboundHandlerAdapter() {
      @Override
      public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        boolean isWritable = ctx.channel().isWritable();
        fireWritabilityListener(isWritable);

        ctx.fireChannelWritabilityChanged();
      }
    });

    fireWritabilityListener(true); // we have a new connection, make sure we wake up all "blocked" tasks
  }

  private void fireWritabilityListener(boolean isWritable) {
    logger.trace("channel writability changed: {}", isWritable);
    channelWritable = isWritable;
    for (ChannelWritabilityListener cwl : cwls.values()) {
      cwl.channelWritabilityChanged(isWritable);
    }
  }

  public boolean isChannelWritable() {
    return channelWritable;
  }

  public interface ChannelWritabilityListener {
    void channelWritabilityChanged(boolean writable);
  }
}