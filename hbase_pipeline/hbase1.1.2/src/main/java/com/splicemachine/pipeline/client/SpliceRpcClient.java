/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.pipeline.client;

import com.google.protobuf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Hacky way of getting around HBase 1.1.2's weird package protection and assumed type semantics.
 *
 * The base AbstractRpcClient and RpcClientImpl assume that any RpcController which is passed in is an instance
 * of the PayloadCarryingRpcController (in some circumstances). However, this is not true in all cases: in SpliceMachine,
 * we pass a SpliceRpcController directly. Since that code is handled above the distribution level (i.e. it's in the
 * core pipeline code) but the problem only occurs on specific distributions (specifically, HDP 2.4.2 versus any other
 * version), we have to use this hacky solution. In order to use it, we have to set the configuration
 * property "hbase.rpc.client.impl", but ONLY on distributions which encounter this problem (to date, only HDP 2.4.2),
 * otherwise the existing logic works just fine.
 *
 * @author Scott Fines
 *         Date: 8/9/16
 */
public class SpliceRpcClient extends RpcClientImpl{
    public SpliceRpcClient(Configuration conf,String clusterId){
        super(conf,clusterId);
    }

    public SpliceRpcClient(Configuration conf,String clusterId,SocketAddress localAddr){
        super(conf,clusterId,localAddr);
    }

    @Override
    public BlockingRpcChannel createBlockingRpcChannel(ServerName sn,User ticket,int defaultOperationTimeout){
        return new SafeBlockingRpcChannelImplementation(this,sn,ticket,defaultOperationTimeout);
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private Message callBlockingMethod(Descriptors.MethodDescriptor md,
                                       PayloadCarryingRpcController pcrc,
                                       Message param,
                                       Message returnType,
                                       final User ticket,
                                       final InetSocketAddress isa) throws ServiceException {
    /*
     * Make a blocking call. Throws exceptions if there are network problems or if the remote code
     * threw an exception.
     *
     * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
     *               {@link UserProvider#getCurrent()} makes a new instance of User each time so
     *               will be a
     *               new Connection each time.
     * @return A pair with the Message response and the Cell data (if any).
     */
        if (pcrc == null) {
            pcrc = new PayloadCarryingRpcController();
        }

        long startTime = 0;
        if (LOG.isTraceEnabled()) {
            startTime = EnvironmentEdgeManager.currentTime();
        }
        Pair<Message, CellScanner> val;
        try {
            val = call(pcrc, md, param, returnType, ticket, isa);
            // Shove the results into controller so can be carried across the proxy/pb service void.
            pcrc.setCellScanner(val.getSecond());

            if (LOG.isTraceEnabled()) {
                long callTime = EnvironmentEdgeManager.currentTime() - startTime;
                LOG.trace("Call: " + md.getName() + ", callTime: " + callTime + "ms");
            }
            return val.getFirst();
        } catch (Throwable e) {
            throw new ServiceException(e);
        }
    }

    private static class SafeBlockingRpcChannelImplementation implements BlockingRpcChannel{
        private final InetSocketAddress isa;
        private final SpliceRpcClient rpcClient;
        private final User ticket;
        private final int channelOperationTimeout;

        /**
         * @param channelOperationTimeout - the default timeout when no timeout is given
         */
        SafeBlockingRpcChannelImplementation(final SpliceRpcClient rpcClient,
                                             final ServerName sn,final User ticket,int channelOperationTimeout) {
            this.isa = new InetSocketAddress(sn.getHostname(), sn.getPort());
            this.rpcClient = rpcClient;
            this.ticket = ticket;
            this.channelOperationTimeout = channelOperationTimeout;
        }

        @Override
        public Message callBlockingMethod(Descriptors.MethodDescriptor md,RpcController controller,
                                          Message param,Message returnType) throws ServiceException{
            PayloadCarryingRpcController pcrc = configurePayload(controller);

            return this.rpcClient.callBlockingMethod(md, pcrc, param, returnType, this.ticket, this.isa);
        }

        private PayloadCarryingRpcController configurePayload(RpcController controller){
            PayloadCarryingRpcController pcrc;
            if (controller instanceof PayloadCarryingRpcController) {
                pcrc = (PayloadCarryingRpcController) controller;
            } else if(controller==null){
                pcrc = new PayloadCarryingRpcController();
            }else{
                pcrc = new DelegatingPayloadRpcController(controller);
            }

            if (!pcrc.hasCallTimeout()) {
                pcrc.setCallTimeout(channelOperationTimeout);
            }
            return pcrc;
        }
    }
}
