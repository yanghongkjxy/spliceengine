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

package com.splicemachine.hbase;

import com.splicemachine.pipeline.client.SpliceRpcClient;
import com.splicemachine.test.PlatformConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.log4j.Logger;

/**
 * @author Scott Fines
 *         Date: 8/9/16
 */
public class Hbase112PlatformConfigurator implements PlatformConfigurator{
    private static final Logger logger = Logger.getLogger(Hbase112PlatformConfigurator.class);
    @Override
    public void platformSpecificConfigure(Configuration baseConfig){
        logger.info("Performing platform specific configuration for HBase 1.1.2 versions");
        if(System.getProperty("platform").equals("hdp2.4.2")){
            logger.info("Configuring for HDP2.4.2");
            baseConfig.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,SpliceRpcClient.class.getCanonicalName());
        }
    }
}
