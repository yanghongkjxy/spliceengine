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

package com.splicemachine.si.testsetup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * @author Scott Fines
 *         Date: 8/10/16
 */
public class HBase112SITestEnv extends HBaseSITestEnv{
    @Override
    protected Configuration transformConfiguration(Configuration conf,int basePort){
        return new Configuration(super.transformConfiguration(conf,basePort)){
            @Override
            public void set(String name,String value){
                if(name.equals(FileSystem.FS_DEFAULT_NAME_KEY)) return; //don't overload my setting, damnit!
                super.set(name,value);
            }
        };
    }

    @Override
    protected void bootMiniCluster() throws Exception{
        testUtility.startMiniCluster(1,1,0,null,null,null,false);
    }
}
