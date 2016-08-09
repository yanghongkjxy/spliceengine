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

import com.google.protobuf.RpcController;
import com.splicemachine.hbase.SpliceRpcController;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.TimeLimitedRpcController;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 8/9/16
 */
public class DelegatingPayloadRpcController extends PayloadCarryingRpcController{
    private final RpcController delegate;

    public DelegatingPayloadRpcController(RpcController delegate){
        super();
        this.delegate=delegate;
    }

    public DelegatingPayloadRpcController(CellScanner cellScanner,RpcController delegate){
        super(cellScanner);
        this.delegate=delegate;
    }

    public DelegatingPayloadRpcController(List<CellScannable> cellIterables,RpcController delegate){
        super(cellIterables);
        this.delegate=delegate;
    }

    @Override
    public void setFailed(IOException e){
        super.setFailed(e);
        if(delegate instanceof SpliceRpcController)
            ((SpliceRpcController)delegate).setFailed(e);
        else if(delegate instanceof TimeLimitedRpcController)
            ((TimeLimitedRpcController)delegate).setFailed(e);
    }

    @Override
    public void setFailed(String reason){
        super.setFailed(reason);
        delegate.setFailed(reason);
    }
}
