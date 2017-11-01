/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.si.impl.store;

import org.apache.hadoop.hbase.util.Pair;

import java.util.*;

/**
 * Created by jyuan on 10/30/17.
 */
public class ActiveIgnoreTxnCacheSupplier {
    private final Set<Pair<Long,Long>> cache;

    IgnoreTxnSupplier delegate;

    public ActiveIgnoreTxnCacheSupplier (IgnoreTxnSupplier delegate) {
        cache = new HashSet<>();
        this.delegate = delegate;
    }

    public boolean shouldIgnore(Long txnId) {

        boolean ignore = false;
        try {

            if (cache.isEmpty()) {
                Set<Pair<Long, Long>> ignoreTxns = delegate.getIgnoreTxnList();
                for (Pair<Long, Long> ignoreTxn : ignoreTxns) {
                    cache.add(ignoreTxn);
                }
            }
            for (Pair<Long, Long> range : cache) {
                if (txnId > range.getFirst() && txnId < range.getSecond()) {
                    ignore = true;
                    break;
                }
            }

        } catch (Exception e) {
            return false;
        }
        return ignore;
    }
}
