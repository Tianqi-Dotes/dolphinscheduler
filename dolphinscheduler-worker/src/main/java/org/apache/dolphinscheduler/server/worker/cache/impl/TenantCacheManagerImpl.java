/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.worker.cache.impl;

import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.server.worker.cache.TenantCacheManager;

import java.util.Collections;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TenantCacheManagerImpl implements TenantCacheManager {

    private volatile Set<String> tenantCache = Collections.emptySet();

    @Override
    public boolean createTenantIfAbsent(String tenantName) {
        if(this.contains(tenantName)) {
            return true;
        }
        synchronized(TenantCacheManagerImpl.class) {
            boolean success = OSUtils.createTenant(tenantName);
            if (success) {
                this.refresh(OSUtils.getUserList());
                return this.contains(tenantName);
            }
        }
        return false;
    }

    @Override
    public boolean contains(String sysUserName) {
        return tenantCache.contains(sysUserName);
    }

    @Override
    public void refresh(Set<String> tenantSet) {
        if (tenantSet.equals(tenantCache)) {
            return;
        }
        log.info("Refresh tenant list into memory: {}", tenantSet);
        tenantCache = Sets.newConcurrentHashSet(tenantSet);
    }

    @Override
    public void clearCache() {
        tenantCache.clear();
    }

}
