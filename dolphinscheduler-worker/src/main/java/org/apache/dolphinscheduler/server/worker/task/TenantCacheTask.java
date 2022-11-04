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

package org.apache.dolphinscheduler.server.worker.task;

import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.server.worker.cache.TenantCacheManager;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TenantCacheTask extends BaseDaemonThread {

    protected boolean runningFlag;

    protected long refreshInterval;

    @Autowired
    private TenantCacheManager tenantCacheManager;

    public TenantCacheTask(@NonNull WorkerConfig workerConfig) {
        super("SystemUserTask");
        this.refreshInterval = workerConfig.getRefreshSystemUserInterval().toMillis();
        this.runningFlag = true;
    }

    @Override
    public synchronized void start() {
        log.info("Starting {}, refreshInterval: {} ms", this.getName(), refreshInterval);
        super.start();
        log.info("Started {}, refreshInterval: {} ms", this.getName(), refreshInterval);
    }

    @Override
    public void run() {
        while (runningFlag) {
            try {
                tenantCacheManager.refresh(OSUtils.getUserList());
            } catch (Exception ex) {
                log.error("{} task execute failed", this.getName(), ex);
            } finally {
                try {
                    Thread.sleep(this.refreshInterval);
                } catch (InterruptedException ex) {
                    handleInterruptException(ex);
                }
            }
        }
    }

    public void shutdown() {
        log.warn("{} task finished", this.getName());
        runningFlag = false;
        tenantCacheManager.clearCache();
    }

    private void handleInterruptException(InterruptedException ex) {
        log.warn("{} has been interrupted, will stop this thread", this.getName(), ex);
        Thread.currentThread().interrupt();
    }
}
