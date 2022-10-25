package org.apache.dolphinscheduler.server.master.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.dao.dto.IsolationTaskStatus;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.dolphinscheduler.dao.repository.IsolationTaskDao;
import org.apache.dolphinscheduler.server.master.cache.ProcessInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class IsolationTaskManager {

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    private volatile Map<Long, IsolationTask> onlineIsolationTasksInMemory = new HashMap<>();

    private volatile Set<Long> currentIsolationTaskIdsInMemory = new HashSet<>();

    @Autowired
    private IsolationTaskDao isolationTaskDao;

    @PostConstruct
    public void init() {
        refreshIsolationTaskMapFromDB();
    }

    public void refreshIsolationTaskMapFromDB() {
        Map<Long, IsolationTask> totalOnlineIsolationTasksInDB =
                isolationTaskDao.queryByStatus(IsolationTaskStatus.ONLINE)
                        .stream()
                        .collect(Collectors.toMap(IsolationTask::getId, Function.identity()));
        Set<Long> totalOnlineIsolationTaskCodesInDB = totalOnlineIsolationTasksInDB.keySet();

        Collection<IsolationTask> needToOfflineIsolationTasks =
                CollectionUtils.subtract(currentIsolationTaskIdsInMemory, totalOnlineIsolationTaskCodesInDB)
                        .stream()
                        .map(onlineIsolationTasksInMemory::get)
                        .collect(Collectors.toList());

        Collection<IsolationTask> needToOnlineIsolationTasks =
                CollectionUtils.subtract(totalOnlineIsolationTaskCodesInDB, currentIsolationTaskIdsInMemory)
                        .stream()
                        .map(totalOnlineIsolationTasksInDB::get)
                        .collect(Collectors.toList());

        currentIsolationTaskIdsInMemory = totalOnlineIsolationTaskCodesInDB;
        onlineIsolationTasksInMemory = totalOnlineIsolationTasksInDB;

        offlineIsolationTask(needToOfflineIsolationTasks);
        onlineIsolationTask(needToOnlineIsolationTasks);
    }

    private void offlineIsolationTask(Collection<IsolationTask> needOfflineIsolationTasks) {
        if (CollectionUtils.isEmpty(needOfflineIsolationTasks)) {
            return;
        }
        for (IsolationTask needOfflineIsolation : needOfflineIsolationTasks) {
            WorkflowExecuteRunnable workflowExecuteRunnable = processInstanceExecCacheManager
                    .getByProcessInstanceId(needOfflineIsolation.getWorkflowInstanceId());
            if (workflowExecuteRunnable == null) {
                continue;
            }
            workflowExecuteRunnable.cancelTaskIsolation(needOfflineIsolation.getTaskCode());
            log.info("Backend offline isolation task, isolationTaskId: {}", needOfflineIsolation.getId());
        }
    }

    private void onlineIsolationTask(Collection<IsolationTask> needOnlineIsolationTasks) {
        if (CollectionUtils.isEmpty(needOnlineIsolationTasks)) {
            return;
        }
        for (IsolationTask needOnlineIsolationTask : needOnlineIsolationTasks) {
            WorkflowExecuteRunnable workflowExecuteRunnable = processInstanceExecCacheManager
                    .getByProcessInstanceId(needOnlineIsolationTask.getWorkflowInstanceId());
            if (workflowExecuteRunnable == null) {
                continue;
            }
            workflowExecuteRunnable.onlineTaskIsolation(needOnlineIsolationTask.getTaskCode());
            log.info("Backend online isolation task, isolationTaskId: {}", needOnlineIsolationTask.getId());
        }
    }

}
