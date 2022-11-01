package org.apache.dolphinscheduler.server.master.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.dolphinscheduler.common.utils.LoggerUtils;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.dolphinscheduler.dao.repository.IsolationTaskDao;
import org.apache.dolphinscheduler.server.master.cache.ProcessInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class IsolationMetadataManager {

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    private volatile Map<Long, IsolationTask> isolationTasksInMemory = new HashMap<>();
    private final Map<Integer, Set<Long>> workflowIsolatedTaskCodeMap = new HashMap<>();
    private final Map<Integer, Set<IsolationTask>> workflowIsolatedTaskMap = new HashMap<>();
    @Autowired
    private IsolationTaskDao isolationTaskDao;

    @PostConstruct
    public void init() {
        refreshIsolationTaskMetadata();
    }

    public synchronized void refreshIsolationTaskMetadata() {
        StopWatch stopWatch = StopWatch.createStarted();

        Map<Long, IsolationTask> isolationTasksInDB =
                isolationTaskDao.queryAllIsolationTask()
                        .stream()
                        .collect(Collectors.toMap(IsolationTask::getId, Function.identity()));

        List<IsolationTask> needToCancelIsolationTasks = isolationTasksInMemory.values()
                .stream()
                .filter(isolationTask -> !isolationTasksInDB.containsKey(isolationTask.getId()))
                .collect(Collectors.toList());

        List<IsolationTask> needToAddIsolationTasks = isolationTasksInDB.values()
                .stream()
                .filter(isolationTask -> !isolationTasksInMemory.containsKey(isolationTask.getId()))
                .collect(Collectors.toList());

        isolationTasksInMemory = isolationTasksInDB;

        cancelIsolationTask(needToCancelIsolationTasks);
        addIsolationTask(needToAddIsolationTasks);

        stopWatch.stop();
        log.info(
                "Refresh isolation task from db finished, current isolationTaskSize: {}, needToCancelIsolationTask: {}, needToAddIsolationTasks: {}, cost: {} ms",
                isolationTasksInMemory.size(), needToCancelIsolationTasks.size(), needToAddIsolationTasks.size(),
                stopWatch.getTime());
    }

    public boolean isIsolatedTask(int workflowInstanceId, long taskCode) {
        Set<Long> taskCodes = workflowIsolatedTaskCodeMap.get(workflowInstanceId);
        if (taskCodes == null) {
            return false;
        }
        return taskCodes.contains(taskCode);
    }

    private void cancelIsolationTask(List<IsolationTask> needCancelIsolationTasks) {
        if (CollectionUtils.isEmpty(needCancelIsolationTasks)) {
            return;
        }
        for (IsolationTask needCancelIsolation : needCancelIsolationTasks) {
            Set<Long> taskCodes = workflowIsolatedTaskCodeMap.get(needCancelIsolation.getWorkflowInstanceId());
            Set<IsolationTask> isolationTasks =
                    workflowIsolatedTaskMap.get(needCancelIsolation.getWorkflowInstanceId());
            if (taskCodes != null) {
                taskCodes.remove(needCancelIsolation.getTaskCode());
                isolationTasks.remove(needCancelIsolation);
                if (taskCodes.isEmpty()) {
                    workflowIsolatedTaskCodeMap.remove(needCancelIsolation.getWorkflowInstanceId());
                    workflowIsolatedTaskMap.remove(needCancelIsolation.getWorkflowInstanceId());
                }
            }

            WorkflowExecuteRunnable workflowExecuteRunnable = processInstanceExecCacheManager
                    .getByProcessInstanceId(needCancelIsolation.getWorkflowInstanceId());
            if (workflowExecuteRunnable == null) {
                continue;
            }
            workflowExecuteRunnable.cancelTaskIsolation(needCancelIsolation.getTaskCode());
            log.info("Backend cancel isolation task, isolationTaskId: {}", needCancelIsolation.getId());
        }
    }

    private void addIsolationTask(List<IsolationTask> needAddIsolationTasks) {
        if (CollectionUtils.isEmpty(needAddIsolationTasks)) {
            return;
        }
        for (IsolationTask needAddIsolationTask : needAddIsolationTasks) {
            workflowIsolatedTaskCodeMap
                    .computeIfAbsent(needAddIsolationTask.getWorkflowInstanceId(), k -> new HashSet<>())
                    .add(needAddIsolationTask.getTaskCode());
            workflowIsolatedTaskMap
                    .computeIfAbsent(needAddIsolationTask.getWorkflowInstanceId(), k -> new HashSet<>())
                    .add(needAddIsolationTask);
            WorkflowExecuteRunnable workflowExecuteRunnable = processInstanceExecCacheManager
                    .getByProcessInstanceId(needAddIsolationTask.getWorkflowInstanceId());
            if (workflowExecuteRunnable == null) {
                continue;
            }
            try {
                LoggerUtils.setWorkflowInstanceIdMDC(needAddIsolationTask.getWorkflowInstanceId());
                log.info("Begin to add new isolation task, taskCode: {}", needAddIsolationTask.getTaskCode());
                workflowExecuteRunnable.addTaskIsolation(needAddIsolationTask.getTaskCode());
            } catch (Exception ex) {
                log.error("Add new isolation task: {} failed, meet an known exception",
                        needAddIsolationTask.getTaskCode());
            } finally {
                LoggerUtils.removeWorkflowInstanceIdMDC();
            }
        }
    }

    public Set<IsolationTask> queryIsolationTasksByWorkflowInstanceId(int workflowInstanceId) {
        return workflowIsolatedTaskMap.getOrDefault(workflowInstanceId, Collections.emptySet());
    }
}
