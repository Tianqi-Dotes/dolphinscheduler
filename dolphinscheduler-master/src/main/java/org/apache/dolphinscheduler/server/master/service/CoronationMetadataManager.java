package org.apache.dolphinscheduler.server.master.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.dolphinscheduler.common.enums.NodeType;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.utils.LoggerUtils;
import org.apache.dolphinscheduler.dao.dto.CoronationTaskDTO;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.CommandDao;
import org.apache.dolphinscheduler.dao.repository.CoronationTaskDao;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.remote.command.coronation.RefreshCoronationMetadataRequest;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.server.master.cache.ProcessInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.rpc.MasterRPCClient;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.apache.dolphinscheduler.server.master.transformer.MasterCommandTransformer;
import org.apache.dolphinscheduler.service.registry.RegistryClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CoronationMetadataManager {

    @Autowired
    private CoronationTaskDao coronationTaskDao;

    @Autowired
    private RegistryClient registryClient;

    @Autowired
    private ProcessInstanceDao processInstanceDao;

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Autowired
    private MasterCommandTransformer commandTransformer;

    @Autowired
    private CommandDao commandDao;

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    @Autowired
    private MasterRPCClient masterRPCClient;

    private volatile CoronationMode coronationMode = CoronationMode.NOT_IN_CORONATION;

    // coronationId -> coronationTask
    private volatile Map<Long, CoronationTaskDTO> coronationTaskInMemory = new HashMap<>();
    private final Map<Integer, Set<CoronationTaskDTO>> workflowCoronationTaskMap = new HashMap<>();
    // workflowInstanceId -> coronationTaskCodes
    private final Map<Integer, Set<Long>> workflowCoronationTaskCodeMap = new HashMap<>();

    @PostConstruct
    public void init() {
        refreshCoronationTaskMetadata();
    }

    public synchronized void refreshCoronationTaskMetadata() {
        StopWatch stopWatch = StopWatch.createStarted();
        Map<Long, CoronationTaskDTO> coronationTaskInDB = coronationTaskDao.queryAllCoronationTasks()
                .stream()
                .collect(Collectors.toMap(CoronationTaskDTO::getId, Function.identity()));

        List<CoronationTaskDTO> addCoronationTasks = coronationTaskInDB.values()
                .stream()
                .filter(coronationTask -> !coronationTaskInMemory.containsKey(coronationTask.getId()))
                .collect(Collectors.toList());
        List<CoronationTaskDTO> deleteCoronationTasks = coronationTaskInMemory.values()
                .stream()
                .filter(coronationTask -> !coronationTaskInDB.containsKey(coronationTask.getId()))
                .collect(Collectors.toList());
        coronationTaskInMemory = coronationTaskInDB;
        if (coronationTaskInMemory.isEmpty()) {
            if (coronationMode == CoronationMode.IN_CORONATION) {
                log.info("There is not coronation tasks, will begin to close coronation mode...");
                coronationMode = CoronationMode.NOT_IN_CORONATION;
                log.info("Close coronation mode success...");
            }
            insertRecoveryCoronationCommandIfNeeded();
        } else {
            addCoronationTasks(addCoronationTasks);
            cancelCoronationTasks(deleteCoronationTasks);
            if (coronationMode == CoronationMode.NOT_IN_CORONATION) {
                coronationMode = CoronationMode.IN_CORONATION;
                log.info("Open the coronation mode...");
            }
        }
        stopWatch.stop();
        log.info(
                "Refresh coronation task from DB finished, coronationTaskSize: {}, needToAddCoronationTasksSize: {}, needToCancelCoronationTaskSize: {}, cost: {} ms",
                coronationTaskInMemory.size(), addCoronationTasks.size(), deleteCoronationTasks.size(),
                stopWatch.getTime());
    }

    public boolean isCoronationTask(int workflowInstanceId, long taskCode) {
        Set<Long> taskCodes = workflowCoronationTaskCodeMap.get(workflowInstanceId);
        if (taskCodes == null) {
            return false;
        }
        return taskCodes.contains(taskCode);
    }

    public void deleteCoronationMetadataInDB(long taskCode) {
        Optional<CoronationTaskDTO> successCoronationTask = coronationTaskInMemory.values()
                .stream()
                .filter(coronationTask -> coronationTask.getTaskCode() == taskCode)
                .findAny();
        if (!successCoronationTask.isPresent()) {
            return;
        }
        coronationTaskDao.deleteById(successCoronationTask.get().getId());
        if (coronationTaskDao.queryAllCoronationTaskNumber() == 0) {
            // sendCoronationRefreshRequestTo All master
            try {
                log.info("There is no coronation tasks, will send refresh coronation meta data to all master");
                List<Server> masters = registryClient.getServerList(NodeType.MASTER);
                RefreshCoronationMetadataRequest request = new RefreshCoronationMetadataRequest();
                for (Server master : masters) {
                    try {
                        masterRPCClient.sendCommand(new Host(master.getHost(), master.getPort()),
                                request.convert2Command());
                    } catch (Exception e) {
                        log.error(
                                "Close coronation failed, send refresh coronation metadata request to master: {} error",
                                master, e);
                    }
                }
            } catch (Exception ex) {
                log.error(
                        "Close coronation failed, meet an unknown exception, will wait the master to auto refresh the coronation metadata",
                        ex);
            }
        }
    }

    private void insertRecoveryCoronationCommandIfNeeded() {
        // The current server is in coronation mode, need to close coronation.
        // Need to acquire a lock to guarantee there is only one master recovery the pause_by_coronation workflow
        // block to acquire the master lock
        try {
            if (!registryClient.getLock(NodeType.MASTER.getRegistryPath())) {
                log.warn("Cannot acquire the master lock: {} to close coronation", NodeType.MASTER.getRegistryPath());
                return;
            }
            // find the all instance that need to be recovery
            // create recovery command
            List<Command> needToInsertCommand =
                    processInstanceDao.queryProcessInstanceByStatus(ExecutionStatus.PAUSE_BY_CORONATION)
                            .stream()
                            .filter(processInstance -> {
                                List<Command> commands = commandDao
                                        .queryRecoveryCoronationCommandByWorkflowInstanceId(processInstance.getId());
                                // this workflow instance has not been recovery.
                                return CollectionUtils.isEmpty(commands);
                            })
                            .map(processInstance -> {
                                List<Integer> needToRecoveryTaskInstanceIds = taskInstanceDao
                                        .queryValidatedTaskInstanceByWorkflowInstanceIdAndStatus(
                                                processInstance.getId(), ExecutionStatus.PAUSE_BY_CORONATION)
                                        .stream()
                                        .map(TaskInstance::getId)
                                        .collect(Collectors.toList());
                                return commandTransformer.transformToRecoveryFromCoronationPauseCommand(processInstance,
                                        needToRecoveryTaskInstanceIds);
                            })
                            .collect(Collectors.toList());
            commandDao.batchInsertCommand(needToInsertCommand);
        } finally {
            registryClient.releaseLock(NodeType.MASTER.getRegistryPath());
        }
    }

    private void addCoronationTasks(List<CoronationTaskDTO> coronationTasksNeedToOnline) {
        if (CollectionUtils.isEmpty(coronationTasksNeedToOnline)) {
            return;
        }
        for (CoronationTaskDTO coronationTask : coronationTasksNeedToOnline) {
            workflowCoronationTaskCodeMap.computeIfAbsent(coronationTask.getWorkflowInstanceId(), k -> new HashSet<>())
                    .add(coronationTask.getTaskCode());
            workflowCoronationTaskMap.computeIfAbsent(coronationTask.getWorkflowInstanceId(), k -> new HashSet<>())
                    .add(coronationTask);
            WorkflowExecuteRunnable workflowExecuteRunnable =
                    processInstanceExecCacheManager.getByProcessInstanceId(coronationTask.getWorkflowInstanceId());
            if (workflowExecuteRunnable == null) {
                continue;
            }
            try {
                LoggerUtils.setWorkflowInstanceIdMDC(coronationTask.getWorkflowInstanceId());
                log.info("Begin to add new coronation task: {}", coronationTask);
                workflowExecuteRunnable.onlineTaskCoronation(coronationTask);
            } catch (Exception ex) {
                log.error("Add new coronation task: {} failed, meet an unknown exception", coronationTask, ex);
            } finally {
                LoggerUtils.removeWorkflowInstanceIdMDC();
            }
        }
    }

    private void cancelCoronationTasks(List<CoronationTaskDTO> coronationTasksNeedToOffline) {
        if (CollectionUtils.isEmpty(coronationTasksNeedToOffline)) {
            return;
        }
        for (CoronationTaskDTO coronationTask : coronationTasksNeedToOffline) {
            Set<Long> taskCodes = workflowCoronationTaskCodeMap.get(coronationTask.getWorkflowInstanceId());
            Set<CoronationTaskDTO> coronationTasks =
                    workflowCoronationTaskMap.get(coronationTask.getWorkflowInstanceId());
            if (taskCodes != null) {
                taskCodes.remove(coronationTask.getTaskCode());
                coronationTasks.remove(coronationTask);
                if (taskCodes.isEmpty()) {
                    workflowCoronationTaskCodeMap.remove(coronationTask.getWorkflowInstanceId());
                    workflowCoronationTaskMap.remove(coronationTask.getWorkflowInstanceId());
                }
            }
            WorkflowExecuteRunnable workflowExecuteRunnable =
                    processInstanceExecCacheManager.getByProcessInstanceId(coronationTask.getWorkflowInstanceId());
            if (workflowExecuteRunnable == null) {
                continue;
            }
            try {
                LoggerUtils.setWorkflowInstanceIdMDC(coronationTask.getWorkflowInstanceId());
                log.info("Begin to cancel coronation task: {}", coronationTask);
                workflowExecuteRunnable.cancelTaskCoronation(coronationTask);
            } catch (Exception ex) {
                log.error("Cancel coronation task: {} failed, meed an unknown exception", coronationTask, ex);
            } finally {
                LoggerUtils.removeWorkflowInstanceIdMDC();
            }
        }
    }

    public boolean isInCoronationMode() {
        return coronationMode == CoronationMode.IN_CORONATION;
    }

    public Set<CoronationTaskDTO> getCoronationTasksByWorkflowInstanceId(int workflowInstanceId) {
        return workflowCoronationTaskMap.getOrDefault(workflowInstanceId, Collections.emptySet());
    }

    public enum CoronationMode {

        IN_CORONATION(0, "The current server is already in coronation mode"),
        NOT_IN_CORONATION(1, "The current server is not in coronation mode"),
        ;

        private final int code;
        private final String desc;

        CoronationMode(int code, String desc) {
            this.code = code;
            this.desc = desc;
        }

    }
}
