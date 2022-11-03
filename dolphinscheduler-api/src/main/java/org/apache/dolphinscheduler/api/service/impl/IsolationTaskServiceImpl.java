package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.dolphinscheduler.api.checker.IsolationTaskChecker;
import org.apache.dolphinscheduler.api.dto.request.IsolationTaskListingRequest;
import org.apache.dolphinscheduler.api.dto.request.IsolationTaskSubmitRequest;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.remote.ApiServerRPCClient;
import org.apache.dolphinscheduler.api.service.IsolationTaskService;
import org.apache.dolphinscheduler.api.service.WorkflowDAGService;
import org.apache.dolphinscheduler.api.transformer.CommandTransformer;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.vo.IsolationTaskExcelParseVO;
import org.apache.dolphinscheduler.api.vo.IsolationTaskListingVO;
import org.apache.dolphinscheduler.common.enums.NodeType;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.repository.IsolationTaskDao;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.utils.DagHelper;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.remote.command.isolation.RefreshIsolationMetadataRequest;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.registry.RegistryClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_NOT_EXIST;

@Slf4j
@Service
public class IsolationTaskServiceImpl implements IsolationTaskService {

    @Autowired
    private IsolationTaskDao isolationTaskDao;

    @Autowired
    private IsolationTaskChecker isolationTaskChecker;

    @Autowired
    private ProcessInstanceDao processInstanceDao;

    @Autowired
    private CommandTransformer commandTransformer;

    @Autowired
    private ApiServerRPCClient apiServerRPCClient;

    @Autowired
    private RegistryClient registryClient;

    @Autowired
    private ProcessService processService;

    @Autowired
    private WorkflowDAGService workflowDAGService;

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Autowired
    private IsolationTaskService isolationTaskService;

    @Override
    public void submitTaskIsolations(@NonNull User loginUser,
                                     long projectCode,
                                     @NonNull IsolationTaskSubmitRequest isolationTaskSubmitRequest) {
        Map<Integer, List<IsolationTaskExcelParseVO>> workflow2VoMap =
                isolationTaskSubmitRequest.getIsolationTaskExcelParseVOList()
                        .stream().collect(
                                HashMap::new, (map, vo) -> {
                                    map.computeIfAbsent(vo.getWorkflowInstanceId(), k -> new ArrayList<>()).add(vo);
                                },
                                Map::putAll);

        List<Pair<ProcessInstance, List<IsolationTask>>> needToOnlineIsolations = new ArrayList<>();
        List<IsolationTask> needToInsertIntoDB = new ArrayList<>();
        for (Map.Entry<Integer, List<IsolationTaskExcelParseVO>> entry : workflow2VoMap.entrySet()) {
            Integer workflowInstanceId = entry.getKey();
            List<IsolationTaskExcelParseVO> vos = entry.getValue();
            ProcessInstance processInstance = processInstanceDao.queryProcessInstanceById(workflowInstanceId)
                    .orElseThrow(() -> new ServiceException(Status.PROCESS_INSTANCE_NOT_EXIST));
            isolationTaskChecker.checkCanSubmitTaskIsolation(loginUser, projectCode, processInstance, vos);

            List<IsolationTask> isolationTasks = entry.getValue().stream().map(vo -> {
                return IsolationTask.builder()
                        .projectCode(projectCode)
                        .workflowInstanceId(vo.getWorkflowInstanceId())
                        .workflowInstanceName(vo.getWorkflowInstanceName())
                        .taskName(vo.getTaskName())
                        .taskCode(vo.getTaskCode())
                        .build();
            }).collect(Collectors.toList());
            needToInsertIntoDB.addAll(isolationTasks);
            needToOnlineIsolations.add(Pair.of(processInstance, isolationTasks));
        }
        isolationTaskDao.batchInsert(needToInsertIntoDB);
        sendIsolationTaskRefreshRequestToMaster();
    }

    @Override
    public void cancelTaskIsolation(@NonNull User loginUser,
                                    long projectCode,
                                    long isolationId) {
        isolationTaskService.cancelTaskIsolationInDB(loginUser, projectCode, isolationId);
        sendIsolationTaskRefreshRequestToMaster();
    }

    @Transactional
    @Override
    public void cancelTaskIsolationInDB(@NonNull User loginUser, long projectCode, long isolationId) {
        IsolationTask isolationTask = isolationTaskDao.queryById(isolationId)
                .orElseThrow(() -> new ServiceException(ISOLATION_TASK_NOT_EXIST));

        Integer workflowInstanceId = isolationTask.getWorkflowInstanceId();
        ProcessInstance processInstance = processInstanceDao.queryProcessInstanceById(workflowInstanceId)
                .orElseThrow(() -> new ServiceException(Status.PROCESS_INSTANCE_NOT_EXIST));
        isolationTaskChecker.checkCanCancelTaskIsolation(loginUser, projectCode, processInstance, isolationTask);

        isolationTaskDao.deleteById(isolationTask.getId());
        insertRecoveryCommandIfNeeded(processInstance);
    }

    @Override
    public PageInfo<IsolationTaskListingVO> listingTaskIsolation(@NonNull User loginUser,
                                                                 long projectCode,
                                                                 @NonNull IsolationTaskListingRequest request) {
        isolationTaskChecker.checkCanListingTaskIsolation(loginUser, projectCode);

        Integer pageNo = request.getPageNo();
        Integer pageSize = request.getPageSize();

        IPage<IsolationTask> iPage = isolationTaskDao.pageQueryIsolationTask(
                request.getWorkflowInstanceName(),
                request.getTaskName(),
                projectCode,
                pageNo,
                pageSize);

        List<IsolationTask> isolationTasks = iPage.getRecords();

        Map<Integer, Map<Long, TaskInstance>> taskInstanceMap = taskInstanceDao
                .queryValidatedTaskInstanceByWorkflowInstanceId(
                        isolationTasks.stream().map(IsolationTask::getWorkflowInstanceId).collect(Collectors.toList()))
                .stream()
                .collect(HashMap::new,
                        (map, taskInstance) -> {
                            map.computeIfAbsent(taskInstance.getProcessInstanceId(), k -> new HashMap<>())
                                    .put(taskInstance.getTaskCode(), taskInstance);
                        },
                        Map::putAll);

        List<IsolationTaskListingVO> isolationTaskListingVOList = isolationTasks
                .stream()
                .map(isolationTask -> {
                    TaskInstance taskInstance =
                            taskInstanceMap.get(isolationTask.getWorkflowInstanceId()).get(isolationTask.getTaskCode());

                    IsolationTaskListingVO vo = IsolationTaskListingVO.builder()
                            .id(isolationTask.getId())
                            .workflowInstanceId(isolationTask.getWorkflowInstanceId())
                            .workflowInstanceName(isolationTask.getWorkflowInstanceName())
                            .taskName(isolationTask.getTaskName())
                            .taskCode(isolationTask.getTaskCode())
                            .createTime(isolationTask.getCreateTime())
                            .updateTime(isolationTask.getUpdateTime())
                            .build();
                    if (taskInstance != null) {
                        vo.setTaskStatus(taskInstance.getState());
                    }
                    return vo;
                }).collect(Collectors.toList());
        PageInfo<IsolationTaskListingVO> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotal((int) iPage.getTotal());
        pageInfo.setTotalList(isolationTaskListingVOList);
        return pageInfo;
    }

    private void sendIsolationTaskRefreshRequestToMaster() {
        List<Server> masters = registryClient.getServerList(NodeType.MASTER);
        if (CollectionUtils.isEmpty(masters)) {
            return;
        }

        org.apache.dolphinscheduler.remote.command.Command refreshIsolationRequest =
                new RefreshIsolationMetadataRequest().convert2Command();
        for (Server master : masters) {
            try {
                apiServerRPCClient.send(new Host(master.getHost(), master.getPort()), refreshIsolationRequest);
            } catch (RemotingException e) {
                log.error("Send RefreshIsolationTask request to master error, master: {}", master, e);
            }
        }
    }

    private void insertRecoveryCommandIfNeeded(@NonNull ProcessInstance processInstance) {
        if (processInstance.getState() != ExecutionStatus.PAUSE_BY_ISOLATION) {
            return;
        }
        log.info("The current workflow instance is in PAUSE_BY_ISOLATION status, will insert a recovery command");
        int workflowInstanceId = processInstance.getId();
        // find the isolationTaskInstanceIds need to recovery
        // find the sub node is in pause or kill
        DAG<String, TaskNode, TaskNodeRelation> workflowDAG = workflowDAGService.getWorkflowDAG(
                processInstance.getProcessDefinitionCode(),
                processInstance.getProcessDefinitionVersion());

        List<TaskInstance> taskInstances =
                taskInstanceDao.queryValidatedTaskInstanceByWorkflowInstanceId(workflowInstanceId);
        Set<String> onlineIsolationTaskCodes =
                isolationTaskDao.queryByWorkflowInstanceId(workflowInstanceId)
                        .stream()
                        .map(onlineIsolationTask -> String.valueOf(onlineIsolationTask.getTaskCode()))
                        .collect(Collectors.toSet());

        List<TaskInstance> canRecoveryTaskInstances = taskInstances.stream()
                .filter(taskInstance -> taskInstance.getState().typeIsIsolated())
                .filter(taskInstance -> !DagHelper.isChildOfAnyParentNodes(String.valueOf(taskInstance.getTaskCode()),
                        onlineIsolationTaskCodes, workflowDAG))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(canRecoveryTaskInstances)) {
            log.error("The current workflow instance has no task instance can recovery");
            return;
        }
        // find if this taskInstance still exist pre isolationTasks
        Command command = commandTransformer.transformToRecoveryFromTaskIsolationCommand(processInstance,
                canRecoveryTaskInstances);
        processService.createCommand(command);
    }
}
