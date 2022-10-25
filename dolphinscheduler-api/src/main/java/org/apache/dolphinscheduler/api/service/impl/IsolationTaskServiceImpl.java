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
import org.apache.dolphinscheduler.common.enums.NodeType;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.dao.dto.IsolationTaskStatus;
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
import org.apache.dolphinscheduler.remote.command.isolation.RefreshIsolationTaskRequest;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.registry.RegistryClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_NOT_EXIST;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_SUBMIT_ERROR_SEND_REQUEST_TO_MASTER_ERROR;

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

    @Override
    @Transactional
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
            ProcessInstance processInstance = processInstanceDao.queryProcessInstanceById(workflowInstanceId);
            isolationTaskChecker.checkCanSubmitTaskIsolation(loginUser, projectCode, processInstance, vos);

            List<IsolationTask> isolationTasks = entry.getValue().stream().map(vo -> {
                return IsolationTask.builder()
                        .workflowInstanceId(vo.getWorkflowInstanceId())
                        .workflowInstanceName(vo.getWorkflowInstanceName())
                        .taskName(vo.getTaskName())
                        .taskCode(vo.getTaskCode())
                        .status(IsolationTaskStatus.ONLINE.getCode())
                        .build();
            }).collect(Collectors.toList());
            needToInsertIntoDB.addAll(isolationTasks);
            needToOnlineIsolations.add(Pair.of(processInstance, isolationTasks));
        }
        isolationTaskDao.batchInsert(needToInsertIntoDB);
        // we split here to avoid rollback RPC request
        try {
            refreshIsolationTasks();
        } catch (Exception ex) {
            throw new ServiceException(ISOLATION_TASK_SUBMIT_ERROR_SEND_REQUEST_TO_MASTER_ERROR);
        }
    }

    @Override
    @Transactional
    public void onlineTaskIsolation(@NonNull User loginUser, long projectCode, long isolationTaskId) {
        IsolationTask isolationTask = isolationTaskDao.queryById(isolationTaskId)
                .orElseThrow(() -> new ServiceException(ISOLATION_TASK_NOT_EXIST));
        ProcessInstance processInstance =
                processInstanceDao.queryProcessInstanceById(isolationTask.getWorkflowInstanceId());

        isolationTaskChecker.checkCanOnlineTaskIsolation(loginUser, projectCode, processInstance, isolationTask);
        isolationTaskDao.updateIsolationTaskStatus(isolationTaskId, IsolationTaskStatus.ONLINE);
        try {
            refreshIsolationTasks();
        } catch (Exception ex) {
            throw new ServiceException(Status.ISOLATION_TASK_ONLINE_ERROR);
        }
    }

    @Override
    @Transactional
    public void cancelTaskIsolation(@NonNull User loginUser,
                                    long projectCode,
                                    long isolationId) {
        IsolationTask isolationTask = isolationTaskDao.queryById(isolationId)
                .orElseThrow(() -> new ServiceException(ISOLATION_TASK_NOT_EXIST));

        Integer workflowInstanceId = isolationTask.getWorkflowInstanceId();
        ProcessInstance processInstance = processInstanceDao.queryProcessInstanceById(workflowInstanceId);
        isolationTaskChecker.checkCanCancelTaskIsolation(loginUser, projectCode, processInstance, isolationTask);
        isolationTaskDao.updateIsolationTaskStatus(isolationTask.getId(), IsolationTaskStatus.OFFLINE);
        insertRecoveryCommandIfNeeded(processInstance, isolationTask);
        try {
            refreshIsolationTasks();
        } catch (RemotingException | InterruptedException e) {
            throw new ServiceException(Status.ISOLATION_TASK_CANCEL_ERROR);
        }
    }

    @Override
    public PageInfo<IsolationTask> listingTaskIsolation(@NonNull User loginUser,
                                                        long projectCode,
                                                        @NonNull IsolationTaskListingRequest request) {
        isolationTaskChecker.checkCanListingTaskIsolation(loginUser, projectCode);

        Integer pageNo = request.getPageNo();
        Integer pageSize = request.getPageSize();

        IPage<IsolationTask> iPage = isolationTaskDao.pageQueryIsolationTask(
                request.getWorkflowInstanceName(),
                request.getTaskName(),
                pageNo,
                pageSize);

        PageInfo<IsolationTask> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotal((int) iPage.getTotal());
        pageInfo.setTotalList(iPage.getRecords());
        return pageInfo;
    }

    @Override
    public void deleteTaskIsolation(@NonNull User loginUser, long projectCode, long id) {
        isolationTaskChecker.checkCanDeleteTaskIsolation(loginUser, projectCode, id);
        int deleteNum = isolationTaskDao.deleteByIdAndStatus(id, IsolationTaskStatus.OFFLINE);
        if (deleteNum <= 0) {
            throw new ServiceException(ISOLATION_TASK_NOT_EXIST);
        }
    }

    private void refreshIsolationTasks() throws RemotingException, InterruptedException {
        List<Server> masters = registryClient.getServerList(NodeType.MASTER);
        if (CollectionUtils.isEmpty(masters)) {
            return;
        }

        org.apache.dolphinscheduler.remote.command.Command refreshIsolationRequest =
                new RefreshIsolationTaskRequest().convert2Command();
        for (Server master : masters) {
            try {
                apiServerRPCClient.sendSyncCommand(new Host(master.getHost(), master.getPort()),
                        refreshIsolationRequest);
            } catch (RemotingException | InterruptedException e) {
                log.error("Send RefreshIsolationTask request to master error, master: {}", master, e);
                throw e;
            }
        }
    }

    private void insertRecoveryCommandIfNeeded(@NonNull ProcessInstance processInstance,
                                               @NonNull IsolationTask isolationTask) {
        if (processInstance.getState() != ExecutionStatus.PAUSE_BY_ISOLATION) {
            return;
        }
        int workflowInstanceId = processInstance.getId();
        // find the isolationTaskInstanceIds need to recovery
        // find the sub node is in pause or kill
        DAG<String, TaskNode, TaskNodeRelation> workflowDAG = workflowDAGService.getWorkflowDAG(
                processInstance.getProcessDefinitionCode(),
                processInstance.getProcessDefinitionVersion());

        List<TaskInstance> taskInstances =
                taskInstanceDao.queryValidatedTaskInstanceByWorkflowInstanceId(workflowInstanceId);
        Set<String> onlineIsolationTaskCodes =
                isolationTaskDao.queryByWorkflowInstanceId(workflowInstanceId, IsolationTaskStatus.ONLINE)
                        .stream()
                        .map(onlineIsolationTask -> String.valueOf(onlineIsolationTask.getTaskCode()))
                        .collect(Collectors.toSet());

        List<TaskInstance> canRecoveryTaskInstances = taskInstances.stream()
                .filter(taskInstance -> taskInstance.getState().typeIsIsolated())
                .filter(taskInstance -> !DagHelper.isChildOfAnyParentNodes(String.valueOf(taskInstance.getTaskCode()),
                        onlineIsolationTaskCodes, workflowDAG))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(canRecoveryTaskInstances)) {
            return;
        }
        // find if this taskInstance still exist pre isolationTasks
        Command command = commandTransformer.transformToRecoveryFromTaskIsolationCommand(processInstance,
                canRecoveryTaskInstances);
        processService.createCommand(command);
    }
}
