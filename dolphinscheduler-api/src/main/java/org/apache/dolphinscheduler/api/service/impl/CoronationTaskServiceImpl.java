package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.api.checker.CoronationTaskChecker;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskListingRequest;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskParseRequest;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskSubmitRequest;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.remote.ApiServerRPCClient;
import org.apache.dolphinscheduler.api.service.CoronationTaskService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.service.WorkflowDAGService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.vo.CoronationTaskExcelImportVO;
import org.apache.dolphinscheduler.api.vo.CoronationTaskParseVO;
import org.apache.dolphinscheduler.common.enums.NodeType;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.dto.CoronationTaskDTO;
import org.apache.dolphinscheduler.dao.dto.TaskSimpleInfoDTO;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.repository.CoronationTaskDao;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.utils.DagHelper;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.coronation.RefreshCoronationMetadataRequest;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.service.registry.RegistryClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CoronationTaskServiceImpl implements CoronationTaskService {

    @Autowired
    private CoronationTaskChecker coronationTaskChecker;

    @Autowired
    private CoronationTaskDao coronationTaskDao;

    @Autowired
    private ProcessInstanceDao processInstanceDao;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private WorkflowDAGService workflowDAGService;

    @Autowired
    private RegistryClient registryClient;

    @Autowired
    private ApiServerRPCClient apiServerRPCClient;

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Override
    public List<CoronationTaskParseVO> parseCoronationTask(@NonNull User loginUser,
                                                           long projectCode,
                                                           @NonNull CoronationTaskParseRequest request) {
        List<CoronationTaskExcelImportVO> coronationTasks = request.getCoronationTasks();
        Project project = projectService.queryByCode(loginUser, projectCode);
        return coronationTasks.stream()
                .map(vo -> {
                    String taskCode = Long.toString(vo.getTaskCode());
                    ProcessInstance workflowInstance =
                            processInstanceDao.queryProcessInstanceById(vo.getWorkflowInstanceId())
                                    .orElseThrow(() -> new ServiceException(Status.PROCESS_INSTANCE_NOT_EXIST));
                    coronationTaskChecker.checkCanParseCoronationTask(loginUser, project, workflowInstance, vo);
                    DAG<String, TaskNode, TaskNodeRelation> workflowDAG =
                            workflowDAGService.getWorkflowDAG(workflowInstance.getProcessDefinitionCode(),
                                    workflowInstance.getProcessDefinitionVersion());
                    if (!vo.getTaskName().equals(workflowDAG.getNode(taskCode).getName())) {
                        throw new ServiceException(Status.CORONATION_TASK_PARSE_ERROR_TASK_NODE_NAME_IS_NOT_VALIDATED);
                    }
                    List<TaskSimpleInfoDTO> previousTaskNodeDTO =
                            DagHelper.getAllPreNodes(taskCode, workflowDAG)
                                    .stream()
                                    .filter(previousNodeCode -> !previousNodeCode.equals(taskCode))
                                    .map(previousNodeCode -> {
                                        TaskNode node = workflowDAG.getNode(previousNodeCode);
                                        return new TaskSimpleInfoDTO(node.getName(), node.getCode());
                                    }).collect(Collectors.toList());
                    return CoronationTaskParseVO.builder()
                            .workflowInstanceId(workflowInstance.getId())
                            .workflowInstanceName(workflowInstance.getName())
                            .taskNode(vo.getTaskName())
                            .taskCode(vo.getTaskCode())
                            .upstreamTasks(previousTaskNodeDTO)
                            .build();
                }).collect(Collectors.toList());
    }

    @Override
    public void submitCoronationTask(@NonNull User loginUser, long projectCode,
                                     @NonNull CoronationTaskSubmitRequest request) {
        Project project = projectService.queryByCode(loginUser, projectCode);

        Map<Integer, List<CoronationTaskParseVO>> coronationTaskMap = request.getCoronationTasks().stream()
                .collect(Collectors.groupingBy(CoronationTaskParseVO::getWorkflowInstanceId));

        List<CoronationTask> needToInsertIntoDB = new ArrayList<>();

        for (Map.Entry<Integer, List<CoronationTaskParseVO>> entry : coronationTaskMap.entrySet()) {
            Integer workflowInstanceId = entry.getKey();
            List<CoronationTaskParseVO> vos = entry.getValue();

            ProcessInstance workflowInstance = processInstanceDao.queryProcessInstanceById(workflowInstanceId)
                    .orElseThrow(() -> new ServiceException(Status.PROCESS_INSTANCE_NOT_EXIST));
            DAG<String, TaskNode, TaskNodeRelation> workflowDAG = workflowDAGService.getWorkflowDAG(
                    workflowInstance.getProcessDefinitionCode(), workflowInstance.getProcessDefinitionVersion());
            coronationTaskChecker.checkCanSubmitTaskCoronation(loginUser, project, workflowInstance, workflowDAG, vos);

            List<CoronationTask> coronationTasks = vos.stream()
                    .map(vo -> {
                        String coronationTaskCode = Long.toString(vo.getTaskCode());
                        Set<String> previousNodes = DagHelper.getAllPreNodes(coronationTaskCode, workflowDAG);
                        Set<String> selectNodes = vo.getUpstreamTasks()
                                .stream()
                                .map(taskNode -> String.valueOf(taskNode.getTaskCode()))
                                .collect(Collectors.toSet());
                        // The upstream node hasn't been selected will be set to forbidden execute
                        List<TaskSimpleInfoDTO> needToForbiddenTaskCodes =
                                CollectionUtils.subtract(previousNodes, selectNodes)
                                        .stream()
                                        .filter(taskCode -> !taskCode.equals(coronationTaskCode))
                                        .map(taskNode -> {
                                            TaskNode node = workflowDAG.getNode(taskNode);
                                            return new TaskSimpleInfoDTO(node.getName(), node.getCode());
                                        }).collect(Collectors.toList());

                        return CoronationTask.builder()
                                .workflowInstanceId(vo.getWorkflowInstanceId())
                                .workflowInstanceName(vo.getWorkflowInstanceName())
                                .taskName(vo.getTaskNode())
                                .taskCode(vo.getTaskCode())
                                .forbiddenUpstreamTasks(JSONUtils.toJsonString(needToForbiddenTaskCodes))
                                .build();
                    }).collect(Collectors.toList());
            needToInsertIntoDB.addAll(coronationTasks);
        }
        try {
            coronationTaskDao.batchInsert(needToInsertIntoDB);
        } catch (Exception ex) {
            log.error("Insert coronation task into db failed", ex);
            throw new ServiceException(Status.CORONATION_TASK_SUBMIT_ERROR);
        }
        sendSyncCoronationTasksRequestToMaster();
    }

    @Override
    public void cancelCoronationTask(@NonNull User loginUser, long projectCode, long id) {
        // do checker
        CoronationTask coronationTask = coronationTaskDao.queryCoronationTaskById(id)
                .orElseThrow(() -> new ServiceException(Status.CORONATION_TASK_NOT_EXIST));
        ProcessInstance workflowInstance =
                processInstanceDao.queryProcessInstanceById(coronationTask.getWorkflowInstanceId())
                        .orElseThrow(() -> new ServiceException(Status.PROCESS_INSTANCE_NOT_EXIST));
        coronationTaskChecker.checkCanCancelTaskCoronation(loginUser, projectCode, workflowInstance, coronationTask);
        coronationTaskDao.deleteById(id);
        sendSyncCoronationTasksRequestToMaster();
    }

    @Override
    public PageInfo<CoronationTaskDTO> listingCoronationTasks(@NonNull User loginUser, long projectCode,
                                                              @NonNull CoronationTaskListingRequest request) {
        coronationTaskChecker.checkCanListingTaskCoronation(loginUser, projectCode);

        Integer pageNo = request.getPageNo();
        Integer pageSize = request.getPageSize();

        IPage<CoronationTask> iPage = coronationTaskDao.pageQueryCoronationTask(
                request.getWorkflowInstanceName(),
                request.getTaskName(),
                pageNo,
                pageSize);

        List<CoronationTask> coronationTasks = iPage.getRecords();

        Map<Integer, Map<Long, TaskInstance>> taskInstanceMap = taskInstanceDao
                .queryValidatedTaskInstanceByWorkflowInstanceId(coronationTasks.stream()
                        .map(CoronationTask::getWorkflowInstanceId).collect(Collectors.toList()))
                .stream()
                .collect(HashMap::new,
                        (map, taskInstance) -> {
                            map.computeIfAbsent(taskInstance.getProcessInstanceId(), k -> new HashMap<>())
                                    .put(taskInstance.getTaskCode(), taskInstance);
                        },
                        Map::putAll);

        List<CoronationTaskDTO> coronationTaskDTOs = iPage.getRecords()
                .stream()
                .map(coronationTask -> {
                    CoronationTaskDTO coronationTaskDTO = new CoronationTaskDTO(coronationTask);
                    TaskInstance taskInstance = taskInstanceMap.get(coronationTask.getWorkflowInstanceId())
                            .get(coronationTask.getTaskCode());
                    if (taskInstance != null) {
                        coronationTaskDTO.setTaskStatus(taskInstance.getState());
                    }
                    return coronationTaskDTO;
                }).collect(Collectors.toList());

        PageInfo<CoronationTaskDTO> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotal((int) iPage.getTotal());
        // inject taskStatus
        pageInfo.setTotalList(coronationTaskDTOs);

        return pageInfo;
    }

    private void sendSyncCoronationTasksRequestToMaster() {
        List<Server> masters = registryClient.getServerList(NodeType.MASTER);
        if (CollectionUtils.isEmpty(masters)) {
            return;
        }
        Command command = new RefreshCoronationMetadataRequest().convert2Command();
        for (Server master : masters) {
            try {
                apiServerRPCClient.send(new Host(master.getHost(), master.getPort()), command);
                log.info("Send RefreshCoronationTask request to master: {}:{}", master.getHost(), master.getPort());
            } catch (RemotingException e) {
                log.error("Send RefreshCoronationTask request to master error, master: {}", master, e);
            }
        }
    }
}
