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
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.repository.CoronationTaskDao;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.coronation.RefreshCoronationMetadataRequest;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.service.registry.RegistryClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
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

    @Override
    public List<CoronationTaskParseVO> parseCoronationTask(@NonNull User loginUser,
                                                           long projectCode,
                                                           @NonNull CoronationTaskParseRequest request) {
        List<CoronationTaskExcelImportVO> coronationTasks = request.getCoronationTasks();
        Project project = projectService.queryByCode(loginUser, projectCode);
        return coronationTasks.stream()
                .map(vo -> {
                    ProcessInstance workflowInstance =
                            processInstanceDao.queryProcessInstanceById(vo.getWorkflowInstanceId())
                                    .orElseThrow(() -> new ServiceException(Status.PROCESS_INSTANCE_NOT_EXIST));
                    coronationTaskChecker.checkCanParseCoronationTask(loginUser, project, workflowInstance, vo);
                    DAG<String, TaskNode, TaskNodeRelation> workflowDAG =
                            workflowDAGService.getWorkflowDAG(workflowInstance.getProcessDefinitionCode(),
                                    workflowInstance.getProcessDefinitionVersion());
                    if (!vo.getTaskName().equals(workflowDAG.getNode(Long.toString(vo.getTaskCode())).getName())) {
                        throw new ServiceException(Status.CORONATION_TASK_PARSE_ERROR_TASK_NODE_NAME_IS_NOT_VALIDATED);
                    }
                    List<TaskSimpleInfoDTO> previousTaskNodeDTO =
                            workflowDAG.getPreviousNodes(Long.toString(vo.getTaskCode()))
                                    .stream()
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
                        Set<String> previousNodes = workflowDAG.getPreviousNodes(vo.getTaskCode().toString());
                        Set<String> selectNodes = vo.getUpstreamTasks()
                                .stream()
                                .map(taskNode -> Long.toString(taskNode.getTaskCode()))
                                .collect(Collectors.toSet());
                        // The upstream node hasn't been selected will be set to forbidden execute
                        List<TaskSimpleInfoDTO> needToForbiddenTaskCodes =
                                CollectionUtils.subtract(previousNodes, selectNodes)
                                        .stream()
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
        coronationTaskDao.batchInsert(needToInsertIntoDB);
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

        PageInfo<CoronationTaskDTO> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotal((int) iPage.getTotal());
        pageInfo.setTotalList(iPage.getRecords().stream().map(CoronationTaskDTO::new).collect(Collectors.toList()));

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
            } catch (RemotingException e) {
                log.error("Send RefreshCoronationTask request to master error, master: {}", master, e);
            }
        }
    }
}
