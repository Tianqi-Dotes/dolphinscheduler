package org.apache.dolphinscheduler.api.checker;

import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.service.WorkflowDAGService;
import org.apache.dolphinscheduler.api.vo.IsolationTaskExcelParseVO;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.dao.dto.IsolationTaskStatus;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.repository.IsolationTaskDao;
import org.apache.dolphinscheduler.dao.repository.ProcessDefinitionLogDao;
import org.apache.dolphinscheduler.dao.utils.DagHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ISOLATION_TASK_CANCEL;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ISOLATION_TASK_DELETE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ISOLATION_TASK_LIST;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ISOLATION_TASK_ONLINE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ISOLATION_TASK_SUBMIT;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_CANCEL_ERROR_THE_ISOLATION_ALREADY_CANCEL;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_DELETE_ERROR_IS_NOT_OFFLINE;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_NOT_EXIST;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_ONLINE_ERROR_ALREADY_ONLINE;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_ONLINE_ERROR_PROCESS_NOT_BELONG_TO_PROJECT_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_SUBMIT_ERROR_EXIST_SUB_ISOLATION_TASK;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_SUBMIT_ERROR_TASK_NOT_EXIST;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_SUBMIT_ERROR_WORKFLOW_INSTANCE_NOT_BELONG_TO_CURRENT_PROJECT;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_SUBMIT_ERROR_WORKFLOW_INSTANCE_NOT_SUPPORT;
import static org.apache.dolphinscheduler.api.enums.Status.PROCESS_DEFINE_NOT_EXIST;
import static org.apache.dolphinscheduler.api.enums.Status.PROCESS_INSTANCE_NOT_EXIST;

@Component
public class IsolationTaskCheckerImpl implements IsolationTaskChecker {

    @Autowired
    private ProjectService projectService;

    @Autowired
    private ProcessDefinitionLogDao processDefinitionLogDao;

    @Autowired
    private WorkflowDAGService workflowDAGService;

    @Autowired
    private IsolationTaskDao isolationTaskDao;

    @Override
    public void checkCanSubmitTaskIsolation(@NonNull User loginUser,
                                            long projectCode,
                                            ProcessInstance processInstance,
                                            @NonNull List<IsolationTaskExcelParseVO> voList) {
        Project project = projectService.queryByCode(loginUser, projectCode);
        if (processInstance == null) {
            throw new ServiceException(PROCESS_INSTANCE_NOT_EXIST);
        }

        ProcessDefinitionLog processDefinitionLog = processDefinitionLogDao
                .queryProcessDefinitionByCode(processInstance.getProcessDefinitionCode(),
                        processInstance.getProcessDefinitionVersion())
                .orElseThrow(() -> new ServiceException(PROCESS_DEFINE_NOT_EXIST, processInstance.getName()));
        checkSubmitIsolateTaskAuth(loginUser, project, processDefinitionLog);

        DAG<String, TaskNode, TaskNodeRelation> workflowDAG = workflowDAGService.getWorkflowDAG(processDefinitionLog);
        checkWorkflowInstanceCanSubmitIsolateTask(processInstance.getName(), processInstance);

        checkTaskNodeCanSubmitIsolateTask(voList, workflowDAG);
    }

    @Override
    public void checkCanOnlineTaskIsolation(@NonNull User loginUser,
                                            long projectCode,
                                            ProcessInstance processInstance,
                                            @NonNull IsolationTask isolationTask) {
        Project project = projectService.queryByCode(loginUser, projectCode);
        checkOnlineIsolationTaskAuth(loginUser, project, processInstance);

        if (IsolationTaskStatus.ONLINE.getCode() == isolationTask.getStatus()) {
            throw new ServiceException(ISOLATION_TASK_ONLINE_ERROR_ALREADY_ONLINE);
        }

        String workflowInstanceName = isolationTask.getWorkflowInstanceName();
        checkWorkflowInstanceCanSubmitIsolateTask(workflowInstanceName, processInstance);
    }

    @Override
    public void checkCanListingTaskIsolation(@NonNull User loginUser, long projectCode) {
        Project project = projectService.queryByCode(loginUser, projectCode);

        projectService.checkProjectAndAuth(loginUser, project, projectCode, ISOLATION_TASK_LIST);
    }

    @Override
    public void checkCanDeleteTaskIsolation(@NonNull User loginUser,
                                            long projectCode,
                                            long isolationId) {
        Project project = projectService.queryByCode(loginUser, projectCode);
        projectService.checkProjectAndAuth(loginUser, project, projectCode, ISOLATION_TASK_DELETE);

        IsolationTask isolationTask = isolationTaskDao.queryById(isolationId)
                .orElseThrow(() -> new ServiceException(ISOLATION_TASK_NOT_EXIST));
        if (isolationTask.getStatus() != IsolationTaskStatus.OFFLINE.getCode()) {
            throw new ServiceException(ISOLATION_TASK_DELETE_ERROR_IS_NOT_OFFLINE);
        }
    }

    @Override
    public void checkCanCancelTaskIsolation(@NonNull User loginUser,
                                            long projectCode,
                                            ProcessInstance processInstance,
                                            @NonNull IsolationTask isolationTask) {
        if (processInstance == null) {
            throw new ServiceException(PROCESS_INSTANCE_NOT_EXIST);
        }
        Project project = projectService.queryByCode(loginUser, projectCode);
        checkCancelIsolationTaskAuth(loginUser, project, processInstance);

        if (isolationTask.getStatus() == IsolationTaskStatus.OFFLINE.getCode()) {
            throw new ServiceException(ISOLATION_TASK_CANCEL_ERROR_THE_ISOLATION_ALREADY_CANCEL);
        }
    }

    private void checkWorkflowInstanceCanSubmitIsolateTask(@NonNull String workflowInstanceName,
                                                           ProcessInstance processInstance) {
        // check if the given workflow instance can do isolate operation
        // If the workflow instance is at ready_xx status, it cannot do isolate operation
        if (processInstance == null) {
            throw new ServiceException(PROCESS_INSTANCE_NOT_EXIST, workflowInstanceName);
        }
        if (processInstance.getState().typeIsReady()) {
            throw new ServiceException(ISOLATION_TASK_SUBMIT_ERROR_WORKFLOW_INSTANCE_NOT_SUPPORT, workflowInstanceName);
        }
    }

    private void checkTaskNodeCanSubmitIsolateTask(@NonNull List<IsolationTaskExcelParseVO> voList,
                                                   @NonNull DAG<String, TaskNode, TaskNodeRelation> workflowDAG) {
        for (IsolationTaskExcelParseVO vo : voList) {
            String taskCodeStr = Long.toString(vo.getTaskCode());
            // check if the taskNode exist in DAG
            if (!workflowDAG.containsNode(taskCodeStr)) {
                throw new ServiceException(ISOLATION_TASK_SUBMIT_ERROR_TASK_NOT_EXIST, vo.getTaskName());
            }
            // check if the pre task exist an online isolation task
            // if existed, we cannot create a new isolation task
            Set<String> allPreNodes = DagHelper.getAllPreNodes(taskCodeStr, workflowDAG);
            List<IsolationTask> isolationTasks = isolationTaskDao.queryByTaskCodes(vo.getWorkflowInstanceId(),
                    allPreNodes.stream().map(Long::parseLong).collect(Collectors.toList()));
            // todo: Do we need to support if the sub isolation task is offline?
            if (CollectionUtils.isNotEmpty(isolationTasks)) {
                throw new ServiceException(ISOLATION_TASK_SUBMIT_ERROR_EXIST_SUB_ISOLATION_TASK);
            }
        }
    }

    private void checkSubmitIsolateTaskAuth(@NonNull User loginUser,
                                            @NonNull Project project,
                                            @NonNull ProcessDefinitionLog processDefinitionLog) {
        if (processDefinitionLog.getProjectCode() != project.getCode()) {
            throw new ServiceException(ISOLATION_TASK_SUBMIT_ERROR_WORKFLOW_INSTANCE_NOT_BELONG_TO_CURRENT_PROJECT);
        }
        projectService.checkProjectAndAuth(loginUser, project, project.getCode(), ISOLATION_TASK_SUBMIT);
    }

    private void checkOnlineIsolationTaskAuth(User loginUser, Project project, ProcessInstance processInstance) {
        if (processInstance == null) {
            throw new ServiceException(PROCESS_INSTANCE_NOT_EXIST);
        }
        ProcessDefinitionLog processDefinitionLog = processDefinitionLogDao
                .queryProcessDefinitionByCode(processInstance.getProcessDefinitionCode(),
                        processInstance.getProcessDefinitionVersion())
                .orElseThrow(() -> new ServiceException(PROCESS_DEFINE_NOT_EXIST, processInstance.getName()));
        if (processDefinitionLog.getProjectCode() != project.getCode()) {
            throw new ServiceException(ISOLATION_TASK_ONLINE_ERROR_PROCESS_NOT_BELONG_TO_PROJECT_ERROR);
        }

        projectService.checkProjectAndAuth(loginUser, project, project.getCode(), ISOLATION_TASK_ONLINE);
    }

    private void checkCancelIsolationTaskAuth(User loginUser, Project project, ProcessInstance processInstance) {
        if (processInstance == null) {
            throw new ServiceException(PROCESS_INSTANCE_NOT_EXIST);
        }
        ProcessDefinitionLog processDefinitionLog = processDefinitionLogDao
                .queryProcessDefinitionByCode(processInstance.getProcessDefinitionCode(),
                        processInstance.getProcessDefinitionVersion())
                .orElseThrow(() -> new ServiceException(PROCESS_DEFINE_NOT_EXIST, processInstance.getName()));
        if (processDefinitionLog.getProjectCode() != project.getCode()) {
            throw new ServiceException(ISOLATION_TASK_ONLINE_ERROR_PROCESS_NOT_BELONG_TO_PROJECT_ERROR);
        }
        projectService.checkProjectAndAuth(loginUser, project, project.getCode(), ISOLATION_TASK_CANCEL);

    }

}
