package org.apache.dolphinscheduler.api.checker;

import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.repository.ProcessDefinitionLogDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.CLEAN_TASK_INSTANCE_STATE;
import static org.apache.dolphinscheduler.api.enums.Status.CLEAN_TASK_INSTANCE_ERROR_WORKFLOW_INSTANCE_IS_RUNNING;
import static org.apache.dolphinscheduler.api.enums.Status.PROCESS_DEFINE_NOT_EXIST;
import static org.apache.dolphinscheduler.api.enums.Status.PROCESS_INSTANCE_NOT_EXIST;

@Component
public class WorkflowInstanceCheckerImpl implements WorkflowInstanceChecker {

    @Autowired
    private ProcessDefinitionLogDao processDefinitionLogDao;

    @Autowired
    private ProjectService projectService;

    @Override
    public void checkCanCleanTaskInstanceState(User loginUser, ProcessInstance processInstance) {
        if (processInstance == null) {
            throw new ServiceException(PROCESS_INSTANCE_NOT_EXIST);
        }
        ProcessDefinitionLog processDefinitionLog =
                processDefinitionLogDao
                        .queryProcessDefinitionByCode(processInstance.getProcessDefinitionCode(),
                                processInstance.getProcessDefinitionVersion())
                        .orElseThrow(() -> new ServiceException(PROCESS_DEFINE_NOT_EXIST,
                                processInstance.getProcessDefinitionCode()));
        Project project = projectService.queryByCode(loginUser, processDefinitionLog.getProjectCode());
        projectService.checkProjectAndAuth(loginUser, project, project.getCode(), CLEAN_TASK_INSTANCE_STATE);

        // check state
        if (!processInstance.getState().typeIsFinished()) {
            throw new ServiceException(CLEAN_TASK_INSTANCE_ERROR_WORKFLOW_INSTANCE_IS_RUNNING);
        }
    }
}
