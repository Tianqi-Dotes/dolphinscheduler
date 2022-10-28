package org.apache.dolphinscheduler.api.checker;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.CORONATION_TASK_CANCEL;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.CORONATION_TASK_PARSE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.CORONATION_TASK_VIEW;
import static org.apache.dolphinscheduler.api.enums.Status.PROCESS_DEFINE_NOT_EXIST;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.vo.CoronationTaskExcelImportVO;
import org.apache.dolphinscheduler.api.vo.CoronationTaskParseVO;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.repository.ProcessDefinitionDao;

import java.util.List;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CoronationTaskCheckerImpl implements CoronationTaskChecker {

    @Autowired
    private ProcessDefinitionDao processDefinitionDao;

    @Autowired
    private ProjectService projectService;

    @Override
    public void checkCanParseCoronationTask(@NonNull User loginUser,
                                            @NonNull Project project,
                                            @NonNull ProcessInstance processInstance,
                                            @NonNull CoronationTaskExcelImportVO vo) throws ServiceException {
        ProcessDefinition processDefinition =
                processDefinitionDao.queryProcessDefinitionByCode(processInstance.getProcessDefinitionCode())
                        .orElseThrow(() -> new ServiceException(PROCESS_DEFINE_NOT_EXIST,
                                processInstance.getProcessDefinitionCode()));
        if (processDefinition.getProjectCode() != project.getCode()) {
            throw new ServiceException(Status.PROCESS_INSTANCE_NOT_BELONG_TO_CURRENT_PROJECT);
        }
        projectService.checkProjectAndAuth(loginUser, project, project.getCode(), CORONATION_TASK_PARSE);
    }

    @Override
    public void checkCanSubmitTaskCoronation(@NonNull User loginUser,
                                             @NonNull Project project,
                                             @NonNull ProcessInstance processInstance,
                                             @NonNull DAG<String, TaskNode, TaskNodeRelation> workflowDAG,
                                             @NonNull List<CoronationTaskParseVO> voList) throws ServiceException {
        ProcessDefinition processDefinition =
                processDefinitionDao.queryProcessDefinitionByCode(processInstance.getProcessDefinitionCode())
                        .orElseThrow(() -> new ServiceException(PROCESS_DEFINE_NOT_EXIST,
                                processInstance.getProcessDefinitionCode()));

        if (processDefinition.getProjectCode() != project.getCode()) {
            throw new ServiceException(Status.PROCESS_INSTANCE_NOT_BELONG_TO_CURRENT_PROJECT);
        }
        projectService.checkProjectAndAuth(loginUser, project, project.getCode(), CORONATION_TASK_PARSE);
        // todo: check if the vos can submit coronation tasks
        // if there already exist coronaton task in db or the upstream task contains coronation task, we cannot submit

    }

    @Override
    public void checkCanListingTaskCoronation(@NonNull User loginUser,
                                              long projectCode) {
        Project project = projectService.queryByCode(loginUser, projectCode);
        projectService.checkProjectAndAuth(loginUser, project, projectCode, CORONATION_TASK_VIEW);
    }

    @Override
    public void checkCanCancelTaskCoronation(@NonNull User loginUser, long projectCode,
                                             @NonNull ProcessInstance processInstance,
                                             @NonNull CoronationTask coronationTask) {
        ProcessDefinition processDefinition =
                processDefinitionDao.queryProcessDefinitionByCode(processInstance.getProcessDefinitionCode())
                        .orElseThrow(() -> new ServiceException(PROCESS_DEFINE_NOT_EXIST,
                                processInstance.getProcessDefinitionCode()));

        if (processDefinition.getProjectCode() != projectCode) {
            throw new ServiceException(Status.PROCESS_INSTANCE_NOT_BELONG_TO_CURRENT_PROJECT);
        }

        Project project = projectService.queryByCode(loginUser, projectCode);
        projectService.checkProjectAndAuth(loginUser, project, projectCode, CORONATION_TASK_CANCEL);
    }
}
