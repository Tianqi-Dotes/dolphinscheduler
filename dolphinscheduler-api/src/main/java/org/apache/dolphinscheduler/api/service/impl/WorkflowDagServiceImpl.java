package org.apache.dolphinscheduler.api.service.impl;

import lombok.NonNull;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.WorkflowDAGService;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.common.process.ProcessDag;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelation;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.repository.ProcessDefinitionLogDao;
import org.apache.dolphinscheduler.dao.utils.DagHelper;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

import static org.apache.dolphinscheduler.api.enums.Status.PROCESS_DEFINE_NOT_EXIST;

@Service
public class WorkflowDagServiceImpl implements WorkflowDAGService {

    @Autowired
    private ProcessDefinitionLogDao processDefinitionLogDao;

    // todo: use dao
    @Autowired
    private ProcessService processService;

    @Override
    public DAG<String, TaskNode, TaskNodeRelation> getWorkflowDAG(@NonNull Long processDefinitionCode,
                                                                  @NonNull Integer processDefinitionVersion) {
        ProcessDefinitionLog processDefinitionLog =
                processDefinitionLogDao.queryProcessDefinitionByCode(processDefinitionCode, processDefinitionVersion)
                        .orElseThrow(() -> new ServiceException(PROCESS_DEFINE_NOT_EXIST, processDefinitionCode));
        return getWorkflowDAG(processDefinitionLog);
    }

    @Override
    public DAG<String, TaskNode, TaskNodeRelation> getWorkflowDAG(@NonNull ProcessDefinitionLog processDefinitionLog) {
        List<ProcessTaskRelation> processTaskRelations =
                processService.findRelationByCode(processDefinitionLog.getCode(), processDefinitionLog.getVersion());
        List<TaskDefinitionLog> taskDefinitionLogs =
                processService.getTaskDefineLogListByRelation(processTaskRelations);
        List<TaskNode> taskNodeList = processService.transformTask(processTaskRelations, taskDefinitionLogs);

        ProcessDag processDag = DagHelper.generateFlowDag(
                taskNodeList,
                Collections.emptyList(),
                Collections.emptyList(),
                TaskDependType.TASK_POST);
        if (processDag == null) {
            throw new ServiceException(Status.PROCESS_DAG_IS_EMPTY);
        }

        return DagHelper.buildDagGraph(processDag);
    }
}
