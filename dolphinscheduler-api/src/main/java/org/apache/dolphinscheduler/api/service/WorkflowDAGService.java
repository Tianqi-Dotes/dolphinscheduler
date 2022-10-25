package org.apache.dolphinscheduler.api.service;

import lombok.NonNull;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionLog;

public interface WorkflowDAGService {

    DAG<String, TaskNode, TaskNodeRelation> getWorkflowDAG(@NonNull Long processDefinitionCode,
                                                           @NonNull Integer processDefinitionVersion);

    DAG<String, TaskNode, TaskNodeRelation> getWorkflowDAG(@NonNull ProcessDefinitionLog processDefinitionLog);
}
