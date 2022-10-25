package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelationLog;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;

import java.util.List;

public interface TaskDefinitionLogDao {

    List<TaskDefinitionLog> queryTaskDefinitionByRelations(List<ProcessTaskRelationLog> processTaskRelations);

}
