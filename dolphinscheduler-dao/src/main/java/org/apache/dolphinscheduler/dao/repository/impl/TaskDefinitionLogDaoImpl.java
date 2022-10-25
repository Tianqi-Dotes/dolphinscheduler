package org.apache.dolphinscheduler.dao.repository.impl;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelation;
import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelationLog;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionLogMapper;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionLogDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class TaskDefinitionLogDaoImpl implements TaskDefinitionLogDao {

    @Autowired
    private TaskDefinitionLogMapper taskDefinitionLogMapper;

    @Override
    public List<TaskDefinitionLog> queryTaskDefinitionByRelations(List<ProcessTaskRelationLog> processTaskRelations) {
        List<TaskDefinitionLog> taskDefinitionLogs = new ArrayList<>();
        if (CollectionUtils.isEmpty(processTaskRelations)) {
            return taskDefinitionLogs;
        }
        Map<Long, Integer> taskCodeVersionMap = new HashMap<>();
        for (ProcessTaskRelation processTaskRelation : processTaskRelations) {
            if (processTaskRelation.getPreTaskCode() > 0) {
                taskCodeVersionMap.put(processTaskRelation.getPreTaskCode(), processTaskRelation.getPreTaskVersion());
            }
            if (processTaskRelation.getPostTaskCode() > 0) {
                taskCodeVersionMap.put(processTaskRelation.getPostTaskCode(), processTaskRelation.getPostTaskVersion());
            }
        }

        taskCodeVersionMap.forEach((code, version) -> {
            taskDefinitionLogs.add(taskDefinitionLogMapper.queryByDefinitionCodeAndVersion(code, version));
        });
        return taskDefinitionLogs;
    }
}
