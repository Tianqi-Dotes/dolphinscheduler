package org.apache.dolphinscheduler.dao.repository.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.exception.RepositoryException;
import org.apache.dolphinscheduler.dao.mapper.TaskInstanceMapper;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;

@Slf4j
@Repository
public class TaskInstanceDaoImpl implements TaskInstanceDao {

    @Autowired
    private TaskInstanceMapper taskInstanceMapper;

    @Override
    public List<TaskInstance> queryTaskInstanceByIds(List<Integer> taskInstanceIds) {
        if (CollectionUtils.isEmpty(taskInstanceIds)) {
            return Collections.emptyList();
        }
        return taskInstanceMapper.selectBatchIds(taskInstanceIds);
    }

    @Override
    public List<TaskInstance> queryValidatedTaskInstanceByWorkflowInstanceId(Integer workflowInstanceId) {
        return taskInstanceMapper.findValidTaskListByProcessId(workflowInstanceId, Flag.YES);
    }

    @Override
    public void updateTaskInstance(@NonNull TaskInstance taskInstance) throws RepositoryException {
        try {
            int updateCount = taskInstanceMapper.updateById(taskInstance);
            if (updateCount <= 0) {
                throw new RepositoryException("Update taskInstance error, updateCount is " + updateCount);
            }
        } catch (RepositoryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RepositoryException("Update taskInstance error, get a unknown exception", ex);
        }
    }

    @Override
    public boolean updateTaskInstanceSafely(@NonNull TaskInstance taskInstance) {
        try {
            updateTaskInstance(taskInstance);
            return true;
        } catch (RepositoryException e) {
            log.error("Update task instance failed, get a exception will return false", e);
            return false;
        }
    }
}
