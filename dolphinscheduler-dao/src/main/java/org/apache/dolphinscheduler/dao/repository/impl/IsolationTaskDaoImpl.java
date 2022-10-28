package org.apache.dolphinscheduler.dao.repository.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.dolphinscheduler.dao.mapper.IsolationTaskMapper;
import org.apache.dolphinscheduler.dao.repository.IsolationTaskDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
@Repository
public class IsolationTaskDaoImpl implements IsolationTaskDao {

    @Autowired
    private IsolationTaskMapper isolationTaskMapper;

    @Override
    public IPage<IsolationTask> pageQueryIsolationTask(String workflowInstanceName,
                                                       String taskName,
                                                       int pageNumber,
                                                       int pageSize) {
        Page<IsolationTask> page = new Page<>(pageNumber, pageSize);
        return isolationTaskMapper.pageQuery(page, workflowInstanceName, taskName);
    }

    @Override
    public List<IsolationTask> queryAllIsolationTask() {
        return isolationTaskMapper.queryAllIsolationTask();
    }

    @Override
    public List<IsolationTask> queryByTaskCodes(Integer workflowInstanceId, List<Long> taskCodes) {
        if (CollectionUtils.isEmpty(taskCodes)) {
            return Collections.emptyList();
        }
        return isolationTaskMapper.queryByTaskCodes(workflowInstanceId, taskCodes);
    }

    @Override
    public List<IsolationTask> queryByWorkflowInstanceId(Integer workflowInstanceId) {
        return isolationTaskMapper.queryByWorkflowInstanceId(workflowInstanceId);
    }

    @Override
    public Optional<IsolationTask> queryById(long isolationTaskId) {
        return Optional.ofNullable(isolationTaskMapper.selectById(isolationTaskId));
    }

    @Override
    public int deleteById(long id) {
        return isolationTaskMapper.deleteById(id);
    }

    @Override
    public void insert(IsolationTask isolationTask) {
        isolationTaskMapper.insert(isolationTask);
    }

    @Override
    public void batchInsert(List<IsolationTask> isolationTasks) {
        if (CollectionUtils.isEmpty(isolationTasks)) {
            return;
        }
        isolationTaskMapper.batchInsert(isolationTasks);
    }
}
