package org.apache.dolphinscheduler.dao.repository.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.dao.dto.CoronationTaskDTO;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.dolphinscheduler.dao.mapper.CoronationTaskMapper;
import org.apache.dolphinscheduler.dao.repository.CoronationTaskDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class CoronationTaskDaoImpl implements CoronationTaskDao {

    @Autowired
    private CoronationTaskMapper coronationTaskMapper;

    @Override
    public IPage<CoronationTask> pageQueryCoronationTask(String workflowInstanceName, String taskName,
                                                         @NonNull Integer pageNo,
                                                         @NonNull Integer pageSize) {
        Page<IsolationTask> page = new Page<>(pageNo, pageSize);
        return coronationTaskMapper.pageQueryCoronationTask(page, workflowInstanceName, taskName);
    }

    @Override
    public List<CoronationTaskDTO> queryAllCoronationTasks() {
        return coronationTaskMapper.queryAllCoronationTasks()
                .stream()
                .map(CoronationTaskDTO::new)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<CoronationTask> queryCoronationTaskById(long id) {
        return Optional.ofNullable(coronationTaskMapper.selectById(id));
    }

    @Override
    public int deleteById(long id) {
        return coronationTaskMapper.deleteById(id);
    }

    @Override
    public void batchInsert(List<CoronationTask> coronationTasks) {
        if (CollectionUtils.isEmpty(coronationTasks)) {
            return;
        }
        coronationTaskMapper.batchInsert(coronationTasks);
    }

    @Override
    public int queryAllCoronationTaskNumber() {
        return coronationTaskMapper.queryAllCoronationTaskNumber();
    }

    @Override
    public int deleteByWorkflowInstanceId(Integer workflowInstanceId) {
        return coronationTaskMapper.deleteByWorkflowInstanceId(workflowInstanceId);
    }
}
