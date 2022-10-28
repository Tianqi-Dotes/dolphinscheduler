package org.apache.dolphinscheduler.dao.repository;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;

import java.util.List;
import java.util.Optional;

public interface IsolationTaskDao {

    /**
     * Page query, pageNumber is start from 0.
     */
    IPage<IsolationTask> pageQueryIsolationTask(String workflowInstanceName,
                                                String taskName,
                                                int pageNumber,
                                                int pageSize);

    List<IsolationTask> queryAllIsolationTask();

    List<IsolationTask> queryByTaskCodes(Integer workflowInstanceId, List<Long> taskCodes);

    List<IsolationTask> queryByWorkflowInstanceId(Integer workflowInstanceId);

    Optional<IsolationTask> queryById(long isolationTaskId);

    int deleteById(long id);

    void insert(IsolationTask isolationTaskDTO);

    void batchInsert(List<IsolationTask> isolationTasks);
}
