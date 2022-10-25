package org.apache.dolphinscheduler.dao.repository;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.NonNull;
import org.apache.dolphinscheduler.dao.dto.IsolationTaskStatus;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface IsolationTaskDao {

    /**
     * Page query, pageNumber is start from 0.
     */
    IPage<IsolationTask> pageQueryIsolationTask(String workflowInstanceName,
                                                String taskName,
                                                int pageNumber,
                                                int pageSize);

    List<IsolationTask> queryByTaskCodes(Integer workflowInstanceId, List<Long> taskCodes);

    List<IsolationTask> queryByWorkflowInstanceId(Integer workflowInstanceId, IsolationTaskStatus isolationTaskStatus);

    Optional<IsolationTask> queryById(long isolationTaskId);

    List<IsolationTask> queryByIds(List<Long> isolationTaskIds);

    List<IsolationTask> queryByStatus(@NonNull IsolationTaskStatus isolationTaskStatus);

    int deleteByIdAndStatus(long id, IsolationTaskStatus status);

    void insert(IsolationTask isolationTaskDTO);

    void updateIsolationTaskStatus(long isolationTaskId, IsolationTaskStatus isolationTaskStatus);

    void batchInsert(List<IsolationTask> isolationTasks);
}
