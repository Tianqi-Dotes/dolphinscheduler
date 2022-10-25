package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface IsolationTaskMapper extends BaseMapper<IsolationTask> {

    IPage<IsolationTask> pageQuery(@Param("workflowInstanceName") String workflowInstanceName,
                                   @Param("taskName") String taskName,
                                   IPage<IsolationTask> page);

    List<IsolationTask> queryByTaskCodes(@NonNull @Param("workflowInstanceId") Integer workflowInstanceId,
                                         @NonNull @Param("taskCodes") List<Long> taskCodes);

    void updateIsolationTaskStatus(@Param("id") long isolationTaskId,
                                   @Param("status") int status);

    int deleteByIdAndStatus(@Param("id") long isolationTaskId,
                            @Param("status") int status);

    List<IsolationTask> queryByWorkflowInstanceId(@Param("workflowInstanceId") Integer workflowInstanceId,
                                                  @Param("status") int status);

    void batchInsert(@Param("isolationTasks") List<IsolationTask> isolationTasks);

    List<IsolationTask> queryByStatus(@Param("status") int code);
}
