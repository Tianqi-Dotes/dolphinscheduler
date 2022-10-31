package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface IsolationTaskMapper extends BaseMapper<IsolationTask> {

    IPage<IsolationTask> pageQuery(IPage<IsolationTask> page,
                                   @Param("workflowInstanceName") String workflowInstanceName,
                                   @Param("taskName") String taskName);

    List<IsolationTask> queryByTaskCodes(@NonNull @Param("workflowInstanceId") Integer workflowInstanceId,
                                         @NonNull @Param("taskCodes") List<Long> taskCodes);

    List<IsolationTask> queryByWorkflowInstanceId(@Param("workflowInstanceId") Integer workflowInstanceId);

    void batchInsert(@Param("isolationTasks") List<IsolationTask> isolationTasks);

    List<IsolationTask> queryAllIsolationTask();

    int deleteByWorkflowInstanceId(@Param("workflowInstanceId") Integer workflowInstanceId);
}
