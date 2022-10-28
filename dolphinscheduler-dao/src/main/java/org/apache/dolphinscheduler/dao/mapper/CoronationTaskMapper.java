package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface CoronationTaskMapper extends BaseMapper<CoronationTask> {

    IPage<CoronationTask> pageQueryCoronationTask(Page<IsolationTask> page,
                                                  @Param("workflowInstanceName") String workflowInstanceName,
                                                  @Param("taskName") String taskName);

    void batchInsert(@Param("coronationTasks") List<CoronationTask> coronationTasks);

    List<CoronationTask> queryAllCoronationTasks();

    int queryAllCoronationTaskNumber();

}
