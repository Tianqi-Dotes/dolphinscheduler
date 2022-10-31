package org.apache.dolphinscheduler.dao.repository;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.NonNull;
import org.apache.dolphinscheduler.dao.dto.CoronationTaskDTO;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;

import java.util.List;
import java.util.Optional;

public interface CoronationTaskDao {

    IPage<CoronationTask> pageQueryCoronationTask(String workflowInstanceName,
                                                  String taskName,
                                                  @NonNull Integer pageNo,
                                                  @NonNull Integer pageSize);

    List<CoronationTaskDTO> queryAllCoronationTasks();

    Optional<CoronationTask> queryCoronationTaskById(long id);

    int deleteById(long id);

    void batchInsert(List<CoronationTask> coronationTasks);

    int queryAllCoronationTaskNumber();

    int deleteByWorkflowInstanceId(Integer processInstanceId);
}
