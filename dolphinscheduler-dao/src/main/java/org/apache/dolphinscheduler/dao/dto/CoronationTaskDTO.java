package org.apache.dolphinscheduler.dao.dto;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

import java.util.Date;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoronationTaskDTO {

    private Long id;

    private int workflowInstanceId;

    private String workflowInstanceName;

    private String taskName;

    private long taskCode;

    private ExecutionStatus taskStatus;

    private List<TaskSimpleInfoDTO> forbiddenUpstreamTasks;

    private Date createTime;

    private Date updateTime;

    public CoronationTaskDTO(@NonNull CoronationTask coronationTask) {
        this.id = coronationTask.getId();
        this.workflowInstanceId = coronationTask.getWorkflowInstanceId();
        this.workflowInstanceName = coronationTask.getWorkflowInstanceName();
        this.taskName = coronationTask.getTaskName();
        this.taskCode = coronationTask.getTaskCode();
        this.forbiddenUpstreamTasks = JSONUtils.parseObject(coronationTask.getForbiddenUpstreamTasks(),
                new TypeReference<List<TaskSimpleInfoDTO>>() {
                });
        this.createTime = coronationTask.getCreateTime();
        this.updateTime = coronationTask.getUpdateTime();
    }

}
