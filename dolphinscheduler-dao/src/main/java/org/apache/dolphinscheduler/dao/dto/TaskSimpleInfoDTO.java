package org.apache.dolphinscheduler.dao.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskSimpleInfoDTO {

    private String taskNode;
    private long taskCode;
}
