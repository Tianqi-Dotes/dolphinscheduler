package org.apache.dolphinscheduler.dao.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskSimpleInfoDTO {

    private String taskName;
    private long taskCode;
}
