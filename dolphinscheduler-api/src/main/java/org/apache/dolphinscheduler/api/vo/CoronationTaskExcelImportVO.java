package org.apache.dolphinscheduler.api.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoronationTaskExcelImportVO {

    @NotNull(message = "Workflow instance id cannot be null")
    private Integer workflowInstanceId;

    @NotNull(message = "Workflow instance name cannot be null")
    private String workflowInstanceName;

    @NotNull(message = "Task code cannot be null")
    private Long taskCode;

    @NotNull(message = "Task name cannot be null")
    private String taskName;
}
