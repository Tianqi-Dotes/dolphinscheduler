package org.apache.dolphinscheduler.api.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.dao.dto.TaskSimpleInfoDTO;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoronationTaskParseVO {

    @NotNull(message = "workflow instance cannot be null")
    private String workflowInstanceName;

    @NotNull(message = "workflow instance id cannot be null")
    private Integer workflowInstanceId;

    @NotNull(message = "taskNode cannot be null")
    private String taskNode;

    @NotNull(message = "taskCode cannot be null")
    private Long taskCode;

    @NotEmpty(message = "upstream task nodes cannot be null")
    private List<TaskSimpleInfoDTO> upstreamTasks;

}
