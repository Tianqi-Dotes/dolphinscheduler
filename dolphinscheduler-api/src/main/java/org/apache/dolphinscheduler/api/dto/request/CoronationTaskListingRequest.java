package org.apache.dolphinscheduler.api.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CoronationTaskListingRequest {

    private String workflowInstanceName;

    private String taskName;

    @NotNull(message = "pageNo cannot be null")
    private Integer pageNo;

    @NotNull(message = "pageSize cannot be null")
    private Integer pageSize;

}
