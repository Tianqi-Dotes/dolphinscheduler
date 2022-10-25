package org.apache.dolphinscheduler.api.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IsolationTaskListingRequest {

    private String workflowInstanceName;

    private String taskName;

    @NotNull(message = "page number cannot be null")
    private Integer pageNo;

    @NotNull(message = "page size cannot be null")
    private Integer pageSize;
}
