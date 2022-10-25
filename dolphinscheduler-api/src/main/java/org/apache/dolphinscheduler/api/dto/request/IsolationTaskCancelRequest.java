package org.apache.dolphinscheduler.api.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotEmpty;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IsolationTaskCancelRequest {

    @NotEmpty
    private List<Long> isolationIds;
}
