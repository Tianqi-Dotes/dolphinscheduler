package org.apache.dolphinscheduler.api.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotEmpty;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IsolationTaskDeleteRequest {

    @NotEmpty
    private List<Long> isolationIds;
}
