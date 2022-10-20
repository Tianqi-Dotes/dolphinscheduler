package org.apache.dolphinscheduler.api.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CleanTaskInstanceStateRequest {

    private List<Integer> taskInstanceIds;
}
