package org.apache.dolphinscheduler.dao.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TaskRuntimeContext {

    private boolean hasBeenIsolated;
    private boolean isIsolationTask;

    private boolean hasBeenCoronatted;
    private boolean isCoronationTask;

}
