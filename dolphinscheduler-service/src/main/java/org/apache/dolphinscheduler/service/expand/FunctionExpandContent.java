package org.apache.dolphinscheduler.service.expand;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
public class FunctionExpandContent {

    private boolean global;

    private String parameters;

    private Integer processInstanceId;

    private String timezone;

    private String placeholderName;

    public FunctionExpandContent(boolean global, String parameters, Integer processInstanceId,
                                 String timezone, String placeholderName) {
        this.global = global;
        this.parameters = parameters;
        this.processInstanceId = processInstanceId;
        this.timezone = timezone;
        this.placeholderName = placeholderName;
    }
}
