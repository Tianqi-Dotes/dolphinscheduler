package org.apache.dolphinscheduler.api.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IsolationTaskListingVO {

    private Long id;

    private int workflowInstanceId;

    private String workflowInstanceName;

    private String taskName;

    private long taskCode;

    private String taskStatus;

    private Date createTime;

    private Date updateTime;
}
