package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_ds_coronation_task")
public class CoronationTask {

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private long projectCode;

    private int workflowInstanceId;

    private String workflowInstanceName;

    private String taskName;

    private long taskCode;

    private String forbiddenUpstreamTasks;

    private Date createTime;

    private Date updateTime;

}
