package org.apache.dolphinscheduler.dao.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CleanTaskInstanceStateCommandParamDTO {

    private List<Integer> cleanStateTaskInstanceIds;
}
