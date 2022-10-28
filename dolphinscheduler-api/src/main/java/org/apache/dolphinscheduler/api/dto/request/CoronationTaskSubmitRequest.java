package org.apache.dolphinscheduler.api.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.api.vo.CoronationTaskParseVO;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CoronationTaskSubmitRequest {

    private List<CoronationTaskParseVO> CoronationTasks;
}
