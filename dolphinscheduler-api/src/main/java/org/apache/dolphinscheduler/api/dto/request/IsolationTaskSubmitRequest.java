package org.apache.dolphinscheduler.api.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.api.vo.IsolationTaskExcelParseVO;

import javax.validation.constraints.NotEmpty;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IsolationTaskSubmitRequest {

    @NotEmpty
    private List<IsolationTaskExcelParseVO> isolationTaskExcelParseVOList;
}
