package org.apache.dolphinscheduler.api.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.api.vo.CoronationTaskExcelImportVO;

import javax.validation.constraints.NotEmpty;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoronationTaskParseRequest {

    @NotEmpty
    private List<CoronationTaskExcelImportVO> coronationTasks;
}
