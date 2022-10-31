package org.apache.dolphinscheduler.api.controller;

import io.swagger.annotations.Api;
import org.apache.dolphinscheduler.api.aspect.AccessLogAnnotation;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskListingRequest;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskParseRequest;
import org.apache.dolphinscheduler.api.dto.request.CoronationTaskSubmitRequest;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.CoronationTaskService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.api.vo.CoronationTaskExcelImportVO;
import org.apache.dolphinscheduler.api.vo.CoronationTaskParseVO;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.dto.CoronationTaskDTO;
import org.apache.dolphinscheduler.dao.entity.CoronationTask;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.util.List;

import static org.apache.dolphinscheduler.api.enums.Status.CORONATION_TASK_CANCEL_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.CORONATION_TASK_DELETE_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.CORONATION_TASK_LISTING_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.CORONATION_TASK_ONLINE_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.CORONATION_TASK_PARSE_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.CORONATION_TASK_SUBMIT_ERROR;

@Api(tags = "ISOLATION_TASK_TAG")
@RestController
@RequestMapping("/projects/{projectCode}/coronation-task")
public class CoronationTaskController {

    @Autowired
    private CoronationTaskService coronationTaskService;

    @PostMapping(value = "/parse")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(CORONATION_TASK_PARSE_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<List<CoronationTaskParseVO>> parseCoronationTask(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                                   @PathVariable("projectCode") long projectCode,
                                                                   @RequestBody CoronationTaskParseRequest request) {
        return Result.success(coronationTaskService.parseCoronationTask(loginUser, projectCode, request));
    }

    @PostMapping(value = "/submit")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(CORONATION_TASK_SUBMIT_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<Void> submitCoronationTask(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                             @PathVariable("projectCode") long projectCode,
                                             @RequestBody CoronationTaskSubmitRequest request) {
        coronationTaskService.submitCoronationTask(loginUser, projectCode, request);
        return Result.success(null);
    }

    @PutMapping(value = "/cancel/{id}")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(CORONATION_TASK_CANCEL_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<Void> cancelCoronationTask(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                             @PathVariable("projectCode") long projectCode,
                                             @PathVariable("id") long id) {
        coronationTaskService.cancelCoronationTask(loginUser, projectCode, id);
        return Result.success(null);
    }

    @GetMapping("")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(CORONATION_TASK_LISTING_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<PageInfo<CoronationTaskDTO>> listingCoronationTasks(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                                      @PathVariable("projectCode") long projectCode,
                                                                      @RequestParam(required = false) String workflowInstanceName,
                                                                      @RequestParam(required = false) String taskName,
                                                                      @RequestParam Integer pageNo,
                                                                      @RequestParam Integer pageSize) {
        CoronationTaskListingRequest request = CoronationTaskListingRequest.builder()
                .workflowInstanceName(workflowInstanceName)
                .taskName(taskName)
                .pageNo(pageNo)
                .pageSize(pageSize)
                .build();
        return Result.success(coronationTaskService.listingCoronationTasks(loginUser, projectCode, request));

    }

}
