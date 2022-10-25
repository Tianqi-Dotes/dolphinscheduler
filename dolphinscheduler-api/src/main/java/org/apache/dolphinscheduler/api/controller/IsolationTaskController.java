package org.apache.dolphinscheduler.api.controller;

import io.swagger.annotations.Api;
import org.apache.dolphinscheduler.api.aspect.AccessLogAnnotation;
import org.apache.dolphinscheduler.api.dto.request.IsolationTaskListingRequest;
import org.apache.dolphinscheduler.api.dto.request.IsolationTaskSubmitRequest;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.IsolationTaskService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.IsolationTask;
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
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import javax.validation.Valid;

import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_DELETE_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_LISTING_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.ISOLATION_TASK_SUBMIT_ERROR;

@Api(tags = "ISOLATION_TASK_TAG")
@RestController
@RequestMapping("/projects/{projectCode}/isolation-task")
public class IsolationTaskController {

    @Autowired
    private IsolationTaskService isolationTaskService;

    @PostMapping(value = "/submit")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(ISOLATION_TASK_SUBMIT_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<Void> submitIsolationTask(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                            @PathVariable long projectCode,
                                            @RequestBody IsolationTaskSubmitRequest isolationTaskSubmitRequest) {
        isolationTaskService.submitTaskIsolations(loginUser, projectCode, isolationTaskSubmitRequest);
        return Result.success(null);
    }

    @PutMapping(value = "/online/{id}")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(ISOLATION_TASK_SUBMIT_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<Void> onlineIsolationTask(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                            @PathVariable long projectCode,
                                            @PathVariable(name = "id") long isolationId) {
        isolationTaskService.onlineTaskIsolation(loginUser, projectCode, isolationId);
        return Result.success(null);
    }

    @PutMapping(value = "/cancel/{id}")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(ISOLATION_TASK_SUBMIT_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<Void> cancelIsolationTask(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                            @PathVariable long projectCode,
                                            @PathVariable(name = "id") long isolationId) {
        isolationTaskService.cancelTaskIsolation(loginUser, projectCode, isolationId);
        return Result.success(null);
    }

    @GetMapping("")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(ISOLATION_TASK_LISTING_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<PageInfo<IsolationTask>> listingIsolationTask(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                                @PathVariable long projectCode,
                                                                @RequestBody @Valid IsolationTaskListingRequest request) {
        return Result.success(isolationTaskService.listingTaskIsolation(loginUser, projectCode, request));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(ISOLATION_TASK_DELETE_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result<Void> deleteIsolationTask(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                            @PathVariable long projectCode,
                                            @PathVariable long id) {
        isolationTaskService.deleteTaskIsolation(loginUser, projectCode, id);
        return Result.success(null);
    }

}
