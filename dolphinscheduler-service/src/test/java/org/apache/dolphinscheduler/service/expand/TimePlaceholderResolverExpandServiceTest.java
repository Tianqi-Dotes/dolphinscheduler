/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.service.expand;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TimePlaceholderResolverExpandServiceTest {

    @Mock
    private TimePlaceholderResolverExpandService timePlaceholderResolverExpandService;

    @InjectMocks
    private TimePlaceholderResolverExpandServiceImpl timePlaceholderResolverExpandServiceImpl;

    private static final String placeHolderName = "$[yyyy-MM-dd-1]";

    @Test
    public void testTimePlaceholderResolverExpandService() {
        FunctionExpandContent functionExpandContent = new FunctionExpandContent(true, null, 1, "", placeHolderName, null);

        boolean checkResult = timePlaceholderResolverExpandService.timeFunctionNeedExpand(placeHolderName);
        Assert.assertFalse(checkResult);
        String resultString = timePlaceholderResolverExpandService.timeFunctionExtension(functionExpandContent);
        Assert.assertTrue(StringUtils.isEmpty(resultString));

        boolean implCheckResult = timePlaceholderResolverExpandServiceImpl.timeFunctionNeedExpand(placeHolderName);
        Assert.assertFalse(implCheckResult);
        String implResultString =
                timePlaceholderResolverExpandServiceImpl.timeFunctionExtension(functionExpandContent);
        Assert.assertTrue(StringUtils.isEmpty(implResultString));
    }
}
