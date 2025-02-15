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

import {NDataTable, NPagination, NSpace} from 'naive-ui'
import {defineComponent, onMounted, toRefs, watch} from 'vue'
import { useI18n } from 'vue-i18n'
import { useTable } from '../definition/timing/use-table'
import Card from '@/components/card'
import TimingModal from '../definition/components/timing-modal'
import TimingCondition from '@/views/projects/workflow/timing/components/timing-condition'
import { ITimingSearch } from '@/views/projects/workflow/timing/types'
import DependenciesModal from "@/views/projects/components/dependencies/dependencies-modal";

export default defineComponent({
  name: 'WorkflowTimingList',
  setup() {
    const { variables, createColumns, getTableData } = useTable()

    const requestData = () => {
      getTableData({
        pageSize: variables.pageSize,
        pageNo: variables.page,
        searchVal: variables.searchVal,
        projectCode: variables.projectCode,
        processDefinitionCode: variables.processDefinitionCode
      })
    }

    const handleUpdateList = () => {
      requestData()
    }

    const handleSearch = (params: ITimingSearch) => {
      variables.processDefinitionCode = params.processDefinitionCode
      variables.page = 1
      requestData()
    }

    const handleChangePageSize = () => {
      variables.page = 1
      requestData()
    }

    onMounted(() => {
      createColumns(variables)
      requestData()
    })

    watch(useI18n().locale, () => {
      createColumns(variables)
    })

    return {
      requestData,
      handleSearch,
      handleUpdateList,
      handleChangePageSize,
      ...toRefs(variables)
    }
  },
  render() {
    const { t } = useI18n()
    const { loadingRef } = this

    return (
      <NSpace vertical>
        <Card>
          <TimingCondition onHandleSearch={this.handleSearch} />
        </Card>
        <Card title={t('project.workflow.cron_manage')}>
          <NSpace vertical>
            <NDataTable
              loading={loadingRef}
              columns={this.columns}
              data={this.tableData}
              striped
              size={'small'}
              scrollX={this.tableWidth}
            />
            <NSpace justify='center'>
              <NPagination
                v-model:page={this.page}
                v-model:page-size={this.pageSize}
                page-count={this.totalPage}
                show-size-picker
                page-sizes={[10, 30, 50]}
                show-quick-jumper
                onUpdatePage={this.requestData}
                onUpdatePageSize={this.handleChangePageSize}
              />
            </NSpace>
          </NSpace>
        </Card>
        <TimingModal
          type={'update'}
          v-model:row={this.row}
          v-model:show={this.showRef}
          onUpdateList={this.handleUpdateList}
        />
        <DependenciesModal
            v-model:show={this.dependenciesData.showRef}
            v-model:taskLinks={this.dependenciesData.taskLinks}
            required={this.dependenciesData.required}
            content={this.dependenciesData.tip}
            onConfirm={this.dependenciesData.action}
        />
      </NSpace>
    )
  }
})
