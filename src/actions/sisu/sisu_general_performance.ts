import * as Hub from "../../hub"
import axios from "axios"

const TAG = "sisu"
export class SisuAction extends Hub.Action {

  name = "sisu"
  label = "Create a general performance kda."
  iconName = "sisu/sisu_logo.svg"
  description = "Send data to Sisu and create a general performance kda."
  supportedActionTypes = [Hub.ActionType.Cell, Hub.ActionType.Dashboard, Hub.ActionType.Query]
  supportedFormats = [Hub.ActionFormat.JsonDetail]
  supportedFormattings = [Hub.ActionFormatting.Formatted]
  requiredFields = [{ tag: TAG }]
  params = [
    {
      name: "sisu_api_token",
      label: "API token",
      required: false,
      sensitive: false,
    }
  ]

  async execute(request: Hub.ActionRequest) {
    // console.log('--- REQUEST ------\n', JSON.stringify(request))
    // if (request) {
    //   return new Hub.ActionResponse({ success: true })
    // }

    try {
      const tableInfo = await this.getTableInfo(request)
      const dimensions = await this.getAllDimensionsForTable(request, tableInfo)
      const sisuBaseQuery = this.buildSisuBaseQuery(request, tableInfo, dimensions)
      const baseQuery = await this.createQuery(request, sisuBaseQuery)
      const metric = await this.createMetric(request, baseQuery.base_query_id)
      await this.updateDefaultMetricDimensions(request, baseQuery.base_query_id, metric.metric_id)
      const kda = await this.createKDA(request, metric.metric_id)
      this.runKDA(request, kda.analysis_id)

      return new Hub.ActionResponse({ success: true })
    } catch (error) {
      return new Hub.ActionResponse({ success: false })
    }
  }

  private async getAllDimensionsForTable(request: Hub.ActionRequest, tableInfo: string[]): Promise<string[]> {
    const axiosConfig = this.getAxiosConfig(request)
    const connectionId = request.formParams.connection
    const tableName = request.scheduledPlan?.query?.model || request.scheduledPlan?.query?.view
    try {
      const customQueries = await axios.get(`https://dev.sisu.ai/rest/data_sources/${connectionId}/custom_queries`, axiosConfig)
      const lookerAllDimensionsCustomQuery = customQueries.data.find(({ name }: any) => name === `Looker ${tableName} all dimensions`)
      if (lookerAllDimensionsCustomQuery) {
        const allDimensionsQueryId = lookerAllDimensionsCustomQuery.base_query_id
        const dimensionsRequest = await axios.get(`https://dev.sisu.ai/rest/base_queries/${allDimensionsQueryId}/dimensions`, axiosConfig)
        return dimensionsRequest.data.map((dimension: any) => `${tableInfo[2].toLowerCase()}."${dimension.columnName}"`)
      } else {
        const queryName = `Looker ${tableName} all dimensions`
        const allDimensionsQueryString = `SELECT * FROM ${tableInfo[0]}.${tableInfo[1]}.${tableInfo[2]} LIMIT 1`
        const newAllDimensionsQuery = {
          name: queryName,
          query_string: allDimensionsQueryString
        }
        const queryRequest = await axios.post(`https://dev.sisu.ai/rest/data_sources/${connectionId}/custom_queries`, newAllDimensionsQuery, axiosConfig)
        const allDimensionsQueryId = queryRequest.data.base_query_id
        const dimensionsRequest = await axios.get(`https://dev.sisu.ai/rest/base_queries/${allDimensionsQueryId}/dimensions`, axiosConfig)
        return dimensionsRequest.data.map((dimension: any) => `${tableInfo[2].toLowerCase()}."${dimension.columnName}"`)
      }
    } catch (error) {
      console.error('------- ERROR ------', error)
      throw error
    }
  }

  private async runKDA(request: Hub.ActionRequest, analysisId: number) {
    const axiosConfig = this.getAxiosConfig(request)
    try {
      await axios.post(`https://dev.sisu.ai/rest/analyses/${analysisId}/results`, {}, axiosConfig)
    } catch (error) {
      console.error('------- ERROR ------', error)
      throw error
    }
  }

  private async createKDA(request: Hub.ActionRequest, metricId: number) {
    const axiosConfig = this.getAxiosConfig(request)
    const currentTime = new Date().toISOString()
    const kdaName = `${currentTime}_${request.scheduledPlan?.title}_kda` || `${currentTime}_kda`
    const newKDA = {
      name: kdaName
    }

    try {
      // TODO, dynamic project selection
      const kdaRequest = await axios.post(`https://dev.sisu.ai/rest/projects/951/metrics/${metricId}/analyses`, newKDA, axiosConfig)
      return kdaRequest.data
    } catch (error) {
      console.error('------- ERROR ------', error)
      throw error
    }
  }

  private async getTableInfo(request: Hub.ActionRequest) {
    const axiosConfig = this.getAxiosConfig(request)
    const connectionId = request.formParams.connection
    const tableName = request.scheduledPlan?.query?.model || request.scheduledPlan?.query?.view

    // TODO - move to a validate function using enums for params
    // and it will ahve its error messages
    if (!connectionId) {
      throw "User needs to select a Sisu connection"
    }

    if (!tableName) {
      throw "There is no table name in the data"
    }

    try {
      const tables = await axios.get(`https://dev.sisu.ai/rest/connections/${connectionId}/tables`, axiosConfig)
      const tableInfo = this.findTableInfo(tables.data.tables, tableName)
      if (!tableInfo) {
        throw "Wasn't able to find a table in Sisu."
      }
      return tableInfo
    } catch (error) {
      throw error
    }
  }

  private removeNumericFunctions(dimension: string, startStr: string, existingDimensionsList: string[]) {
    const endStr = ")"
    const firstHalf = dimension.substring(dimension.indexOf(`${startStr}`) + startStr.length, dimension.lastIndexOf(endStr))
    const secondHalf = dimension.substring(dimension.lastIndexOf(endStr) + 1)
    const cleanNumericDimension = firstHalf + secondHalf
    existingDimensionsList.push(cleanNumericDimension)
    return cleanNumericDimension
  }

  private getDimenionsListFromSQL(sql: string) {
    const indexOfSelect = sql.indexOf("SELECT") + "SELECT".length
    const indexOfFrom = sql.indexOf("FROM")
    const dimensions = sql.substring(indexOfSelect, indexOfFrom).trim()
    return dimensions.split(',')
  }

  private getExistingDimensions(sql: string, tableName: string) {
    const existingDimensionsMap: Record<string, boolean> = {}
    const existingDimensionsList: string[] = []
    const existingDimensions = this.getDimenionsListFromSQL(sql)
    existingDimensions.forEach((dimension) => {
      let dimensionName
      if (dimension.indexOf("AVG") >= 0) {
        dimensionName = this.removeNumericFunctions(dimension, "AVG(", existingDimensionsList)
      } else if (dimension.indexOf("COUNT") >= 0) {
        dimensionName = this.removeNumericFunctions(dimension, "COUNT(", existingDimensionsList)
      } else {
        dimensionName = dimension.substring(dimension.indexOf(`${tableName}."`), dimension.indexOf('AS')).trim()
        existingDimensionsList.push(dimension)
      }

      if (typeof dimensionName !== 'string') {
        throw "SQL function not supported."
      }
      existingDimensionsMap[dimensionName] = true
    })
    return {
      existingDimensionsMap,
      existingDimensionsList
    }
  }

  private getWhereStatement(sql: string) {
    const start = sql.indexOf('WHERE')
    const end = sql.indexOf('GROUP BY')
    const whereStatement = sql.includes('GROUP BY') ? sql.substring(start, end) : sql.substring(start)
    return whereStatement.trim()
  }

  private buildSisuBaseQuery(request: Hub.ActionRequest, tableInfo: string[], dimensions: string[]) {
    const requestSQL: string = request.attachment?.dataJSON.sql
    const tableDB = tableInfo[0]
    const tablePrivacy = tableInfo[1]
    const tableName = tableInfo[2]
    if (!requestSQL) {
      throw "There is no sql query in data"
    }
    const {existingDimensionsMap, existingDimensionsList} = this.getExistingDimensions(requestSQL, tableName)
    const nonIncludedDimensions = dimensions.filter((dimension) => !existingDimensionsMap[dimension])
    const dimensionToSelect = [...nonIncludedDimensions, ...existingDimensionsList].join(",")
    const whereStatementSQL = this.getWhereStatement(requestSQL)
    const baseQuery = `SELECT ${dimensionToSelect} FROM ${tableDB}.${tablePrivacy}.${tableName} ${whereStatementSQL}`
    return baseQuery.trim()
  }

  private async createMetric(request: Hub.ActionRequest, baseQueryId: string) {
    const axiosConfig = this.getAxiosConfig(request)
    const currentTime = new Date().toISOString()
    const metricName = `${currentTime}_${request.scheduledPlan?.title}_metric` || `${currentTime}_metric`
    const connectionId = request.formParams.connection
    if (!connectionId) {
      throw "User needs to select a Sisu connection"
    }
    const measure = request.attachment?.dataJSON.fields.measures[0]
    if (!measure) {
      throw "No measures in data"
    }

    const columnName = measure.name.includes('.') ? measure.name : measure.name.toUpperCase()

    const metricBody = {
      created_at: currentTime,
      data_source_id: connectionId,
      default_calculation: measure.type,
      desired_direction: measure.sorted.desc ? 'increase' : 'decrease',
      kpi_column_name: columnName,
      name: metricName,
      static_base_query_id: baseQueryId,
      metric_type: 'scalar'
    }

    try {
      const metricRequest = await axios.post('https://dev.sisu.ai/rest/metrics', metricBody, axiosConfig)
      return metricRequest.data
    } catch (error) {
      console.error('------- ERROR ------', error)
      throw error
    }
  }

  private async createQuery(request: Hub.ActionRequest, queryString: string) {
    const axiosConfig = this.getAxiosConfig(request)
    const connectionId = request.formParams.connection
    if (!connectionId) {
      throw "User needs to select a Sisu connection"
    }
    const currentTime = new Date().toISOString()
    const queryName = `${currentTime}_${request.scheduledPlan?.title}_query` || `${currentTime}_query`

    const newBaseQuery = {
      name: queryName,
      query_string: queryString
    }

    try {
      const queryRequest = await axios.post(`https://dev.sisu.ai/rest/data_sources/${connectionId}/custom_queries`, newBaseQuery, axiosConfig)
      return queryRequest.data
    } catch (error) {
      console.error(error)
      throw "Error creating a query."
    }
  }

  private async updateDefaultMetricDimensions(request: Hub.ActionRequest, baseQueryId: number, metricId: number) {
    const axiosConfig = this.getAxiosConfig(request)
    const dimensions = request.attachment?.dataJSON.fields.dimensions
    if (!dimensions || dimensions.length <= 0) {
      throw "No dimensions in data"
    }
    const lookerDimensionsMap: Record<string, boolean> = {}
    dimensions.forEach((dimension: Record<string, string>) => {
      lookerDimensionsMap[dimension.name] = true
    })

    try {
      const dimensionsReuqest = await axios.get(`https://dev.sisu.ai/rest/base_queries/${baseQueryId}/dimensions`, axiosConfig)
      const sisuDimensions = dimensionsReuqest.data
      const defaultDimensionsIds: number[] = []
      sisuDimensions.forEach((dimension: any) => {
        if (lookerDimensionsMap[dimension.columnName]) {
          defaultDimensionsIds.push(dimension.id)
        }
      })
      console.log('--- defaultDimensionsIds', defaultDimensionsIds)
      const body = {
        ids: defaultDimensionsIds,
      }
      const defaultDimensionsRequest = await axios.post(`https://dev.sisu.ai/rest/metrics/${metricId}/default_dimensions`, body, axiosConfig)
      console.log('--- defaultDimensionsRequest', defaultDimensionsRequest.data)
    } catch (error) {
      console.error(error)
      throw "Error creating a query."
    }
  }

  private findTableInfo(tables: string[][], tableName: string) {
    let tableDB
    const upperCaseTableName = tableName.toUpperCase()
    for (let index = 0; index < tables.length; index++) {
      const table = tables[index];
      const tableWithUpperCaseInfo = table.map(info => info.toUpperCase())
      if (tableWithUpperCaseInfo.includes(upperCaseTableName)) {
        tableDB = tableWithUpperCaseInfo
        break;
      }
    }
    return tableDB
  }

  private getAxiosConfig(request: Hub.ActionRequest) {
    const sisuAPIToken = request.params.sisu_api_token
    if (!sisuAPIToken) {
      throw "Need an API token."
    }
    return {
      headers: {
        'Authorization': sisuAPIToken
      }
    }
  }

  private async getSisuConnectionsOptions(request: Hub.ActionRequest) {
    const axisoConfig = this.getAxiosConfig(request)
    const response = await axios.get('https://dev.sisu.ai/rest/connections', axisoConfig)
    if (!response.data) {
      throw "Wasn't able to load Sisu connections."
    }
    return response.data.map((connection: any) => ({ name: connection.id, label: connection.name }))
  }

  async form(request: Hub.ActionRequest) {
    const connectionOptions = await this.getSisuConnectionsOptions(request)
    const form = new Hub.ActionForm()
    form.fields = [
      {
        label: "Sisu's connections",
        name: "connection",
        description: "Select the Sisu connection where this data is.",
        required: true,
        type: "select",
        options: connectionOptions,
      },
    ]
    return form
  }
}

Hub.addAction(new SisuAction())


