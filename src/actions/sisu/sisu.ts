import * as Hub from "../../hub"
import axios from "axios"

export class SisuAction extends Hub.Action {

  name = "sisu"
  label = "Sisu Data - Create New KDA"
  iconName = "sisu/sisu_logo.svg"
  description = "Send data to Sisu and create a new kda."
  supportedActionTypes = [Hub.ActionType.Cell, Hub.ActionType.Dashboard, Hub.ActionType.Query]
  supportedFormats = [Hub.ActionFormat.JsonDetail]
  supportedFormattings = [Hub.ActionFormatting.Formatted]
  params = [
    {
      name: "sisu_api_token",
      label: "API token",
      required: false,
      sensitive: false,
    }
  ]

  async execute(request: Hub.ActionRequest) {
    try {
      const tableDB = await this.getTableDB(request)
      const sisuBaseQuery = this.buildSisuBaseQuery(request, tableDB)
      const baseQuery= await this.createQuery(request, sisuBaseQuery)
      const metric = await this.createMetric(request, baseQuery.base_query_id)
      console.log('--- metric:', metric)

      return new Hub.ActionResponse({ success: true })
    } catch (error) {
      return new Hub.ActionResponse({ success: false })
    }
  }

  async form(request: Hub.ActionRequest) {
    const axisoConfig = this.getAxiosConfig(request)
    const response = await axios.get('https://dev.sisu.ai/rest/connections', axisoConfig)
    if (!response.data) {
      throw "Wasn't able to load Sisu connections."
    }
    const options = response.data.map((connection: any) => {
      return { name: connection.id, label: connection.name }
    })

    const form = new Hub.ActionForm()
    form.fields = [{
      label: "Sisu's connections",
      name: "connection",
      description: "Select the Sisu connection where this data is.",
      required: true,
      type: "select",
      options,
    }]
    return form
  }

  private async getTableDB(request: Hub.ActionRequest) {
    const axiosConfig = this.getAxiosConfig(request)
    const connectionId = request.formParams.connection
    const tableName = request.scheduledPlan?.query?.view || request.scheduledPlan?.query?.model

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
      const tableDB = this.findTableDB(tables.data.tables, tableName)
      if (!tableDB) {
        throw "Wasn't able to find a table in Sisu."
      }
      return tableDB
    } catch (error) {
      throw error
    }
  }

  private buildSisuBaseQuery(request: Hub.ActionRequest, tableDB: string) {
    const requestSQL: string = request.attachment?.dataJSON.sql
    if (!requestSQL) {
      throw "There is no sql query in data"
    }
    return requestSQL.slice(0, requestSQL.indexOf('FROM') + 'FROM'.length) + ` "${tableDB}".` + requestSQL.slice(requestSQL.indexOf('FROM') + ('FROM'.length + 1))
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
    const metricBody = {
      created_at: currentTime,
      data_source_id: connectionId,
      default_calculation: measure.type,
      desired_direction: measure.sorted.desc ? 'decrease' : 'increase',
      kpi_column_name: measure.sql,
      name: metricName,
      static_base_query_id: baseQueryId
    }
    
    try {
      const metricRequest = await axios.post('https://dev.sisu.ai/rest/metrics', metricBody, axiosConfig)
      return metricRequest.data
    } catch (error) {
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

  private findTableDB(tables: string[][], tableName: string) {
    let tableDB
    const upperCaseTableName = tableName.toUpperCase()
    for (let index = 0; index < tables.length; index++) {
      const table = tables[index];
      const tableWithUpperCaseInfo = table.map(info => info.toUpperCase())
      if (tableWithUpperCaseInfo.includes(upperCaseTableName)) {
        tableDB = tableWithUpperCaseInfo[0]
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
}

Hub.addAction(new SisuAction())


