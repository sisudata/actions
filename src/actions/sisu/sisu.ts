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
    // const stringifyRequest = JSON.stringify(request)
    const connectionId = request.formParams.connection
    const requestSQL: string = request.attachment?.dataJSON.sql
    const tableName = request.scheduledPlan?.query?.view || request.scheduledPlan?.query?.model || ''
    const axiosConfig = this.getAxiosConfig(request)

    // testStr.replace(/\n/g, ' ')
    console.log('** requestSQL:\n', requestSQL)
    console.log('** requestSQL:\n', requestSQL.indexOf('FROM'))
    // const url = "https://l9bte2tk86.execute-api.us-west-1.amazonaws.com/default/lookerActionAPI"
    // const body = {
    //   dataBuffer: request.attachment && request.attachment.dataBuffer,
    //   csvTitle: request.scheduledPlan && request.scheduledPlan.title,
    //   query: request.scheduledPlan && request.scheduledPlan.query,
    // }

    try {
      // await axios.post(url, body)
      const tables = await axios.get(`https://dev.sisu.ai/rest/connections/${connectionId}/tables`, axiosConfig)
      const tableDB = this.findTableDB(tables.data.tables, tableName)
      if (!tableDB) {
        throw "Wasn't able to map a table in Sisu."
      }
      const newSQL = requestSQL.slice(0, requestSQL.indexOf('FROM') + 'FROM'.length) + ` "${tableDB}".` + requestSQL.slice(requestSQL.indexOf('FROM') + ('FROM'.length - 1))
      console.log('--- newSQL:', newSQL)

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
      return { name: connection.id, label: connection.name}
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


