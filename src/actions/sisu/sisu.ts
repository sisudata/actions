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
    const requestSQL = request.attachment?.dataJSON.sql
    const axiosConfig = this.getAxiosConfig(request)

    // testStr.replace(/\n/g, ' ')
    console.log('** requestSQL:\n', requestSQL)
    // const url = "https://l9bte2tk86.execute-api.us-west-1.amazonaws.com/default/lookerActionAPI"
    // const body = {
    //   dataBuffer: request.attachment && request.attachment.dataBuffer,
    //   csvTitle: request.scheduledPlan && request.scheduledPlan.title,
    //   query: request.scheduledPlan && request.scheduledPlan.query,
    // }

    try {
      // await axios.post(url, body)
      const tables = await axios.get(`https://dev.sisu.ai/rest/connections/${connectionId}/tables`, axiosConfig)
      console.log('--- TABLES:', tables)
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