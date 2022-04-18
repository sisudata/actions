import * as Hub from "../../hub"
import axios from "axios"
export class SisuAction extends Hub.Action {

  name = "sisu"
  label = "Sisu Data - Create New KDA"
  iconName = "sisu/sisu_logo.svg"
  description = "Send data to Sisu and create a new kda."
  supportedActionTypes = [Hub.ActionType.Cell, Hub.ActionType.Dashboard, Hub.ActionType.Query]
  supportedFormats = [Hub.ActionFormat.JsonDetail]
  supportedFormattings = [Hub.ActionFormatting.Unformatted]
  params = [
    {
      name: "sisu_api_token",
      label: "API token",
      required: false,
      sensitive: false,
    }
  ]

  async execute(request: Hub.ActionRequest) {
    console.log('** Request:\n', JSON.stringify(request))
    const url = "https://l9bte2tk86.execute-api.us-west-1.amazonaws.com/default/lookerActionAPI"
    const body = {
      dataBuffer: request.attachment && request.attachment.dataBuffer,
      csvTitle: request.scheduledPlan && request.scheduledPlan.title,
      query: request.scheduledPlan && request.scheduledPlan.query,
    }

    try {
      const response = await axios.post(url, body)
      console.log('** Response', response)
      return new Hub.ActionResponse({ success: true })
    } catch (error) {
      console.log('** ERROR', error)
      return new Hub.ActionResponse({ success: false })
    }
  }

  async form(request: Hub.ActionRequest) {
    const sisuAPIToken = request.params.sisu_api_token
    if (!sisuAPIToken) {
      throw "Need an API token."
    }

    const options = [
      { name: 'Test123', label: 'Test'}
    ]

    const axisoConfig = {
      headers: {
        'Authorization': sisuAPIToken
      }
    }
    const connections = await axios.get('https://dev.sisu.ai/rest/connections', axisoConfig)

    console.log('--- connections', connections)
    const form = new Hub.ActionForm()
    form.fields = [{
      label: "Sisu's connections",
      name: "connection",
      description: "Select the equivalent connection'data-warehouse in Sisu.",
      required: true,
      type: "select",
      options,
    }]
    return form
  }
}

Hub.addAction(new SisuAction())