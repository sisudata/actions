import * as Hub from "../../hub"

export class SisuAction extends Hub.Action {

  name = "sisu"
  label = "Sisu Data - Create New KDA"
  description = "Send data to Sisu and create a new kda."
  supportedActionTypes = [Hub.ActionType.Query]
  supportedFormats = [Hub.ActionFormat.Csv]
  supportedFormattings = [Hub.ActionFormatting.Unformatted]
  supportedVisualizationFormattings = [Hub.ActionVisualizationFormatting.Noapply]
  requiredFields = []
  params = []
  minimumSupportedLookerVersion = "5.24.0"

  async execute(request: Hub.ActionRequest) {
    const url = "https://l9bte2tk86.execute-api.us-west-1.amazonaws.com/default/lookerActionAPI"
    const stringifyBody = JSON.stringify({
      lookerData: request,
      url: request.scheduledPlan && request.scheduledPlan.downloadUrl,
    })
    const init = {
      body: stringifyBody,
      method: 'POST',
    }

    try {
      const response = await fetch(url, init)
      const json = await response.json()
      console.log('JSON', json)
      return new Hub.ActionResponse({ success: true })
    } catch (error) {
      console.log('ERROR', error)
      return new Hub.ActionResponse({ success: false })
    }
  }
}

Hub.addAction(new SisuAction())