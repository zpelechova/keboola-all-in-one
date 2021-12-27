import { gotScraping } from 'got-scraping';

export async function getOrCreateOrchestration(shopName) {
    
    // Check if exists, if so, return id
    const url = 'https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations'
    const getMethod = 'GET';
    const getHeaders = { 'x-storageapi-token': process.env.KEBOOLA_TOKEN };

    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method: getMethod,
        headers: getHeaders,
    });

    // console.log(getBody);

    const orchestrationData = JSON.parse(getBody).find((i) => i.name === shopName);
    if (orchestrationData) return orchestrationData;

    // Otherwise, create

    const postMethod = 'POST';
    const requestBody = JSON.stringify({"name": shopName});
    const postHeaders = {
        'content-type': 'application/json',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN,
    };

    const { body: postBody } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method: postMethod,
        headers: postHeaders,
        body: requestBody
    });

    console.dir(JSON.parse(postBody));

    return JSON.parse(postBody).id;
}

export async function updateOrchestrationTasks(orchestrationId, transformationId) {
  // TODO: nicer format of long line
  const url = `https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations/${orchestrationId}/tasks`;

  const method = 'PUT';
  const formData = JSON.stringify(
        [
          {
              "component": "keboola.snowflake-transformation",
              "action": "run",
              "actionParameters": {
                  "config": transformationId
              },
              "timeoutMinutes": null,
              "active": true,
              "continueOnFailure": false,
              "phase": "New phase"
          }
      ]
      )
  ;
  const headers = {
      'content-type': 'application/json',
      'x-storageapi-token': process.env.KEBOOLA_TOKEN,
  };

  const { body } = await gotScraping({
      useHeaderGenerator: false,
      url,
      method,
      headers,
      body: formData,
  });

  console.dir(JSON.parse(body));
}

export async function updateOrchestrationNotifications(orchestrationId) {
  // TODO: nicer format of long line
  const url = `https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations/${orchestrationId}/notifications`;

  const method = 'PUT';
  const formData = JSON.stringify(
    [
      {
          "email": "zuzka@apify.com",
          "channel": "error",
          "parameters": {}
      },
      {
          "email": "zuzka@apify.com",
          "channel": "warning",
          "parameters": {}
      },
      {
          "email": "zuzka@apify.com",
          "channel": "processing",
          "parameters": {
              "tolerance": 20
          }
      }
    ]
  )
  ;
  const headers = {
      'content-type': 'application/json',
      'x-storageapi-token': process.env.KEBOOLA_TOKEN,
  };

  const { body } = await gotScraping({
      useHeaderGenerator: false,
      url,
      method,
      headers,
      body: formData,
  });

  console.dir(JSON.parse(body));
}

export async function getOrCreateOrchestrationToken(orchestrationId) {

  const tokenGetUrl = `https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations`;
  const tokenGetMethod = 'GET';
  const tokenGetHeaders = {
      'x-storageapi-token': process.env.KEBOOLA_TOKEN,
  };

  const { body:tokenGetBody } = await gotScraping({
      useHeaderGenerator: false,
      url: tokenGetUrl,
      method: tokenGetMethod,
      headers: tokenGetHeaders
  });

  const tokenGetData = JSON.parse(tokenGetBody).find((i) => i.id === orchestrationId);
  if (tokenGetData) return tokenGetData.token.id;

  console.dir(JSON.parse(body));
}

export async function updateOrchestrationTriggers(orchestrationId, orchestrationTokenId) {

  const triggerUrl = `https://connection.eu-central-1.keboola.com/v2/storage/triggers/`;


  const triggerMethod = 'POST';
  const triggerForm = {
      "runWithTokenId": orchestrationTokenId,
      "component": "orchestrator",
      "configurationId": orchestrationId,
      "coolDownPeriodMinutes": 78,
      "tableIds[0]": "in.c-Example-new-shops.aaaauto_clean"
  }
  ;
  const triggerHeaders = {
      'content-type': 'application/x-www-form-urlencoded',
      'x-storageapi-token': process.env.KEBOOLA_TOKEN,
  };

  const { body } = await gotScraping({
      useHeaderGenerator: false,
      url: triggerUrl,
      method: triggerMethod,
      headers: triggerHeaders,
      form: triggerForm 
  });

  console.dir(JSON.parse(body));
}

