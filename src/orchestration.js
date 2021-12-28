import { gotScraping } from 'got-scraping'

export async function getOrCreateOrchestration (shopName) {
    // Check if exists, if so, return id

    console.log(`Checking if orchestration ${shopName} already exists.`)

    const url =
        'https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations'
    const getMethod = 'GET'
    const getHeaders = {
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }

    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method: getMethod,
        headers: getHeaders
    })

    const orchestrationGetData = JSON.parse(getBody).find(
        i => i.name === shopName
    )
    if (orchestrationGetData) {
        console.log(
            `The orchestration ${shopName} already exists, returning its information.`
        )
        return orchestrationGetData
    }

    // Otherwise, create

    console.log(
        `The orchestration ${shopName} doesn't exists, I am going to create it now.`
    )

    const postMethod = 'POST'
    const postHeaders = {
        'content-type': 'application/json',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
    const requestBody = JSON.stringify({ name: shopName })

    const { body: orchestrationPostBody } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method: postMethod,
        headers: postHeaders,
        body: requestBody
    })

    console.log(
        `The orchestration ${shopName} has been created, returning its information.`
    )

    return JSON.parse(orchestrationPostBody)
}

export async function updateOrchestrationTasks (
    shopName,
    orchestrationId,
    transformationId
) {
    console.log(`I am going to update tasks in ${shopName} orchestration.`)

    const url = `https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations/${orchestrationId}/tasks`
    const method = 'PUT'
    const headers = {
        'content-type': 'application/json',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    };
    //TODO here I can add more tasks as well
    const requestBody = JSON.stringify([
        {
            component: 'keboola.snowflake-transformation',
            action: 'run',
            actionParameters: {
                config: transformationId
            },
            timeoutMinutes: null,
            active: true,
            continueOnFailure: false,
            phase: 'New phase'
        }
    ]);


    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        body: requestBody
    })

    console.log(`I have updated the tasks in ${shopName} orchestration: `)
    console.dir(body)
}

export async function updateOrchestrationNotifications(shopName, orchestrationId) {

    console.log(`I am going to update notifications in ${shopName} orchestration.`)

    const url = `https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations/${orchestrationId}/notifications`;
    const method = 'PUT';
    const headers = {
        'content-type': 'application/json',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    };
    const requestBody = JSON.stringify([
        {
            email: 'zuzka@apify.com',
            channel: 'error',
            parameters: {}
        },
        {
            email: 'zuzka@apify.com',
            channel: 'warning',
            parameters: {}
        },
        {
            email: 'zuzka@apify.com',
            channel: 'processing',
            parameters: {
                tolerance: 20
            }
        }
    ]);

    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        body: requestBody
    })

    console.log(`I have updated the notifications in ${shopName} orchestration: `)
    console.dir(body)
}

export async function updateOrchestrationTriggers (shopName, orchestrationId, orchestrationTokenId) {
    console.log(`I am going to update triggers in ${shopName} orchestration.`)

    const url = `https://connection.eu-central-1.keboola.com/v2/storage/triggers/`

    const method = 'POST'
    const headers = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
    const form = {
        runWithTokenId: orchestrationTokenId,
        component: 'orchestrator',
        configurationId: orchestrationId,
        coolDownPeriodMinutes: 78,
        'tableIds[0]': 'in.c-Example-new-shops.aaaauto_clean'
    }

    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        form
    })

    console.log(`I have updated the triggers in ${shopName} orchestration: `)
    console.dir(body)
}
