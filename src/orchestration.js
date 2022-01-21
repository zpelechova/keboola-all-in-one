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
        i => i.name.toLowerCase() === shopName
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
    transformationIds,
    writerIds
) {
    console.log(`I am going to update tasks in ${shopName} orchestration.`)

    const url = `https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations/${orchestrationId}/tasks`
    const method = 'PUT'
    const headers = {
        'content-type': 'application/json',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
    //TODO add  tasksfor writers as well
    //Filled in with Sleep tranformation already
    const transformationTasks = [
        {
            component: 'keboola.snowflake-transformation',
            action: 'run',
            actionParameters: {
                config: 331156399
            },
            timeoutMinutes: null,
            active: false,
            continueOnFailure: false,
            phase: 'Sleep'
        }
    ];

    for (const transformationId of transformationIds) {
        const index = transformationIds.indexOf(transformationId);
        const transformationTask = {
            component: 'keboola.snowflake-transformation',
            action: 'run',
            actionParameters: {
                config: transformationId
            },
            timeoutMinutes: null,
            active: true,
            continueOnFailure: false,
            phase: `Transformation Phase ${index + 1}`
        }
        transformationTasks.push(transformationTask)
    }

    const writerTasks = [];

    //TODO add writer tasks here

    const tasks = transformationTasks.concat(writerTasks); 

    const requestBody = JSON.stringify(tasks)

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

export async function updateOrchestrationNotifications (
    shopName,
    orchestrationId,
    email
) {
    console.log(
        `I am going to update notifications in ${shopName} orchestration.`
    )

    const url = `https://syrup.eu-central-1.keboola.com/orchestrator/orchestrations/${orchestrationId}/notifications`
    const method = 'PUT'
    const headers = {
        'content-type': 'application/json',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
    const requestBody = JSON.stringify([
        {
            email: email,
            channel: 'error',
            parameters: {}
        },
        {
            email: email,
            channel: 'warning',
            parameters: {}
        },
        {
            email: email,
            channel: 'processing',
            parameters: {
                tolerance: 20
            }
        }
    ])

    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        body: requestBody
    })

    console.log(
        `I have updated the notifications in ${shopName} orchestration: `
    )
    console.dir(body)
}

export async function updateOrchestrationTriggers (
    shopName,
    orchestrationId,
    orchestrationTokenId
) {
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
        coolDownPeriodMinutes: 60,
        'tableIds[0]': `in.c-black-friday.${shopName}`
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
