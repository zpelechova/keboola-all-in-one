import { gotScraping } from 'got-scraping'

export async function getOrCreateTransformation (shopName, suffix, KEBOOLA_TOKEN) {
    const transformationName = `${shopName}_${suffix}`

    console.log(
        `Checking if transformation ${transformationName} already exists.`
    )

    // Check if exists, if so, return id
    const getUrl =
        'https://connection.europe-west3.gcp.keboola.com/v2/' +
        'storage/' +
        'components' +
        '?componentType=transformation' +
        '&include=configuration'

    const getMethod = 'GET'

    const getHeaders = { 'x-storageapi-token': KEBOOLA_TOKEN }

    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url: getUrl,
        method: getMethod,
        headers: getHeaders
    })

    const transformationAllData = JSON.parse(getBody)

    let transformationData = {}

    for (const t of transformationAllData) {
        if ((t.id = 'keboola.snowflake-transformation')) {
            transformationData = t.configurations.find(
                i => i.name === transformationName
            )
        }
    }

    if (transformationData) {
        console.log(
            `The transformation ${transformationName} already exists, returning its information.`
        )
        return transformationData.id
    }

    // Otherwise, create

    console.log(
        `The transformation ${transformationName} doesn't exists, going to create it now.`
    )
    const description = 'This is description'

    const postUrl =
        'https://connection.europe-west3.gcp.keboola.com/v2/storage/components/keboola.snowflake-transformation/configs'
    const postMethod = 'POST'
    const postFormData = {
        name: `${transformationName}`,
        description
    }
    const postHeaders = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': KEBOOLA_TOKEN
    }

    const { body: postBody } = await gotScraping({
        useHeaderGenerator: false,
        url: postUrl,
        method: postMethod,
        headers: postHeaders,
        form: postFormData
    })

    console.log(
        `The transfromation ${transformationName} has been created, returning its information.`
    )

    return JSON.parse(postBody).id
}

export async function updateTransformation (
    transformationId,
    transformationDescription,
    inputSources,
    inputNames,
    outputNames,
    outputDestinations,
    primaryKeys,
    incremental,
    variables_id,
    variables_values_id,
    blockName,
    codeName,
    code,
    KEBOOLA_TOKEN
) {
    console.log(
        `Going to update tasks in ${transformationId} transformation.`
    )

    const url =
        `https://connection.europe-west3.gcp.keboola.com/v2/` +
        `storage/` +
        `components/` +
        `keboola.snowflake-transformation/` +
        `configs/` +
        `${transformationId}`

    const inTables = []
    if (inputSources.length === inputNames.length) {
        for (const inputSource of inputSources) {
            const index = inputSources.indexOf(inputSource)
            const inTable = {
                source: inputSource,
                destination: inputNames[index]
            }
            inTables.push(inTable)
        }
    } else {
        console.log(
            'The input tables are not defined properly - there is different number of sources and names in their respective arrays.'
        )
        return 'READ THE ERROR MESSAGE'
    }

    const outTables = []
    if (outputDestinations.length === outputNames.length) {
        for (const outputDestination of outputDestinations) {
            const index = outputDestinations.indexOf(outputDestination)
            const outTable = {
                destination: outputDestination,
                source: outputNames[index],
                primary_key: primaryKeys[index],
                incremental: incremental[index]
            }
            outTables.push(outTable)
        }
    } else {
        console.log(
            'The output tables are not defined properly - there is different number of destinations and names/primary keys in their respective arrays.'
        )
        return 'READ THE ERROR MESSAGE'
    }

    const method = 'PUT'
    const formData = {
        configuration: JSON.stringify({
            parameters: {
                blocks: [
                    {
                        name: blockName,
                        codes: [
                            {
                                name: codeName,
                                script: code
                            }
                        ]
                    }
                ]
            },
            storage: {
                input: {
                    tables: inTables
                },
                output: {
                    tables: outTables
                }
            },
            variables_id,
            variables_values_id
        }),
        description: transformationDescription,
        changeDescription: 'Changing the transformation via API'
    }
    const headers = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': KEBOOLA_TOKEN
    }

    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        form: formData
    })

    console.log(`The ${transformationId} transformation has been updated. `)
}

export async function migrate (KEBOOLA_TOKEN) {
    console.log(`Going to run migration transformation.`)

    const url =
        'https://syrup.eu-central-1.keboola.com/docker/keboola.snowflake-transformation/run'
    const method = 'POST'
    const requestBody = JSON.stringify({ config: '367214386' })
    const headers = {
        'content-type': 'application/json',
        'x-storageapi-token': KEBOOLA_TOKEN
    }

    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        body: requestBody
    })

    console.log(`The transformation has started running. `)
}
