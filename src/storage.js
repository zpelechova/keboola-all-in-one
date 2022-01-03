import { Apify } from 'apify'
import { gotScraping } from 'got-scraping'

export async function getOrCreateTable (shopName) {
    console.log(`Checking if table ${shopName} already exists.`)

    const getUrl =
        'https://connection.eu-central-1.keboola.com/v2/storage/tables?include=buckets,columns,metadata,columnMetadata'
    const getMethod = 'GET'
    const getHeaders = {
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }

    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url: getUrl,
        method: getMethod,
        headers: getHeaders
    })

    const getStorageData = JSON.parse(getBody).find(
        i => i.id === `in.c-black-friday.${shopName}`
    )
    if (getStorageData) {
        console.log(
            `The table ${shopName} already exists, returning its information.`
        )
        return getStorageData
    }

    // Otherwise, create

    console.log(
        `The table ${shopName} doesn't exist, I am going to create it now.`
    )

    // Not sure if dataFileId will last forever, hope so, otherwise either get new Id by manually uploading the csv file, or contact Keboola how to do it

    const postUrl =
        'https://connection.eu-central-1.keboola.com/v2/storage/buckets/in.c-black-friday/tables-async'
    const postMethod = 'POST'
    const postHeaders = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
    const postFormData = {
        name: shopName,
        dataFileId: 364223886
    }

    const { body: postBody } = await gotScraping({
        useHeaderGenerator: false,
        url: postUrl,
        method: postMethod,
        headers: postHeaders,
        form: postFormData
    })

    const postStorageData = JSON.parse(postBody)
    if (postStorageData) {
        console.log(
            `The table ${shopName} has been created, returning its information.`
        )
        return postStorageData
    }
}

export async function checkTable () {
    console.log(`Getting information about all tables`)

    const getUrl =
        'https://connection.eu-central-1.keboola.com/v2/storage/tables?include=buckets,columns,metadata,columnMetadata'
    const getMethod = 'GET'
    const getHeaders = {
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }

    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url: getUrl,
        method: getMethod,
        headers: getHeaders
    })

    const tablesData = JSON.parse(getBody)
    return tablesData
}
