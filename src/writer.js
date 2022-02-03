import { gotScraping } from 'got-scraping';

// wip

export async function getOrCreateWriter(shopName, suffix) {
    
    console.log(`Checking if writer ${shopName}_${suffix} exists`);
    const getUrl =  "https://connection.eu-central-1.keboola.com/v2/storage/components?include=configuration"

    const getMethod = 'GET';
    const getHeaders = { 'x-storageapi-token': process.env.KEBOOLA_TOKEN };
    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url: getUrl,
        method: getMethod,
        headers: getHeaders,
    });
    // console.log(getBody);

    const writerDataAll = JSON.parse(getBody).find((i) => i.id === 'keboola.wr-aws-s3').configurations;
    const writerData = writerDataAll.find((i) => i.name.toLowerCase() === `${shopName}_${suffix}`);
    if (writerData) {
        console.log(`Writer ${shopName}_${suffix} exists, returning its ID.`);
        return writerData.id;
    }
    // Otherwise, create
    console.log(`Writer ${shopName}_${suffix} doesn't exist, I am going to create it.`);
    const postUrl =
        'https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs'
    const postMethod = 'POST'
    const formData = ({ name: `${shopName}_${suffix}` })
    const postHeaders = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
    
    const { body: postBody } = await gotScraping({
        useHeaderGenerator: false,
        url: postUrl,
        method: postMethod,
        headers: postHeaders,
        form: formData
    })
    console.log(`Writer ${shopName}_${suffix} has been created.`);
    const writerId = JSON.parse(postBody).id;

    return writerId;
      }

export async function updateWriter (shopName, suffix, writerId) {
    console.log(`Setting up credentials for ${shopName}-${suffix} writer.`)
    const url =
    `https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}`
    const method = 'PUT'
    const formData = {
      "configuration": JSON.stringify(
        {"parameters":
          {"accessKeyId":"AKIAZX7NKEIMII6WOZN7",
          "#secretAccessKey": process.env.AWS_TOKEN,
          "bucket":"data.hlidacshopu.cz"}})
      }
    const headers = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
    
    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        form: formData
    })

    console.log(`Credentials for ${shopName}-${suffix} writer have been set.`)
}

export async function getOrCreateTableRow(shopName, suffix, writerId) {
    console.log(`Checking if table row ${shopName}_${suffix} exists in the writer.`);

    const getUrl = `https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}/rows`
    const getMethod = 'GET';
    const getHeaders = { 'x-storageapi-token': process.env.KEBOOLA_TOKEN };
    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url: getUrl,
        method: getMethod,
        headers: getHeaders,
    });

    const tableRow = JSON.parse(getBody).find((i) => i.id !== '');

    if (tableRow) {
        console.log(`Table row for writer ${shopName}_${suffix} exists, returning its ID.`);
        return tableRow.id;
    }
    // Otherwise, create
    console.log(`Table row for writer ${shopName}_${suffix} doesn't exist, setting up now.`);
    const postUrl =`https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}/rows`
    const postMethod = 'POST'
    const formData =  {
      "name": `${shopName}_${suffix}`
      }
    const postHeaders = {
      'content-type': 'application/x-www-form-urlencoded',
      'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
  
    const { body: postBody } = await gotScraping({
        useHeaderGenerator: false,
        url: postUrl,
        method: postMethod,
        headers: postHeaders,
        form: formData
    })
  console.log(`Table row for ${shopName}_${suffix} writer has been created, returning its id.`);
  
  const rowId = JSON.parse(postBody).id; 
  return rowId;
}

export async function updateTableRow (shopName, suffix, writerId, rowId) {
    const shortSuffix = suffix.substring(3);
    console.log(
        `Updating table rows for writer ${shopName}_${suffix} (writer ID: ${writerId}, row ID: ${rowId}).`
    )

    const url = `https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}/rows/${rowId}`
    const method = 'PUT'
    const formData = {
        "configuration": JSON.stringify({
          "parameters": {
            "prefix": "items/"
          },
          "storage": {
            "input": {
              "tables": [
                {
                  "source": `out.c-${shopName}.${shopName}_${suffix}`,
                  "destination": `${shopName}_${suffix}.csv`
                }
              ]
            }
          },
          "processors": {
            "before": [
              {
                "definition": {
                  "component": "kds-team.processor-json-generator-hlidac-shopu"
                },
                "parameters": {
                  "format": shortSuffix
                }
              }
            ]
          }
        }),
    }

    const headers = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }

    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        form: formData
    })

    console.log(`Table rows for writer ${shopName}_${suffix} have been updated. (writer ID: ${writerId}, row ID: ${rowId}).`)
}
