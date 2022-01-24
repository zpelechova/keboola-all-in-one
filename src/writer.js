import { gotScraping } from 'got-scraping';
const findAnd = require('find-and');

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

export async function getOrCreateTableRow(shopName, suffix, writerId) {
    
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
        console.log(`Writer ${shopName}_${suffix} exists, checking for its table-row ID.`);
        const rows = findAnd.returnFound(writerData, {rows: '0'});
        return rows.id;
  }
  // Otherwise, create
  console.log(`Setting up table-row for ${shopName}_${suffix} writer.`);
  const postUrlRows =
      'https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}/rows'
  const postMethodRows = 'POST'
  const formDataRows = ({"parameters":{"prefix":""},"storage":{"input":{"tables":[{"source":"out.c-test.test","destination":"test.csv"}]}},"processors":{"before":[{"definition":{"component":"keboola.processor-move-files"},"parameters":{"direction":"files"}}]}})
  const postHeadersRows = {
      'content-type': 'application/x-www-form-urlencoded',
      'x-storageapi-token': process.env.KEBOOLA_TOKEN
  }
  
  const { body: postBodyRows } = await gotScraping({
      useHeaderGenerator: false,
      url: postUrlRows,
      method: postMethodRows,
      headers: postHeadersRows,
      form: formDataRows
  })
  console.log(`Table-row for ${shopName}_${suffix} writer has been created.`);
  
  const rowId = JSON.parse(postBodyRows).id; 
  return rowId;
}

export async function updateWriter (shopName, suffix, writerId, rowId) {
    const shortSuffix = suffix.substring(3);
    console.log(
        `I am going to update writer ${shopName}_${suffix} (writer ID: ${writerId}, row ID: ${rowId}).`
    )

    const url = `https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}`

    const method = 'PUT'
    const formData = {
        "configuration": JSON.stringify({
            "parameters": {
                "accessKeyId": "AKIAZX7NKEIMGRBOQF6W",
                "#secretAccessKey": process.env.AWS_TOKEN,
                "bucket": "data.hlidacshopu.cz",
                "prefix": "items/"
            },
            "rowsSortOrder": [],
            "rows": [
                {
                    "id": `${rowId}`,
                    "name": `${shopName}_${suffix}`,
                    "description": "Writing price history to S3 AWS",
                    "isDisabled": false,
                    "changeDescription": "Configuration edited via API",
                    "state": {
                        "component": [],
                        "storage": {
                            "input": {
                                "tables": [
                                    {
                                        "source": `out.c-0-${shopName}.${shopName}_${suffix}`,
                                      }
                                ],
                                "files": []
                            }
                        }
                    },
                    "configuration": {
                        "parameters": {
                            "prefix": "items/"
                        },
                        "storage": {
                            "input": {
                                "tables": [
                                    {
                                        "source": `out.c-0-${shopName}.${shopName}_${suffix}`,
                                        "destination": `shop_${suffix}.csv`
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
                    }
                }
            ],
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

    console.log(`I have updated the writer ${shopName}_${suffix} (writer ID: ${writerId}).`)
}
