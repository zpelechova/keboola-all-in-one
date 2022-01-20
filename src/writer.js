import { gotScraping } from 'got-scraping';

// wip

export async function getOrCreateWriter(shopName, suffix) {
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
    if (writerData) return writerData.id;

    // Otherwise, create
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

    const writerId = JSON.parse(postBody).id; 
    return writerId;
}

export async function updateWriter (shopName, suffix, writerId) {
    const shortSuffix = suffix.substring(3);
    console.log(
        `I am going to update writer ${shopName}_${suffix} (writer ID: ${writerId}).`
    )

    const url = `https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}`

    const method = 'PUT'
    const formData = {
        "description": "Writing price history to S3 AWS",
        "changeDescription": "Configuration edited via API",
        "configuration": JSON.stringify({
            "parameters": {
                "accessKeyId": "AKIAZX7NKEIMGRBOQF6W",
                "#secretAccessKey": process.env.AWS_TOKEN,
                "bucket": "data.hlidacshopu.cz",
                "prefix": "items/"
            },
            "storage": {
                "input": {
                    "tables": [
                        {
                            "source": `out.c-0-${shopName}.${shopName}_${suffix}`,
                            "destination": `shop_${shortSuffix}.csv`
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

    console.log(`I have updated the writer ${shopName}_${suffix} with writer ID: ${writerId}`)
}