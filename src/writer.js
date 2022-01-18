import { gotScraping } from 'got-scraping';

// wip

export async function getOrCreateWriter(shopName) {
    // const postUrl = '';
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
    const writerData = writerDataAll.find((i) => i.name.toLowerCase() === shopName); 
    if (writerData) return writerData.id;

    // Otherwise, create
    const postUrl =
        'https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs'
    const postMethod = 'POST'
    const formData = ({ name: shopName })
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
