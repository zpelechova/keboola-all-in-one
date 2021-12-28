import { gotScraping } from 'got-scraping';

// wip

export async function getOrCreateWriter(shopName, suffix, description) {
    const getUrl = 'https://connection.eu-central-1.keboola.com/v2/storage/components?componentType=writer';
    const getMethod = 'GET';
    const getHeaders = { 'x-storageapi-token': process.env.KEBOOLA_TOKEN };
    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url: getUrl,
        method: getMethod,
        headers: getHeaders,
    });
    console.log(getBody);

    const writerData = JSON.parse(getBody)[0].configurations.find((i) => i.name === shopName + suffix);
    if (writerData) return writerData.id;

    // Otherwise, create
    const postUrl =
        'https://connection.eu-central-1.keboola.com/v2/storage/components/kds-team.wr-dynamodb/configs'
    const postMethod = 'POST'
    const postFormData = {
        name: `${shopName}_${suffix}`,
        description,
        configuration: JSON.stringify({
            storage: {
                input: {
                    tables: [
                        {
                            source: `out.c-0-${shopName}.${shopName}`,
                            destination: '${shopName}.csv',
                            where_column: '',
                            where_values: [],
                            where_operator: 'eq',
                            columns: []
                        }
                    ]
                }
            },
            parameters: {
                region: 'eu-central-1',
                table_name: 'all_shops',
                access_key_id: 'AKIAZX7NKEIMAERBXWRC',
                column_config: [
                    { name: 'p_key', type: 'scalar' },
                    { name: 'json', type: 'scalar' }
                ],
                '#access_key_secret': process.env.AWS_TOKEN
            }
        })
    }
    const postHeaders = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': 'process.env.KEBOOLA_TOKEN'
    }

    const { body: postBody } = await gotScraping({
        useHeaderGenerator: false,
        postUrl,
        postMethod,
        postHeaders,
        form: postFormData
    })

    console.dir(JSON.parse(postBody))
}
