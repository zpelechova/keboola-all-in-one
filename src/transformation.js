import { gotScraping } from 'got-scraping';

export async function getOrCreateTransformation(shopName, suffix) {
    const transformationName = `${shopName}${suffix}`;

    // Check if exists, if so, return id
    const getUrl = 'https://connection.eu-central-1.keboola.com/v2/'
        + 'storage/'
        + 'components'
        + '?componentType=transformation'
        + '&include=configuration';

    const getMethod = 'GET';

    const getHeaders = { 'x-storageapi-token': process.env.KEBOOLA_TOKEN };

    const { body: getBody } = await gotScraping({
        useHeaderGenerator: false,
        url: getUrl,
        method: getMethod,
        headers: getHeaders,
    });

    console.log(getBody);

    const transformationData = JSON.parse(getBody)[0].configurations.find((i) => i.name === shopName + suffix);
    if (transformationData) return transformationData.id;

    // Otherwise, create
    const description = 'This is description';

    const postUrl = 'https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.snowflake-transformation/configs';
    const postMethod = 'POST';
    const postFormData = {
        name: `${transformationName}`,
        description,
    };
    const postHeaders = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN,
    };

    const { body: postBody } = await gotScraping({
        useHeaderGenerator: false,
        url: postUrl,
        method: postMethod,
        headers: postHeaders,
        form: postFormData,
    });

    console.dir(JSON.parse(postBody));

    return JSON.parse(postBody).id;
}

export async function updateTransformation(shopId, trsfDescription, inputSource, inputName, outputName, outputDestination, prim_keys, blockName, codeName, code) {
    // TODO: nicer format of long line
    const url = `https://connection.eu-central-1.keboola.com/v2/`
    + `storage/`
    + `components/`
    + `keboola.snowflake-transformation/`
    + `configs/`
    + `${shopId}`;

    const inTables = [];
    if (inputSource.length === inputName.length) {
        for (const i in inputSource) {
            const inTable = {
                source: inputSource[i],
                destination: inputName[i],            };
            inTables.push(inTable);
        }
    } else {
        console.log("The input tables are not defined properly.")
        return "READ THE ERROR MESSAGE"
    }

    // const outTables = [];
    // if (outputDestination.length === outputName.length) {
    //     for (const i in outputDestination) {
    //         const outTable = {
    //             destination: outputDestination[i],
    //             source: outputName[i],
    //             primary_key: prim_key[i],
    //         };
    //         outTables.push(outTable);
    //     }
    // } else {
    //     console.log("The output tables are not defined properly.")
    //     return "READ THE ERROR MESSAGE"
    // }

    const method = 'PUT';
    const formData = {
        configuration: JSON.stringify({
            parameters: {
                blocks: [
                    {
                        name: blockName,
                        codes: [
                            {
                                name: codeName,
                                script: [code],
                            },
                        ],
                    },
                ],
            },
            storage: {
                input: {
                    tables: inTables,
                },
                output: {
                  tables: [
                      {
                        source: outputName,
                        destination: outputDestination,
                        primary_key: prim_keys,
                      },
                  ],
                },
            },
        }),
        description: trsfDescription,
        changeDescription: 'Playing with API',
    };
    const headers = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN,
    };

    const { body } = await gotScraping({
        useHeaderGenerator: false,
        url,
        method,
        headers,
        form: formData,
    });

    console.dir(JSON.parse(body));
}

