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
    const postUrlNew =
    `https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}`
    const postMethodNew = 'PUT'
    const formDataNew = {
      "configuration": JSON.stringify(
        {"parameters":{"accessKeyId":"AKIAZX7NKEIMGRBOQF6W","#secretAccessKey":"KBC::ProjectSecure::eJxLtDKyqs60MrAutjI0MLZSuveViWGecMEluV0nL2xbuj7npJJ8BuM/oWkHBJgby5w/+hfc2vXHYdLnW/6vS+T81jMYuK1vjtxzhFfn2pESHX/VySELV78W6WayXF3/4nZkYWNYD3PpU42JH/gkMm+ZX9BKcSsum+JaH3v2upJ1ppUhyE4LEyslRiZmhgp1rrT1nO9lTuyTv33j5gExE5fLl/ikKl9LGhZvnVS1SP4cY+a+nkV3xQ7NVOus3+gfvECJgYGhzqCGjVOrzaPtOy8jO9uCfINcJkYGgwyEGKOBHBtnQpsHYyozC6OegSALj0+bCWPWb/VV1WmLPjMxCjRYs5dbH3Ts4l0qc0b04pJ2nQ2+UVqRUbtmqfxcHNo200ax69gpU5vZpcWH5ucXnyg3NBK87/VKRWX9NanuhldK1rUAw7195A==","bucket":"data.hlidacshopu.cz"}})
      }
    const postHeadersNew = {
        'content-type': 'application/x-www-form-urlencoded',
        'x-storageapi-token': process.env.KEBOOLA_TOKEN
    }
    
    const { body: postBodyNew } = await gotScraping({
        useHeaderGenerator: false,
        url: postUrlNew,
        method: postMethodNew,
        headers: postHeadersNew,
        form: formDataNew
    })
    return writerId;
  }

export async function getOrCreateTableRow(shopName, suffix) {
    console.log(`Checking if table-row ${shopName}_${suffix} exists`);
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
    const writerId = writerData.id;

    const getUrlRow = `https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}/rows`
    const getMethodRow = 'GET';
    const getHeadersRow = { 'x-storageapi-token': process.env.KEBOOLA_TOKEN };
    const { body: getBodyRow } = await gotScraping({
        useHeaderGenerator: false,
        url: getUrlRow,
        method: getMethodRow,
        headers: getHeadersRow,
    });
    // console.log(getBody);

    const writerDataAllRow = JSON.parse(getBodyRow).find((i) => i.id !== '');
    if (writerDataAllRow) {
        console.log(`Table-row for writer ${shopName}_${suffix} exists, returning its ID.`);
        return writerDataAllRow.id;
    }
    // Otherwise, create
    console.log(`Setting up table-row for ${shopName}_${suffix} writer.`);
    const postUrlRows =`https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}/rows`
    const postMethodRows = 'POST'
    const formDataRows =  {
      "name": `${shopName}_${suffix}`,
      "configuration": JSON.stringify(
        {"parameters":{"prefix":""},"storage":{"input":{"tables":[{"source":`out.c-0-${shopName}.${shopName}_${suffix}`,"destination":`shop_${suffix}.csv`}]}},"processors":{"before":[{"definition":{"component":"keboola.processor-move-files"},"parameters":{"direction":"files"}}]}})
      }
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

    const url = `https://connection.eu-central-1.keboola.com/v2/storage/components/keboola.wr-aws-s3/configs/${writerId}/rows/${rowId}`
    const method = 'PUT'
    const formData = {
        "configuration": JSON.stringify({
          "parameters": {
            "prefix": "zkouska/items/"
          },
          "storage": {
            "input": {
              "tables": [
                {
                  "source": `out.c-0-${shopName}.${shopName}_${suffix}_zkouska`,
                  "destination": `${shopName}_${suffix}_zkouska.csv`
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

    console.log(`I have updated the writer ${shopName}_${suffix} (writer ID: ${writerId}, row ID: ${rowId}).`)
}
