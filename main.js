import Apify from 'apify'
import fs from 'fs'
import { config } from 'dotenv'
import * as stor from './src/storage.js'
import * as trans from './src/transformation.js'
import * as orch from './src/orchestration.js'
config()

const shopNames = ['aaaaa']
const email = "martina@apify.com, zuzka@apify.com"

let runStorage = true;
let runTransformation = true;
let runWriter = false;
let runOrchestration = true;

Apify.main(async () => {
    console.log(process.env.KEBOOLA_TOKEN)

    for (const shopName of shopNames) {

        const transformationIds = [];
        const writerIds = [];

        if (runStorage) {
            //It checks if the in table already exists and if not, creates it. it also returns the data about the table, but we dont need it for anything at the moment I think
            console.log(`Starting Storage management program`);
            await stor.getOrCreateTable(shopName);
        };

        if (runTransformation) {
            console.log(`Starting Transformation management program`);

            const transformations = [
                '01_unification',
                '02_refprices',
                '03_complete',
                '04_extension',
                '05_pricehistory'
            ]

            for (const transformation of transformations) {
                const transformationId = await trans.getOrCreateTransformation(
                    shopName,
                    transformation
                );
                //I am creating an array of transformation Ids to be used in orchestrations later on
                transformationIds.push(transformationId);

                await trans.updateTransformation(
                    transformationId,
                    fs.readFileSync(`./src/texts/${transformation}_descr.txt`, 'utf-8'),
                    [`in.c-black-friday.${shopName}`],
                    ['shop_raw'],
                    [`shop_${transformation}`],
                    [`out.c-${shopName}.${shopName}_${transformation}`],
                    [['itemId', 'date']],
                    `Codeblock - ${transformation}`,
                    `Shop ${transformation}`,
                    fs.readFileSync(`./src/texts/${transformation}.sql`, 'utf-8')
                )
            }
        }

        if (runWriter) {
            console.log(`Starting Writer management program`)
        }

        if (runOrchestration) {
            console.log(`Starting Orchestration management program`)
            const orchestrationInfo = await orch.getOrCreateOrchestration(
                shopName
            )

            const orchestrationId = orchestrationInfo.id
            const orchestrationTokenId = orchestrationInfo.token.id

            await orch.updateOrchestrationTasks(
                shopName,
                orchestrationId,
                transformationIds
            )

            await orch.updateOrchestrationNotifications(
                shopName,
                orchestrationId,
                email
            )

            await orch.updateOrchestrationTriggers(
                shopName,
                orchestrationId,
                orchestrationTokenId
            )
        }
    }
})
