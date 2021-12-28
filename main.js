import Apify from 'apify'
import fs from 'fs'
import { config } from 'dotenv'
import * as stor from './src/storage.js'
import * as trans from './src/transformation.js'
import * as orch from './src/orchestration.js'
config()

const shopName = 'ccc'
const transformationId = '346265961'

let runStorage = true;
let runTransformation = false;
let runWriter = false;
let runOrchestration = false;

Apify.main(async () => {
    console.log(process.env.KEBOOLA_TOKEN)
    if (runStorage) {
        console.log(`Starting Orchestration management program`);
        await stor.getOrCreateTable(shopName)
    }

    if (runTransformation) {
        console.log(`Starting Transformation management program`)
    }

    if (runWriter) {
        console.log(`Starting Writer management program`)
    }

    if (runOrchestration) {

        console.log(`Starting Orchestration management program`)
        const orchestrationInfo = await orch.getOrCreateOrchestration(shopName)

        const orchestrationId = orchestrationInfo.id
        const orchestrationTokenId = orchestrationInfo.token.id

        await orch.updateOrchestrationTasks(shopName, orchestrationId, transformationId)

        await orch.updateOrchestrationNotifications(shopName, orchestrationId)

        await orch.updateOrchestrationTriggers(shopName, orchestrationId, orchestrationTokenId)
    }
})
