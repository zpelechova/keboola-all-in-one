import Apify from 'apify';
import fs from 'fs';
import { config } from 'dotenv';
import * as lib from './lib.js';
config();
  
const shopName = 'ccc';
const transformationId = "346265961"


Apify.main(async () => {
    console.log(process.env.KEBOOLA_TOKEN);

    const orchestrationInfo = await lib.getOrCreateOrchestration(shopName);

    const orchestrationId = orchestrationInfo.id;
    const orchestrationTokenId = orchestrationInfo.token.id;

    await lib.updateOrchestrationTasks(orchestrationId, transformationId);

    await lib.updateOrchestrationNotifications(orchestrationId);

    await lib.updateOrchestrationTriggers(orchestrationId, orchestrationTokenId);

})
