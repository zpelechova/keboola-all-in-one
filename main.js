import Apify from 'apify'
import fs from 'fs'
import { config } from 'dotenv'
import * as stor from './src/storage.js'
import * as trans from './src/transformation.js'
import * as orch from './src/orchestration.js'
config()

Apify.main(async () => {
    console.log(process.env.KEBOOLA_TOKEN)
    const input = await Apify.getInput()
    console.log(input)

    const shopNames = input.shopNames
    const email = input.email
    const runStorage = input.runStorage
    const runTransformation = input.runTransformation
    const runWriter = input.runWriter
    const runOrchestration = input.runOrchestration
    const testOrchestration = input.testOrchestration
    const testStorage = input.testStorage
    const notifyByMail = input.notifyByMail
    const notifyBySlack = input.notifyBySlack

    for (const shopName of shopNames) {
        const transformationIds = []
        const writerIds = []

        if (runStorage) {
            //It checks if the in table already exists and if not, creates it. it also returns the data about the table, but we dont need it for anything at the moment I think
            console.log(`Starting Storage management program`)
            await stor.getOrCreateTable(shopName)
        }

        if (runTransformation) {
            console.log(`Starting Transformation management program`)

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
                )
                //I am creating an array of transformation Ids to be used in orchestrations later on
                transformationIds.push(transformationId)

                await trans.updateTransformation(
                    transformationId,
                    fs.readFileSync(
                        `./src/texts/${transformation}_descr.txt`,
                        'utf-8'
                    ),
                    [`in.c-black-friday.${shopName}`],
                    ['shop_raw'],
                    [`shop_${transformation}`],
                    [`out.c-${shopName}.${shopName}_${transformation}`],
                    [['itemId', 'date']],
                    `Codeblock - ${transformation}`,
                    `Shop ${transformation}`,
                    fs.readFileSync(
                        `./src/texts/${transformation}.sql`,
                        'utf-8'
                    )
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

        if (testOrchestration) {
            console.log(`Starting Orchestration test program`)
            const orchestrationInfo = await orch.getOrCreateOrchestration(
                shopName
            )

            const today = new Date().toISOString()

            const orchestrationLastTimeStart =
                orchestrationInfo.lastExecutedJob.startTime
            const orchestrationLastTimeEnd =
                orchestrationInfo.lastExecutedJob.endTime
            const orchestrationLastStatus =
                orchestrationInfo.lastExecutedJob.status

            if (
                orchestrationLastTimeStart.substring(0, 10) ===
                today.substring(0, 10)
            ) {
                console.log(`The run for ${shopName} has run today.`)
            } else {
                if (notifyBySlack) {
                    console.log(`Sending notification to Slack...`)
                    await Apify.call('katerinahronik/slack-message', {
                        text: 'This orchestration has not run today',
                        channel: '#monitoring-blackfriday',
                        token: process.env.SLACK_TOKEN
                    })
                    console.log('Email sent. Good luck!')
                }
                if (notifyByMail) {
                    console.log(`Sending email to ${email}...`)
                    await Apify.call('apify/send-mail', {
                        to: email,
                        subject: `The orchestration for ${shopName} has not run yet, check what is wrong!`,
                        html: `<h1>Check it please </h1>`
                    })
                    console.log('Email sent. Good luck!')
                }
            }
        }

        if (testStorage) {
            console.log(`Starting Storage checking program`)
            const tablesData = await stor.checkTable()

            //Makes a dataset from given shops
            //TODO Save to named KVS store and compare to yesterdays event
            // for (const shop of shopNames) {
            //     for (const table of tablesData) {
            //         const tableData = {};
            //         if (shop.toLowerCase() === table.name) {
            //             tableData.name = table.name;
            //             tableData.displayName = table.displayName;
            //             tableData.rowsCount = table.rowsCount;
            //             tableData.bucket = table.bucket.id;
            //             tableData.id = table.id;
            //             await Apify.pushData(tableData);
            //         }
            //     }
            // }

            //makes dataset from all shops
            for (const table of tablesData) {
                const tableData = {
                    "name": table.name,
                    "displayName": table.displayName,
                    "rowsCount": table.rowsCount,
                    "bucket": table.bucket.id,
                    "id": table.id
                }
                await Apify.pushData(tableData)
            }
        }
    }
})
