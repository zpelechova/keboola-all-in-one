import Apify from 'apify'
import fs from 'fs'
import { config } from 'dotenv'
import * as stor from './src/storage.js'
import * as trans from './src/transformation.js'
import * as orch from './src/orchestration.js'
config()

const LATEST = 'LATEST'

Apify.main(async () => {
    console.log(process.env.KEBOOLA_TOKEN)
    const input = await Apify.getInput()
    console.log(input)

    const shopNames = input.shopNames.map(name => name.toLowerCase());
    const email = input.email
    const runStorage = input.runStorage
    const runTransformation = input.runTransformation
    const runWriter = input.runWriter
    const runOrchestration = input.runOrchestration
    const testOrchestration = input.testOrchestration
    const testStorage = input.testStorage
    const getStorage = input.getStorage
    const notifyByMail = input.notifyByMail
    const notifyBySlack = input.notifyBySlack
    const migrateTables = input.migrateTables

    for (const shopName of shopNames) {
        const transformationIds = []
        const writerIds = []


        if (migrateTables) {
            const code = ['alter table "shop_w" drop column "_timestamp";','create table "shop_unified" as\nselect *\nfrom "shop_w"\nlimit 100;'];
            
            await trans.updateTransformation(
                367214386,
                'This transformation migrates data from old to new Keboola',
                [`out.c-0-${shopName}.${shopName}_w`],
                ['shop_w'],
                [`shop_unified`],
                [`out.c-${shopName}.${shopName}_unified`],
                [['itemId', 'date']],
                `Codeblock - MIGRATION`,
                `MIGRATION`,
                code
            )

            await trans.migrate();

        }

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

            const inputTables = [
                [`in.c-black-friday.${shopName}`],
                [`out.c-${shopName}.${shopName}`],
                [`in.c-black-friday.${shopName}`,`out.c-${shopName}.${shopName}`],            ]

            for (const transformation of transformations) {
                const index = transformations.indexOf(transformation);
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
                    inputTables[index],
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

        //TODO It nows saves only the last shop to KVS, I think I have to move it outside the forcycle and assign it a forcycle of its own
        if (testStorage) {
            console.log(`Starting Storage checking program`)
            const tablesRawData = await stor.getTables()

            const kvStore = await Apify.openKeyValueStore('outputTables')

            // Makes a dataset from given shops
            // TODO Save to named KVS store and compare to yesterdays event
            const tablesData = []
            for (const table of tablesRawData) {
                if (
                    `${shopName.toLowerCase()}_clean` === table.name &&
                    table.id.startsWith('out.c-0')
                ) {
                    const tableData = {}
                    tableData.name = table.name
                    tableData.displayName = table.displayName
                    tableData.rowsCount = table.rowsCount
                    tableData.bucket = table.bucket.id
                    tableData.id = table.id
                    if (!tablesData.includes(tableData)) {
                        tablesData.push(tableData)
                    }
                }
            }

            await Apify.pushData(tablesData)

            let latestData = await kvStore.getValue(LATEST)
            if (!latestData) {
                await kvStore.setValue('LATEST', tablesData)
                latestData = tablesData
            }
            const actual = Object.assign({}, tablesData)

            const differences = []

            for (const table of tablesData) {
                for (const latest of latestData) {
                    if (table.name === latest.name && table.id === latest.id) {
                        const yesterday = latest.rowsCount
                        const today = table.rowsCount
                        const diff = yesterday - today
                        //TODO add last change date - if the date from Keboola is not todays date, send 1 notification, then compare dates latest/actual and if the are the same, do nothing, if they are different compare rowsCount
                        const dailyChange = {
                            tableName: table.name,
                            tableId: table.id,
                            yesterday,
                            today,
                            diff
                        }
                        if (!differences.includes(dailyChange)) {
                            differences.push(dailyChange)
                        }
                        //TODO put it elswhere (now it writes manytimes for each shop - and have it really just once for all shops, so maybe a new for cycle reading from differences)
                        if (dailyChange.diff < 1000) {
                            //TODO set the difference correctly for each shop and set notifications
                            console.log(
                                `Hey, there is some problem with ${shopName}, got really very few clean items today.`
                            )
                        }
                    }
                }
            }

            await kvStore.setValue('LATEST', tablesData)
            await Apify.setValue('DIFFERENCES', differences)
            await Apify.setValue('TABLES', tablesData)

            console.log('Done.')
        }
    }

    if (getStorage) {
        console.log(`Starting Storage downloading program`)
        const tablesData = await stor.getTables()

        //makes dataset from all shops
        for (const table of tablesData) {
            const tableData = {
                name: table.name,
                displayName: table.displayName,
                rowsCount: table.rowsCount,
                bucket: table.bucket.id,
                id: table.id,
                lastChange: table.lastChangeDate
            }
            await Apify.pushData(tableData)
        }
        console.log(`All data has been saved to dataset, saving to KVS now.`)
        await Apify.setValue('AllTables', tablesData)
        console.log(`Storage downloading program has finished.`)
    }
})
