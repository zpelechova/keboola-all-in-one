import Apify from 'apify'
import fs from 'fs'
import { config } from 'dotenv'
import * as stor from './src/storage.js'
import * as trans from './src/transformation.js'
import * as orch from './src/orchestration.js'
import * as wr from './src/writer.js'
config()

const LATEST = 'LATEST'

Apify.main(async () => {
    console.log(process.env.KEBOOLA_TOKEN)
    console.log(process.env.SLACK_TOKEN)
    console.log(process.env.AWS_TOKEN)
    const input = await Apify.getInput()
    console.log(input)

    const date = new Date()
    const todaysDate = date.toISOString().substring(0, 10)

    const shopNames = input.shopNames.map(name => name.toLowerCase())
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
    const slackChannel = input.slackChannel

    for (const shopName of shopNames) {
        const transformationIds = []
        const writerIds = []
        const rowIds = []

      if (migrateTables) {
            //TODO adding clean table?
            const code = [
                `alter table "shop_w" drop column "_timestamp";`,
                `create table "shop_unified" as select * from "shop_w" limit 100;`,
                `create table "shop_refprices" as select * from "shop_new";`
            ]

            await trans.updateTransformation(
                367214386,
                'This transformation migrates data from old to new Keboola',
                [
                    `out.c-0-${shopName}.${shopName}_w`,
                    `out.c-0-${shopName}.${shopName}_new`
                ],
                ['shop_w', 'shop_new'],
                [`shop_unified`, 'shop_refprices'],
                [
                    `out.c-${shopName}.${shopName}_unified`,
                    `out.c-${shopName}.${shopName}_refprices`
                ],
                [['itemId', 'date']],
                [true, true]`Codeblock - MIGRATION`,
                `MIGRATION`,
                code
            )

            await trans.migrate()
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
                '05_pricehistory',
                '06_s3format'
                //doplnit 07_dashboard
                //'00_preparation' // only for shops with feed and/or unitPrice items
            ]

            for (const transformation of transformations) {
                const index = transformations.indexOf(transformation)

                const inputTablesSource = [
                    [`in.c-black-friday.${shopName}`],
                    [`out.c-${shopName}.${shopName}_01_unification`],
                    [
                        `out.c-${shopName}.${shopName}_01_unification`,
                        `out.c-${shopName}.${shopName}_02_refprices`
                    ],
                    [`out.c-${shopName}.${shopName}_03_complete`],
                    [`out.c-${shopName}.${shopName}_03_complete`],
                    [
                        `out.c-${shopName}.${shopName}_03_complete`,
                        `out.c-${shopName}.${shopName}_04_extension`,
                        `out.c-${shopName}.${shopName}_05_final_s3`
                    ]
                ]

                const inputTablesName = [
                    ['shop_raw'],
                    ['shop_01_unification'],
                    ['shop_01_unification', 'shop_02_refprices'],
                    ['shop_03_complete'],
                    ['shop_03_complete'],
                    ['shop_03_complete', 'shop_04_extension', 'shop_05_final_s3']
                ]

                const outputTablesName = [
                    [`shop_${transformation}`],
                    [`shop_${transformation}`],
                    [`shop_${transformation}`],
                    [`shop_${transformation}`],
                    [`shop_${transformation}`, `shop_05_final_s3`],
                    [`shop_s3_metadata`, `shop_s3_pricehistory`]
                ]

                const outputTablesSource = [
                    [`out.c-${shopName}.${shopName}_${transformation}`],
                    [`out.c-${shopName}.${shopName}_${transformation}`],
                    [`out.c-${shopName}.${shopName}_${transformation}`],
                    [`out.c-${shopName}.${shopName}_${transformation}`],
                    [
                      `out.c-${shopName}.${shopName}_${transformation}`,
                      `out.c-${shopName}.${shopName}_05_final_s3`
                    ],
                    [
                        `out.c-${shopName}.${shopName}_s3_metadata`,
                        `out.c-${shopName}.${shopName}_s3_pricehistory`
                    ]
                ]

                const outputTablesKeys = [
                    [['itemId', 'date']],
                    [['itemId', 'date']],
                    [['itemId', 'date']],
                    [['pkey']],
                    [['p_key']],
                    [['slug'], ['slug']]
                ]

                const outputIncremental = [
                    [true],
                    [true],
                    [true],
                    [false],
                    [false],
                    [false, false]
                ]
                const transformationId = await trans.getOrCreateTransformation(
                    shopName,
                    transformation
                )
                //I am creating an array of transformation Ids to be used in orchestrations later on
                transformationIds.push(transformationId)

                //I am transforming sql code to array
                const sqlCode = [];
                const sqls = fs
                    .readFileSync(`./src/texts/${transformation}.sql`,'utf-8')
                    .toString()
                    .split('--next_querry')
                for (let sql of sqls) {
                    if (sql != '') {
                        sql = sql.trim()
                        sqlCode.push(sql);
                    }
                }

                await trans.updateTransformation(
                    transformationId,
                    fs.readFileSync(
                        `./src/texts/${transformation}_descr.txt`,
                        'utf-8'
                    ),
                    inputTablesSource[index], //in-table source
                    inputTablesName[index], //in-table alias
                    outputTablesName[index], //out-table alias
                    outputTablesSource[index], //out-table source
                    outputTablesKeys[index], //out-table primary keys
                    outputIncremental[index], //out-table incremental?
                    `Codeblock - ${transformation}`,
                    `Shop ${transformation}`,
                    sqlCode
                )
            }
        }

      if (runWriter) {
            console.log(`Starting Writer management program`)

            const writers = ['s3_metadata', 's3_pricehistory']

            for (const writer of writers) {
                const writerId = await wr.getOrCreateWriter(shopName, writer)
                writerIds.push(writerId)
                const rowId = await wr.getOrCreateTableRow(shopName, writer)
                rowIds.push(rowId)
                console.log('Writer ID is ' + writerId + ', row ID is' + rowId)
                await wr.updateWriter(shopName, writer, writerId, rowId)
            }
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
                transformationIds,
                writerIds
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
            //TODO The folloowing line errors when an incorrect name is provided - also case sensitive still
            const orchestrationLastTimeStart =
                orchestrationInfo.lastExecutedJob.startTime

            if (orchestrationLastTimeStart.substring(0, 10) === todaysDate) {
                console.log(`The run for ${shopName} has run today.`)
            } else {
                //TODO notifications at the very end of the actor run, so that we dont have hunders of them
                if (notifyBySlack) {
                    console.log(`Sending notification to Slack...`)
                    await Apify.call('katerinahronik/slack-message', {
                        text: `The orchestration for ${shopName} has not run yet, check what is wrong!`,
                        channel: slackChannel,
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
    }

    if (testStorage) {
        console.log(`Starting Storage checking program`)
        const tablesRawData = await stor.getTables()

        const kvStore = await Apify.openKeyValueStore('outputTables')

        // Makes a dataset from given shops
        // TODO Save to named KVS store and compare to yesterdays event
        const tablesData = []
        for (const shopName of shopNames) {
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
                    tableData.lastChange = table.lastChangeDate.substring(0, 10)
                    if (!tablesData.includes(tableData)) {
                        tablesData.push(tableData)
                    }
                }
            }
        }

        await Apify.pushData(tablesData)

        let latestData = await kvStore.getValue(LATEST)
        if (!latestData) {
            await kvStore.setValue('LATEST', tablesData)
            latestData = tablesData
        }

        const differences = []

        for (const table of tablesData) {
            let shopInLatest = false
            for (const latest of latestData) {
                if (table.name === latest.name && table.id === latest.id) {
                    shopInLatest = true

                    if (table.lastChange != todaysDate) {
                        console.log(
                            `We are missing todays data for ${table.name}!`
                        )
                    } else if (latest.lastChange != table.lastChange) {
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

                        if (dailyChange.diff < 1000) {
                            //TODO set the difference correctly for each shop and set notifications
                            console.log(
                                `Hey, there is some problem with ${dailyChange.tableName}, got really very few clean items today.`
                            )
                        }
                    } else {
                        console.log(
                            `It seems ${table.name} has already been checked today, skipping it.`
                        )
                    }
                }
            }
            if (!shopInLatest) {
                console.log(`You are missing yesterday data for ${table.name}`)
            }
        }

        await kvStore.setValue('LATEST', tablesData)
        await Apify.setValue('DIFFERENCES', differences)
        await Apify.setValue('TABLES', tablesData)

        console.log('Done.')
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

    console.log('All required tasks have been finished.')
    //here will go all the notifications
})
