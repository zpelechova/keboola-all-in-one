/*
 musím si vyrobit vazbu datum + itemId a to mít pro všechny požadovaný dny,
 pokud nebudu mít tenhle pár informací, tak nebudu schopnej dělat gapfilling,
 protože budu mít úplně prázdný řádky, kdy je NULL i v itemId
 */

CREATE OR REPLACE TABLE "allItemIds" AS
SELECT DISTINCT
       "itemId"
FROM "shop_unified"
WHERE to_date("date") >= dateadd('day', -60, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;

/*
    tohle mi vytvoří řadu čísel - je pak použiju v kartézáku s posledním (max) dnem v shop datech, abych
    získal "kalendář", na kterej joinu seznamy itemIds - ((a nedělám to jen na 90 dní, ale 150 => změněno na 60 dní sledování, takže 90 generuji)),
    protože mám itemIds, který nemají na začátku 90 denního okna data
 */
CREATE OR REPLACE TABLE "sekvence" AS
SELECT seq2() + 1 AS "seq"
FROM TABLE (generator(ROWCOUNT => 90)) --počet dní dozadu, co si generuju kalendář
;

CREATE OR REPLACE TABLE "gendate" AS
SELECT DATEADD(DAY, -"seq"."seq", "DAY") :: DATE AS "GENDATE"
FROM (SELECT MAX("date" :: DATE) AS "DAY"
      FROM "shop_unified") "t1"
         LEFT JOIN "sekvence" "seq"
ORDER BY 1
;

/*
    teď dělám tabulku, na kterou pak najoinuju děravý data v alze
 */
CREATE OR REPLACE TABLE "all_dates_items" AS
SELECT *
FROM "gendate"
         LEFT JOIN "allItemIds"
;

/*
    s těmahle datama z alzy budu dělat megajoin :)
 */
CREATE OR REPLACE TABLE "shop_data" AS
SELECT "itemId",
       "currentPrice"::DECIMAL(12,2) as "currentPrice",
       "date"
FROM "shop_unified"
--beru jen 150 dní dozadu, ať ten kartézák na all_dates_items není moc velkej
--150 proto, že někde na začátku -90 dní mohou být díry, tak jdu dozadu
WHERE to_date("date") >= dateadd('day', -90, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;

CREATE TABLE "shop_data_filled_labelled" AS
SELECT "b"."date"                AS "date",
       "b"."itemId"              AS "itemId",
       "b"."merged_currentPrice" AS "currentPrice",
       "filled_row",
       --lag("merged_currentPrice") OVER (PARTITION BY "itemId" ORDER BY "date") AS "prev_price",
       CASE
           WHEN "currentPrice" < lag("merged_currentPrice") OVER (PARTITION BY "itemId" ORDER BY "date")
               THEN 'down'
           WHEN "currentPrice" > lag("merged_currentPrice") OVER (PARTITION BY "itemId" ORDER BY "date")
               THEN 'up'
           WHEN "currentPrice" = lag("merged_currentPrice") OVER (PARTITION BY "itemId" ORDER BY "date")
               THEN 'steady'
           WHEN lag("merged_currentPrice") OVER (PARTITION BY "itemId" ORDER BY "date") IS NULL
               THEN 'price_init'
           ELSE 'err' END        AS "price_trend"
FROM (
         SELECT
             "a"."date",
             "a"."itemId",
             "a"."currentPrice"                                                            AS "original_currentPrice",
             lag("currentPrice") IGNORE NULLS OVER (PARTITION BY "itemId" ORDER BY "date") AS "gapfilled_currentPrice",
             IFF("original_currentPrice" IS NULL, "gapfilled_currentPrice",
                 "original_currentPrice")                                                  AS "merged_currentPrice",
             IFF("original_currentPrice" IS NULL, '0', '1')                                AS "filled_row"
         FROM (
                  SELECT "all_dates_items"."itemId"  AS "itemId",
                         "all_dates_items"."GENDATE" AS "date",
                         "shop_data"."currentPrice"
                  FROM "all_dates_items"
                           LEFT JOIN "shop_data" ON "all_dates_items"."GENDATE" = "shop_data"."date" AND
                                                    "all_dates_items"."itemId" = "shop_data"."itemId"
                  ORDER BY "all_dates_items"."itemId", "all_dates_items"."GENDATE"
              ) "a"
     ) "b"
WHERE to_date("date") >= dateadd('day', -60, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE) AND
      "currentPrice" IS NOT NULL --tímhle oseknu empty rows u věcí, co jsou nové v tom sledovaném 60d okně
;

CREATE TABLE "shop_common_price" AS
SELECT DISTINCT "itemId",
         first_value("currentPrice")
           over (
             PARTITION BY "itemId"
             ORDER BY "samplesCount" DESC) AS "commonPrice"
FROM (SELECT "itemId",
          	 "currentPrice",
          	 count("date") AS "samplesCount"
      FROM   "shop_data_filled_labelled"
      GROUP BY 1,2)
;

CREATE TABLE "shop_last_price_change" AS
SELECT "t0"."date",
       "t0"."itemId",
       "t0"."currentPrice",
       "t0"."price_trend"
FROM (SELECT "date",
             "itemId",
             "currentPrice",
             "price_trend",
             row_number() OVER (PARTITION BY "itemId" ORDER BY "date" DESC) AS "row_number"
      FROM "shop_data_filled_labelled"
      WHERE "price_trend" NOT IN ('steady', 'price_init')
      ORDER BY "itemId", "date") "t0"
WHERE "row_number" = 1
;

CREATE TABLE "shop_last_sale_vs_prev_30d_min_price" AS
SELECT "t0"."date",
       "t0"."itemId",
       "t0"."currentPrice",
       "t0"."price_trend",
       min("t1"."currentPrice") AS "min_currentPrice"
FROM "shop_last_price_change" "t0"
         LEFT JOIN (SELECT *
                    FROM "shop_data_filled_labelled") "t1"
                   ON "t0"."itemId" = "t1"."itemId" AND
                      "t1"."date" < "t0"."date" AND -- jen starší záznamy
                      "t1"."date" >= dateadd('day', -30, "t0"."date") -- ne vic jak 30 dní dozadu
WHERE "t0"."price_trend" = 'down'
GROUP BY 1, 2, 3, 4
;

CREATE TABLE "shop_refprices" AS
SELECT "c"."itemId",
       "commonPrice",
       "min_currentPrice" as "minPrice",
       CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE as "date"
FROM "shop_common_price" "c"
	LEFT JOIN "shop_last_sale_vs_prev_30d_min_price" "eu" ON "c"."itemId" = "eu"."itemId"
;