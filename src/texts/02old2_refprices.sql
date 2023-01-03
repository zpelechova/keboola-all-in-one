/*
 musím si vyrobit vazbu datum + itemId a to mít pro všechny požadovaný dny,
 pokud nebudu mít tenhle pár informací, tak nebudu schopnej dělat gapfilling,
 protože budu mít úplně prázdný řádky, kdy je NULL i v itemId
 */
set discount_valid_days = 90;
set discount_valid_date = dateadd('day', -$discount_valid_days, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE);
set common_price_days = 90;
set common_price_date = dateadd('day', -$common_price_days, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE);
set data_history = $discount_valid_days + 30;
set data_history_date = dateadd('day', -$data_history, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE);

CREATE OR REPLACE TABLE "allItemIds" AS
SELECT DISTINCT
       "itemId"
FROM "shop_01_unification"
WHERE to_date("date") >= $data_history_date
;

/*
    tohle mi vytvoří řadu čísel - je pak použiju v kartézáku s posledním (max) dnem v shop datech, abych
    získal "kalendář", na kterej joinu seznamy itemIds - ((a nedělám to jen na 90 dní, ale 150 => změněno na 90+30 dní sledování, takže 150+1 generuji)),
    protože mám itemIds, který nemají na začátku 90 denního okna data
 */
CREATE OR REPLACE TABLE "sekvence" AS
select row_number() over (order by seq2()) as "seq"
from table(generator(rowcount => 151))
;

CREATE OR REPLACE TABLE "gendate" AS
SELECT DATEADD(DAY, -("seq"."seq"-1), "DAY") :: DATE AS "GENDATE"
FROM (SELECT MAX("date" :: DATE) AS "DAY"
      FROM "shop_01_unification") "t1"
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
FROM "shop_01_unification"
WHERE to_date("date") >= $data_history_date
    and "currentPrice" != ''
;

CREATE OR REPLACE TABLE "shop_data_filled_labelled" AS
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
WHERE to_date("date") >= $data_history_date AND
      "currentPrice" IS NOT NULL --tímhle oseknu empty rows u věcí, co jsou nové v tom sledovaném 60d okně
;

CREATE OR REPLACE TABLE "shop_common_price" AS
SELECT DISTINCT "itemId",
         first_value("currentPrice")
           over (
             PARTITION BY "itemId"
             ORDER BY "samplesCount" DESC) AS "commonPrice"
FROM (SELECT "itemId",
          	 "currentPrice",
          	 count("date") AS "samplesCount"
      FROM   "shop_data_filled_labelled"
      WHERE "date" >= $common_price_date
      GROUP BY 1,2)
;

CREATE OR REPLACE TABLE "shop_price_change" AS
SELECT "t0"."date",
       "t0"."itemId",
       "t0"."currentPrice",
       "t0"."price_trend"
       , "t0"."row_number"
FROM (SELECT "date",
             "itemId",
             "currentPrice",
             "price_trend",
             row_number() OVER (PARTITION BY "itemId" ORDER BY "date" DESC) AS "row_number"
      FROM "shop_data_filled_labelled"
      WHERE "price_trend" NOT IN ('steady', 'price_init')
        and "date" >= $discount_valid_date
      ORDER BY "itemId", "date") "t0"
--WHERE "row_number" = 1
;

CREATE OR REPLACE TABLE "shop_last_valid_price_change" AS
select *
from
(select distinct("all"."itemId")
    , "all"."date"
    , "all"."currentPrice"
    , "all"."price_trend"
    , "all"."row_number"
    , coalesce("up"."min_row_up", "down"."max_row_down_plus1") as "break_row"
from "shop_price_change" "all"
left join (
    select "itemId" as "itemId"
        , min("row_number") over (partition by "itemId") as "min_row_up"
    from "shop_price_change"
    where "price_trend" = 'up'
) "up"
on "all"."itemId" = "up"."itemId"
left join (
    select distinct("itemId") as "itemId"
        , (max("row_number") over (partition by "itemId")) + 1 as "max_row_down_plus1"
    from "shop_price_change"
) "down"
on "all"."itemId" = "down"."itemId"
having "all"."date" >= $discount_valid_date
    and ("row_number" = (to_number("break_row") -1)  or "row_number" = 1)
    )
qualify "row_number" = max("row_number") over (partition by "itemId")
;

CREATE or replace TABLE "shop_last_sale_vs_prev_30d_min_price" AS
SELECT "t0"."date",
       "t0"."itemId",
       "t0"."currentPrice",
       "t0"."price_trend",
       min("t1"."currentPrice") AS "min_currentPrice"
FROM "shop_last_valid_price_change" "t0"
         LEFT JOIN (SELECT *
                    FROM "shop_data_filled_labelled") "t1"
                   ON "t0"."itemId" = "t1"."itemId" AND
                      "t1"."date" < "t0"."date" AND -- jen starší záznamy
                      "t1"."date" >= dateadd('day', -30, "t0"."date") -- ne vic jak 30 dní dozadu (dle směrnice)
WHERE "t0"."price_trend" = 'down'-- and "t0"."date" >= dateadd('day', -30, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE) -- toto si vyřeším o query dřív
GROUP BY 1, 2, 3, 4
;

CREATE OR REPLACE TABLE "shop_02_refprices" AS
SELECT "c"."itemId",
       "commonPrice",
       "min_currentPrice" as "minPrice",
       CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE as "date"
FROM "shop_common_price" "c"
	LEFT JOIN "shop_last_sale_vs_prev_30d_min_price" "eu" ON "c"."itemId" = "eu"."itemId"
order by "itemId" desc
;