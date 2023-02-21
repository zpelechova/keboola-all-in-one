/*
 musím si vyrobit vazbu datum + itemId a to mít pro všechny požadovaný dny,
 pokud nebudu mít tenhle pár informací, tak nebudu schopnej dělat gapfilling,
 protože budu mít úplně prázdný řádky, kdy je NULL i v itemId
 */

set discount_valid_days = 90;
--next_querry

set discount_valid_date = dateadd('day', -$discount_valid_days, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE);
--next_querry

set common_price_days = 90;
--next_querry

set common_price_date = dateadd('day', -$common_price_days, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE);
--next_querry

set data_history = $discount_valid_days + 30;
--next_querry

set data_history_date = dateadd('day', -$data_history, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE);
--next_querry

CREATE OR REPLACE TABLE "allItemIds" AS
SELECT DISTINCT
       "itemId"
       , "slug"
FROM "shop_01_unification"
WHERE to_date("date") >= $data_history_date
;
--next_querry

/*
    tohle mi vytvoří řadu čísel - je pak použiju v kartézáku s posledním (max) dnem v shop datech, abych
    získal "kalendář", na kterej joinu seznamy itemIds - ((a nedělám to jen na 90 dní, ale 150 => změněno na 90+30 dní sledování, takže 150+1 generuji)),
    protože mám itemIds, který nemají na začátku 90 denního okna data
 */
CREATE OR REPLACE TABLE "sekvence" AS
select row_number() over (order by seq2()) as "seq"
from table(generator(rowcount => 151))
;
--next_querry

CREATE OR REPLACE TABLE "gendate" AS
SELECT DATEADD(DAY, -("seq"."seq"-1), "DAY") :: DATE AS "GENDATE"
FROM (SELECT MAX("date" :: DATE) AS "DAY"
      FROM "shop_01_unification") "t1"
         LEFT JOIN "sekvence" "seq"
ORDER BY 1
;
--next_querry

/*
    teď dělám tabulku, na kterou pak najoinuju děravý data v alze
 */
CREATE OR REPLACE TABLE "all_dates_items" AS
SELECT *
FROM "gendate"
         LEFT JOIN "allItemIds"
;
--next_querry

/*
    s těmahle datama z alzy budu dělat megajoin :)
 */
CREATE OR REPLACE TABLE "shop_data" AS
SELECT "itemId",
			 "slug",
       "currentPrice"::DECIMAL(12,2) as "currentPrice",
       "originalPrice",
       "date",
       case
        when "originalPrice" != ''
            and lag("originalPrice") over (partition by "itemId", "slug" order by "date" asc) = ''
        then TRUE
       end as "first_day_origPrice"
FROM "shop_01_unification"
WHERE to_date("date") >= $data_history_date
    and "currentPrice" != ''
;
--next_querry

CREATE OR REPLACE TABLE "shop_data_filled_labelled" AS
SELECT "b"."date"                AS "date",
       "b"."itemId"              AS "itemId",
       "b"."slug"                AS "slug",
       "b"."merged_currentPrice" AS "currentPrice",
       "filled_row",
       --lag("merged_currentPrice") OVER (PARTITION BY "itemId" ORDER BY "date") AS "prev_price",
       CASE
           WHEN "currentPrice" < lag("merged_currentPrice") OVER (PARTITION BY "itemId", "slug" ORDER BY "date")
               THEN 'down'
           WHEN "currentPrice" > lag("merged_currentPrice") OVER (PARTITION BY "itemId", "slug" ORDER BY "date")
               THEN 'up'
           WHEN "currentPrice" = lag("merged_currentPrice") OVER (PARTITION BY "itemId", "slug" ORDER BY "date")
               THEN 'steady'
           WHEN lag("merged_currentPrice") OVER (PARTITION BY "itemId", "slug" ORDER BY "date") IS NULL
               THEN 'price_init'
           ELSE 'err' END        AS "price_trend"
FROM (
         SELECT
             "a"."date",
             "a"."itemId",
             "a"."slug",
             "a"."currentPrice"                                                            AS "original_currentPrice",
             lag("currentPrice") IGNORE NULLS OVER (PARTITION BY "itemId", "slug" ORDER BY "date") AS "gapfilled_currentPrice",
             IFF("original_currentPrice" IS NULL, "gapfilled_currentPrice",
                 "original_currentPrice")                                                  AS "merged_currentPrice",
             IFF("original_currentPrice" IS NULL, '0', '1')                                AS "filled_row"
         FROM (
                  SELECT "all_dates_items"."itemId"  AS "itemId",
           							 "all_dates_items"."slug"  AS "slug",
                         "all_dates_items"."GENDATE" AS "date",
                         "shop_data"."currentPrice"
                  FROM "all_dates_items"
                           LEFT JOIN "shop_data" ON "all_dates_items"."GENDATE" = "shop_data"."date" AND
                                                    "all_dates_items"."itemId" = "shop_data"."itemId" AND
           																					"all_dates_items"."slug" = "shop_data"."slug"
                  --ORDER BY "all_dates_items"."itemId", "all_dates_items"."GENDATE"
              ) "a"
     ) "b"
WHERE to_date("date") >= $data_history_date AND
      "currentPrice" IS NOT NULL --tímhle oseknu empty rows u věcí, co jsou nové v tom sledovaném 60d okně
;
--next_querry

CREATE OR REPLACE TABLE "shop_common_price" AS
SELECT DISTINCT "itemId",
					"slug",
         first_value("currentPrice")
           over (
             PARTITION BY "itemId", "slug"
             ORDER BY "samplesCount" DESC) AS "commonPrice"
FROM (SELECT "itemId",
      			 "slug",
          	 "currentPrice",
          	 count("date") AS "samplesCount"
      FROM   "shop_data_filled_labelled"
      WHERE "date" >= $common_price_date
      GROUP BY 1,2, 3)
;
--next_querry

CREATE OR REPLACE TABLE "shop_price_change" AS
SELECT "t0"."date",
       "t0"."itemId",
       "t0"."slug",
       "t0"."currentPrice",
       "t0"."price_trend",
       "t0"."row_number",
       "first_day_origPrice"
FROM (SELECT "date",
             "itemId",
      			 "slug",
             "currentPrice",
             "price_trend",
             row_number() OVER (PARTITION BY "itemId", "slug" ORDER BY "date" DESC) AS "row_number"
      FROM "shop_data_filled_labelled"
      WHERE "price_trend" NOT IN ('steady', 'price_init')
        and "date" >= $discount_valid_date
      --ORDER BY "itemId", "date"
      ) "t0"
left join "shop_data"
    on "shop_data"."itemId" = "t0"."itemId"
    and "shop_data"."slug" = "t0"."slug"
    and "shop_data"."date" = "t0"."date"
--WHERE "row_number" = 1

;
--next_querry

CREATE OR REPLACE TABLE "shop_last_valid_price_change" AS
with "up" as (
    select "itemId" as "itemId"
  			 , "slug"
         , "date"
    from "shop_price_change"
    where "price_trend" = 'up'
)
, "up_max_date" as (
    select "itemId"
  			 , "slug"
         , max("date") as "date"
    from "up"
    group by "itemId", "slug"
)
, "shop_price_change_from_last_up" as (
    (select "shop_price_change".*
    from "shop_price_change"
    left join "up_max_date"
        on "shop_price_change"."itemId" = "up_max_date"."itemId"
      	and "shop_price_change"."slug" = "up_max_date"."slug"
    where "shop_price_change"."itemId" in (select "itemId" from "up_max_date")
      	and "shop_price_change"."slug" in (select "slug" from "up_max_date")
        and try_to_date("shop_price_change"."date") > try_to_date("up_max_date"."date"))
    union
    (select "shop_price_change".*
    from "shop_price_change"
    where "shop_price_change"."itemId" not in (select "itemId" from "up_max_date")
     and "shop_price_change"."slug" not in (select "slug" from "up_max_date"))
)
, "down_new_origPrice" as (
    select distinct("itemId") as "itemId"
    		, "slug"
        , (max("row_number") over (partition by "itemId", "slug")) + 1 as "max_row_down_plus1_origPrice"
    from "shop_price_change_from_last_up"
    where "first_day_origPrice" = true
)
, "down" as (
    select distinct("itemId") as "itemId"
    		, "slug"
        , (max("row_number") over (partition by "itemId", "slug")) + 1 as "max_row_down_plus1"
    from "shop_price_change_from_last_up"
)
, "all" as (
    SELECT DISTINCT("all"."itemId") as "itemId"
                          , "all"."slug"
                          , "all"."date"
                          , "all"."currentPrice"
                          , "all"."price_trend"
                          , "all"."row_number"
                          , LEAST("max_row_down_plus1", "max_row_down_plus1_origPrice") AS "break_row"
            FROM "shop_price_change" "all"
                     LEFT JOIN "down_new_origPrice"
                               ON "all"."itemId" = "down_new_origPrice"."itemId"
                               	and "all"."slug" = "down_new_origPrice"."slug"
                     LEFT JOIN "down"
                               ON "all"."itemId" = "down"."itemId"
                               	and "all"."slug" = "down"."slug"
            QUALIFY "all"."date" >= $discount_valid_date
                AND ("row_number" = (TO_NUMBER("break_row") - 1) OR "row_number" = 1)
)
select *
from "all"
QUALIFY "row_number" = max("row_number") over (partition by "itemId", "slug")
;
--next_querry

CREATE or replace TABLE "shop_last_sale_vs_prev_30d_min_price" AS
SELECT "t0"."date",
       "t0"."itemId",
       "t0"."slug",
       "t0"."currentPrice",
       "t0"."price_trend",
       min("t1"."currentPrice") AS "min_currentPrice"
FROM "shop_last_valid_price_change" "t0"
         LEFT JOIN (SELECT *
                    FROM "shop_data_filled_labelled") "t1"
                   ON "t0"."itemId" = "t1"."itemId" AND "t0"."slug" = "t1"."slug" AND
                      "t1"."date" < "t0"."date" AND -- jen starší záznamy
                      "t1"."date" >= dateadd('day', -30, "t0"."date") -- ne vic jak 30 dní dozadu (dle směrnice)
WHERE "t0"."price_trend" = 'down'-- and "t0"."date" >= dateadd('day', -30, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE) -- toto si vyřeším o query dřív
GROUP BY 1, 2, 3, 4, 5
;
--next_querry

CREATE OR REPLACE TABLE "shop_02_refprices_new" AS
SELECT "c"."itemId",
       "c"."slug",
       "commonPrice",
       "min_currentPrice" as "minPrice",
       CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE as "date"
FROM "shop_common_price" "c"
	LEFT JOIN "shop_last_sale_vs_prev_30d_min_price" "eu"
      ON "c"."itemId" = "eu"."itemId"
      	and "c"."slug" = "eu"."slug"
--order by "itemId" desc
;