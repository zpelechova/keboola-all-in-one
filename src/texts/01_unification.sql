set ref_date = DATEADD('day', - {{days_back}}, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;
--next_querry
--takes only records from last 2 days from both input tables - the number of days can be easily modified or omitted altogether
CREATE TABLE "shop" as
SELECT *
	FROM "shop_raw"
	WHERE LEFT("_timestamp", 10) >= $ref_date
;
--next_querry
--takes main table, sets minimum number for current price and maximum for original price, converts date to date format, leaves out rows with no itemId or price or date, leaves out extra information
--creates two md5 hashes - p_key is unique for item and consists of shop and itemId, pk is unique for item a date and is made from p_key, - , date
CREATE TABLE "shop_01_unification"
AS
SELECT DISTINCT "shop" AS "shop"
	,md5("shop" || "itemId") AS "p_key"
	,"itemId" AS "itemId"
	,"itemName" AS "itemName"
	,"itemUrl" AS "itemUrl"
  , "slug" AS "slug"
	, min(try_to_number("currentPrice", 12, 2)) OVER (
				PARTITION BY "itemId"
				, "date"
				) AS "currentPrice"
	,case
    when max(try_to_number("originalPrice", 12, 2)) OVER (PARTITION BY "itemId", to_date("date")) <= try_to_number("currentPrice", 12, 2) then null
    else max(try_to_number("originalPrice", 12, 2)) OVER (PARTITION BY "itemId", to_date("date"))
   end AS "originalPrice"
  ,round(((try_to_number("currentPrice") / nullifzero(try_to_number("originalPrice")) - 1) * -100), 2) AS "officialSale"
	,"img" AS "itemImage"
	,to_date("date") AS "date"
  ,"inStock"
FROM "shop"
WHERE "currentPrice" <> ''
AND "date" <> ''
AND "itemId" <> ''
;
--next_querry

CREATE OR REPLACE TABLE "item_count_check" AS
WITH "shop_origin" AS (
  SELECT "shopOrigin"
  FROM "shop"
  GROUP BY ALL
)

SELECT "shop_origin"."shopOrigin"
    , "date"::DATE  																																													AS "date"
    , ZEROIFNULL(COUNT(DISTINCT "itemId"))																																		AS "count_itemId_with_curPrice"
    , ZEROIFNULL(COUNT(DISTINCT(IFF("originalPrice" is not null AND "originalPrice" != '', "itemId", NULL)))) AS "count_itemId_with_origPrice"
FROM "shop_origin"
LEFT JOIN "shop_raw" ON 1=1
--LEFT JOIN "shop" ON 1=1
WHERE "currentPrice" <> '' AND "currentPrice" IS NOT NULL
GROUP BY ALL