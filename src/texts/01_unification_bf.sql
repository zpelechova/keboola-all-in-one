set ref_date = DATEADD('day', - 2, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;

--next_querry
--takes only records from ref_date from both input tables - the number of days can be easily modified or omitted altogether
CREATE or REPLACE TABLE "shop" as
SELECT *
FROM "shop_raw"
WHERE LEFT("date", 10) >= $ref_date
;

--next_querry
CREATE OR REPLACE TABLE "shop_bf" as
SELECT *
FROM "shop_raw_bf"
WHERE LEFT("date", 10) >= $ref_date
;

--next_querry
--takes MAIN table, sets minimum number for current price and maximum for original price, converts date to date format, leaves out rows with no itemId or price or date
CREATE OR replace TABLE "tmp.shop" as
select DISTINCT "shop" AS "shop"
	,md5("shop" || "itemId") AS "p_key"
	,"itemId" AS "itemId"
	,"itemName" AS "itemName"
	,"itemUrl" AS "itemUrl"
  , "slug" AS "slug"
	, min(try_to_number("currentPrice", 12, 2)) OVER (
				PARTITION BY "itemId"
				, "date"
				) AS "curntPrice"
	,case
    when max(try_to_number("originalPrice", 12, 2)) OVER (PARTITION BY "itemId", to_date("date")) <= try_to_number("currentPrice", 12, 2) then null
    else max(try_to_number("originalPrice", 12, 2)) OVER (PARTITION BY "itemId", to_date("date"))
   end AS "origPrice"
  --,round(((try_to_number("curntPrice") / nullifzero(try_to_number("origPrice")) - 1) * -100), 2) AS "officialSale"
	,"img" AS "itemImage"
	,to_date("date") AS "date"
  ,"inStock"
FROM "shop"
WHERE "currentPrice" <> ''
AND "date" <> ''
AND "itemId" <> ''
;

--next_querry
--takes BF table, sets minimum number for current price and maximum for original price, converts date to date format, leaves out rows with no itemId or price or date

CREATE or replace TABLE "tmp.shop_bf" AS
SELECT DISTINCT "shop" AS "shop"
	--,md5("shop" || "itemId") AS "p_key"
	,"itemId" AS "itemId"
	,"itemName" AS "itemName"
	,"itemUrl" AS "itemUrl"
  , "slug" AS "slug"
	, min(try_to_number("currentPrice", 12, 2)) OVER (
				PARTITION BY "itemId"
				, "date"
				) AS "curntPrice"
	,case
    when max(try_to_number("originalPrice", 12, 2)) OVER (PARTITION BY "itemId", to_date("date")) <= try_to_number("currentPrice", 12, 2) then null
    else max(try_to_number("originalPrice", 12, 2)) OVER (PARTITION BY "itemId", to_date("date"))
   end AS "origPrice"
  --,round(((try_to_number("currentPrice") / nullifzero(try_to_number("originalPrice")) - 1) * -100), 2) AS "officialSale"
	,"img" AS "itemImage"
	,to_date("date") AS "date"
  ,"inStock"
FROM "shop"
WHERE "currentPrice" <> ''
AND "date" <> ''
AND "itemId" <> ''
;

--next_querry
-- combines MAIN and BF tables together

CREATE OR REPLACE TABLE "shop_01_unification" AS
SELECT DISTINCT COALESCE("s"."shop", "bf"."shop") AS "shop"
	, md5(COALESCE("s"."shop", "bf"."shop") || COALESCE("s"."itemId", "bf"."itemId")) AS "p_key"
--  , "shop" || '_' || COALESCE("s"."itemId", "bf"."itemId") AS "shop_itemId"
	, COALESCE("s"."itemId", "bf"."itemId") AS "itemId"
	, COALESCE("s"."itemName", "bf"."itemName") AS "itemName"
	, COALESCE("s"."itemUrl", "bf"."itemUrl") AS "itemUrl"
  , COALESCE("s"."slug", "bf"."slug") AS "slug"
	, (
		CASE
			WHEN to_number("s"."curntPrice") <= to_number("bf"."curntPrice")
				THEN to_number(COALESCE("s"."curntPrice", "bf"."curntPrice"))
			ELSE to_number(COALESCE("bf"."curntPrice", "s"."curntPrice"))
			END
		) AS "currentPrice"
	, COALESCE("bf"."origPrice", "s"."origPrice") AS "originalPrice"
  ,round((("currentPrice" / nullifzero("originalPrice") - 1) * -100), 2) AS "officialSale"
	, COALESCE("s"."itemImage", "bf"."itemImage") AS "itemImage"
 	, COALESCE("s"."date", "bf"."date") AS "date"
  , COALESCE("s"."inStock", "bf"."inStock") AS "inStock"
FROM "tmp.shop" "s"
FULL JOIN "tmp.shop_bf" "bf" ON to_date("s"."date") = to_date("bf"."date")
	AND "bf"."itemId" = "s"."itemId";