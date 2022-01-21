[`
--takes only records from last 7 days from both input tables - the number of days can be easily modified or omitted altogether
CREATE VIEW "shop_bf"
AS
SELECT *
FROM "shop_bf_all"
WHERE left("date", 10) >= to_char(DATEADD("d", - 3,CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE), 'yyyy-mm-dd')
;
`,`
CREATE VIEW "shop"
AS
SELECT *
FROM "shop_all"
WHERE left("date", 10) >= to_char(DATEADD("d", - 3, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE), 'yyyy-mm-dd')
;
`,`
--takes bf table, sets minimum number for current price and maximum for original price, converts date to date format, leaves out rows with no itemId or price or date 
CREATE or replace VIEW "tmp.shop_bf"
AS
SELECT DISTINCT "itemId" AS "itemId"
	, "itemName" AS "itemName"
	, min(try_to_number("currentPrice")) OVER (
  		PARTITION BY "itemId", to_date("date")
			) AS "curntPrice"
	, max(try_to_number("originalPrice")) OVER (
		PARTITION BY "itemId", to_date("date")
		) AS "origPrice"
	, "itemUrl" AS "itemUrl"
   , "slug" as "slug"
	, "img" AS "itemImage"
  , "inStock"
	, to_date("date") AS "date"
FROM "shop_bf"
WHERE "currentPrice" <> ''
	AND "date" <> ''
	AND "itemId" <> ''
;
`,`
--takes main table, sets minimum number for current price and maximum for original price, converts date to date format, leaves out rows with no itemId or price or date

CREATE or replace VIEW "tmp.shop_clean"
AS
SELECT DISTINCT "itemId" AS "itemId"
	, "itemName" AS "itemName"
	, "itemUrl" AS "itemUrl"
   , "slug" as "slug"
	, min(try_to_number("currentPrice")) OVER (
		PARTITION BY "itemId", to_date("date")
		) AS "curntPrice"
	, max(try_to_number("originalPrice")) OVER (
		PARTITION BY "itemId", to_date("date")
		) AS "origPrice"
	, "img" AS "itemImage"
  , "inStock"
	, to_date("date") AS "date"
FROM "shop_all"
WHERE "currentPrice" <> ''
	AND "date" <> ''
	AND "itemId" <> ''
;
`,`
--takes main table, sets minimum number for current price and maximum for original price, converts date to date format, leaves out rows with no itemId or price or date, leaves out extra information
--creates two md5 hashes - p_key is unique for item and consists of shop and itemId, pk is unique for item a date and is made from p_key, - , date

CREATE TABLE "shop_unified"
AS
SELECT DISTINCT "shop" AS "shop"
	, md5("shop" || COALESCE("s"."itemId", "bf"."itemId")) AS "p_key"
  , "shop" || '_' || COALESCE("s"."itemId", "bf"."itemId") AS "shop_itemId"
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
  ,round(((try_to_number("currentPrice") / nullifzero(try_to_number("originalPrice")) - 1) * -100), 2) AS "officialSale"
	, COALESCE("s"."itemImage", "bf"."itemImage") AS "itemImage"
 	, COALESCE("s"."date", "bf"."date") AS "date"
  , COALESCE("s"."inStock", "bf"."inStock") AS "inStock"
FROM "tmp.shop_clean" "s"
FULL JOIN "tmp.shop_bf" "bf" ON to_date("s"."date") = to_date("bf"."date")
	AND "bf"."itemId" = "s"."itemId";
`]