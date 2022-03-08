set ref_date = DATEADD("d", - 7, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;
--next_querry
--MAIN TABLE  
--it uses the last record for given item (and only goes back to history for the same period of time as inc at the very beginning does. 
CREATE TABLE "shop_04_extension" AS

SELECT "shop" AS "shop"
	, "slug" AS "itemUrl"
  , "slug" AS "slug" -- currently a duplication but prepared for global switch from "itemURL" to "slug" column as this makes more sense as column name
	, IFF(TRIM("itemId") = ''
		OR "itemId" IS NULL, 'null', "itemId") AS "itemId"
	, IFF(TRIM("itemName") = ''
		OR "itemName" IS NULL, 'null', "itemName") AS "itemName"
	, IFF(TRIM("itemImage") = ''
		OR "itemImage" IS NULL, 'null', "itemImage") AS "itemImage"
  , CASE 
		WHEN TRIM("commonPrice") = ''
			OR "commonPrice" IS NULL
			THEN 'null'
		ELSE to_varchar("commonPrice")
		END AS "commonPrice"
	, CASE 
		WHEN TRIM("minPrice") = ''
			OR "minPrice" IS NULL
			THEN 'null'
		ELSE to_varchar("minPrice")
		END AS "minPrice"
	, "shop" || ':' || ifnull("itemUrl", 'null') AS "pkey"
FROM 
		(SELECT DISTINCT "shop" AS "shop"
			, row_number() OVER (
				PARTITION BY "itemId" ORDER BY "date"::DATE DESC
				) AS "row_number"
		  , "slug" as "slug"
			, "itemId" AS "itemId"
			, "itemName" AS "itemName"
			, "itemImage" AS "itemImage"
			, "commonPrice"
			, "minPrice"
		FROM "shop_03_complete"
		WHERE "itemId" <> ''
			OR "slug" <> ''
		-- here I set a time period for which it checks backwards so that I dont run all of it again. Could 		easily be only for one day.
		AND "_timestamp" >= $ref_date
		)
WHERE "row_number" = 1;