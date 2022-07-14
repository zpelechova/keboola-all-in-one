
set ref_date = DATEADD("d", - 2, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;
--next_querry
--MAIN TABLE  
--it uses the last record for given item (and only goes back to history for the same period of time as inc at the very beginning does. 
CREATE or replace TABLE "shop_04_extension" AS

SELECT "shop" AS "shop"
  , "slug" AS "slug"
	, IFF(TRIM("itemId") = ''
		OR "itemId" IS NULL, 'null', "itemId") AS "itemId"
	, IFF(TRIM("itemName") = ''
		OR "itemName" IS NULL, 'null', "itemName") AS "itemName"
	, IFF(TRIM("itemImage") = ''
		OR "itemImage" IS NULL, 'null', "itemImage") AS "itemImage"
FROM 
		(SELECT DISTINCT "shop" AS "shop"
			, row_number() OVER (
				PARTITION BY "itemId" ORDER BY "date"::DATE DESC
				) AS "row_number"
		  , "slug" as "slug"
			, "itemId" AS "itemId"
			, "itemName" AS "itemName"
			, "itemImage" AS "itemImage"
		FROM "shop_03_complete"
		WHERE ("itemId" != ''
			and "slug" != ''
      and "commonPrice" != '')
		-- here I set a time period for which it checks backwards so that I dont run all of it again. Could 		easily be only for one day.
		AND to_date("_timestamp") >= $ref_date
		)
WHERE "row_number" = 1;