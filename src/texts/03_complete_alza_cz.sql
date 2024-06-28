CREATE TABLE "shop_03_complete" AS
WITH "last_data" AS (
SELECT "shop"
	,"p_key"
	,"itemId"
	,"itemName"
	,"itemUrl"
  ,"slug"
	,"currentPrice"
	,"originalPrice"
	,"officialSale"
  ,"date"
	,"itemImage"
   ,"inStock"
   , "usedGoods"
FROM "shop_01_unification"
WHERE "_timestamp" = (SELECT max("_timestamp") FROM "shop_01_unification")
)

, "last_data_with_refprices" AS (
SELECT "shop"
	,"p_key"
	,uni."itemId"
	,"itemName"
	,"itemUrl"
  ,"slug"
	,"currentPrice"
	,"originalPrice"
	,"officialSale"
  ,uni."date"
	,"itemImage"
  ,"inStock"
  , "usedGoods"
	,ref."commonPrice"
  ,ref."minPrice"
FROM "last_data" uni
LEFT JOIN "shop_02_refprices" ref
  ON uni."itemId" = ref."itemId"
    AND uni."date" = ref."date"
)

SELECT "shop"
	,"p_key"
	,"itemId"
	,"itemName"
	,"itemUrl"
  ,"slug"
	,"currentPrice"
	,"originalPrice"
	,"officialSale"
  ,"date"
	,"itemImage"
  ,"inStock"
  , "usedGoods"
	, "commonPrice"
  ,"minPrice"
  , CASE WHEN "minPrice" != '' THEN "minPrice" ELSE "commonPrice" END AS "originalPriceHS"
  , ROUND((TRY_TO_NUMBER("currentPrice",12,2) / NULLIFZERO(TRY_TO_NUMBER("originalPriceHS",12,2)) - 1) * -100, 2)  AS "newSale"
FROM "last_data_with_refprices"
;
--next_querry

--vytahuju si produkty, které jsou zdražené o více jak 100% => kontrola chybných cen
SET ref_date = DATEADD('day', - 2, (SELECT max("_timestamp") FROM "shop_01_unification"))
;
--next_querry

CREATE OR REPLACE TABLE "suspicious_prices" AS
SELECT "shop"
	,"itemId"
	,"itemName"
	,"itemUrl"
  ,"slug"
	,"currentPrice"
	,"originalPrice"
	,"officialSale"
  ,"date"
  , LAG(TRY_TO_NUMBER("currentPrice")) IGNORE NULLS OVER (PARTITION BY "itemId" ORDER BY "date" ASC) AS "prev"
  , TRY_TO_NUMBER("currentPrice")/NULLIFZERO("prev") AS "narust"
FROM "shop_01_unification"
WHERE "date" >= $ref_date
QUALIFY ("narust" > 100 OR "narust" < 0.01)
ORDER BY "date" DESC
;
