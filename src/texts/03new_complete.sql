set ref_date = DATEADD("d", - 2000, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;
--next_querry
CREATE TABLE "shop_03_complete" AS
SELECT "uni"."shop"
	,"uni"."p_key"
	,"uni"."itemId"
	,"uni"."itemName"
	,"uni"."itemUrl"
  ,"uni"."slug"
	,"uni"."currentPrice"
	,"uni"."originalPrice"
	,"uni"."officialSale"
  ,"uni"."date"
	,"uni"."itemImage"
  ,"uni"."inStock"
	,"ref"."commonPrice"
  ,"ref"."minPrice"
  ,case when "ref"."minPrice" != '' then "ref"."minPrice" else "ref"."commonPrice" end as "originalPriceHS"
  ,round((try_to_number("currentPrice",12,2) / nullifzero(try_to_number("originalPriceHS",12,2)) - 1) * -100, 2)  as "newSale"
FROM (select
  "shop"
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
	FROM "shop_01_unification"
  where left("_timestamp",10) >= $ref_date) "uni"
LEFT JOIN
    (SELECT "itemId"
        , "commonPrice"
        , "minPrice"
        , "date"::varchar as "date"
    FROM "shop_02_refprices") "ref"
ON "uni"."itemId" = "ref"."itemId" AND "uni"."date" = "ref"."date"
;
--next_querry

--vytahuju si produkty, které jsou zdražené o více jak 100% => kontrola chybných cen
create or replace table "suspicious_prices" as
select "shop"
	,"itemId"
	,"itemName"
	,"itemUrl"
  ,"slug"
	,"currentPrice"
	,"originalPrice"
	,"officialSale"
  ,"date"
    , lag("currentPrice") ignore nulls over (partition by "itemId" order by "date" asc) as "prev"
    , "currentPrice"/nullifzero("prev") as "narust"
from "shop_01_unification"
qualify ("narust" > 100 or "narust" < 0.01)
	--and "date" >= $ref_date
order by "date" desc
;