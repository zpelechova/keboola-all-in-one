[`
CREATE TABLE "shop_complete" AS
SELECT "s"."shop"
	,"s"."p_key"
	,"s"."itemId"
	,"s"."itemName"
	,"s"."itemUrl"
  ,"s"."slug"
	,"s"."currentPrice"
	,"s"."originalPrice"
	,"s"."officialSale"
  ,"s"."date"
	,"s"."itemImage"
  ,"s"."inStock"
	,"n"."commonPrice"
  ,"n"."minPrice"
  ,case when "n"."minPrice" != '' then "n"."minPrice" else "n"."commonPrice" end as "originalPriceHS"
  ,round(("s"."currentPrice" / nullifzero("originalPriceHS") - 1) * -100, 2)  as "newSale"
FROM "shop_unified" "s"
LEFT JOIN
    (SELECT "itemId"
        , "commonPrice"
        , "minPrice"
        , "date"::varchar as "date"
    FROM "shop_refprices") "n"
ON "s"."itemId" = "n"."itemId" AND "s"."date" = "n"."date"
;
`]