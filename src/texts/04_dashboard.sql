[`
create or replace table "refPrice" as
select *,
case when "minPrice" != '' then try_to_number("minPrice")
else try_to_number("commonPrice") end as "originalPriceHS"
from "shop_refprices"
;
`,`
--vyčištěná bf tabulka, beru jen cenu za poslední den, kdy byly v bf kategorii
--MOMENTÁLNĚ TROCHU ZBYTNÁ, UVIDÍME, JAK TO PUJDE U OSTATNÍCH SHOPŮ
create or replace table "bf_last" as
select * from 
(
select distinct "itemId"
  ,"itemName"
  ,"itemUrl"
  ,try_to_number("currentPrice") as "currentPrice"
  ,case when try_to_number("originalPrice") <= to_number("currentPrice") then null
  else try_to_number("originalPrice") end as "originalPrice"
  ,"date",
row_number() over(
partition by "itemId" order by "date" desc) as "row_number" 
from "shop_bf_raw"
where (left("date", 7) = '2021-10' or left("date", 7) = '2021-11') and "currentPrice" !=  'Price not defined.' and "currentPrice" !=  ''
)
where "row_number" = 1
;
`,`
--tady zjistuji nejnizsi cenu v ramci bf
create or replace table "bf_min" as
select * from 
(
select distinct "itemId"
  ,"itemName"
  ,"itemUrl"
  ,try_to_number("currentPrice") as "currentPrice"
  ,case when try_to_number("originalPrice") <= to_number("currentPrice") then null
  else try_to_number("originalPrice") end as "originalPrice"
  ,"date",
row_number() over(
partition by "itemId" order by try_to_number("currentPrice")) as "row_number" 
from "shop_bf_raw"
where (left("date", 7) = '2021-10' or left("date", 7) = '2021-11') and "currentPrice" !=  'Price not defined.' and "currentPrice" !=  ''
)
where "row_number" = 1
;
`,`
--tady dam tabulky dohromady a porovnam slevu podle obchodnika (rozdil mezi nejnizsi currentPrice v ramci bf a jeji originalPrice) a slevu podle Hlidace
create or replace  table "bf_joined" as
select o."shop"
,l."itemId"
,l."itemName"
,l."itemUrl"
,l."currentPrice"
,to_number(m."currentPrice") as "minPrice"
,try_to_number(l."originalPrice") as "lastOriginalPrice" 
,try_to_number(m."originalPrice") as "minOriginalPrice"
,try_to_number(o."originalPriceHS") as "originalPriceHS"
,l."date"
,m."date" as "minDate"
from "bf_min" m
left join "bf_last" l
on m."itemId" = l."itemId"
left join "refPrice" o
on m."itemId" = o."itemId"
;
`,`
create or replace  table "bf_clean" as
select *
,100 - "minPrice" / "minOriginalPrice" * 100 as "declaredSale" 
,case when "originalPriceHS" * 100 < "minPrice" then null
when "originalPriceHS" = 0 then null
else 100 - "minPrice" / "originalPriceHS" * 100 
end as "hsSale"
,"declaredSale" - "hsSale" as "diff"
from "bf_joined"
;
`,`
create or replace table "sales" as  
select "shop", avg("declaredSale") as "avgDeclaredSale", avg("hsSale") as "avgHsSale" from "bf_clean"
;
`,`
--pocet unikátních produktů v shopu za listopad
create or replace table "count_clean" as
select count(distinct "itemId") as "count_clean"
from "shop_complete"
where (left("date", 7) = '2021-10' or left("date", 7) = '2021-11') 
;
`,`
--pocet unikátních produktů v BF kategorii za  listopad
create or replace table "count_bf" as
select count(distinct "itemId") as "count_bf"
from "shop_bf_raw"
where (left("date", 7) = '2021-10' or left("date", 7) = '2021-11') 
;
`,`
--MAIN TABLE - one row for shops numbers (WEB DASHBOARD)
CREATE or replace  TABLE "dash" AS

SELECT "c"."shop"
    , "a"."count_clean"
	, "b"."count_bf"
	, "c"."avgDeclaredSale"
    , "c"."avgHsSale"
    FROM "count_clean" "a"
LEFT JOIN "count_bf" "b"
LEFT JOIN "sales" "c"
;
`]