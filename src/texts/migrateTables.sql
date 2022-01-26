alter table "shop_w" rename column "parsedUrl" to "slug"
;
--next_querry
alter table "shop_clean" rename column "parsedUrl" to "slug"
;
--next_querry
create or replace table "shop_01_unification" as
select "shop"
    , "p_key"
    , "itemId"
    , "itemName"
    , "itemUrl"
    , "slug"
    , to_number("currentPrice",12,2) as "currentPrice"
    , iff(try_to_number("originalPrice",12,2) <= to_number("currentPrice",12,2), null, try_to_number("originalPrice",12,2)) as "originalPrice"
    , round(((to_number("currentPrice",12,2) / nullifzero(iff(try_to_number("originalPrice",12,2) <= to_number("currentPrice",12,2), null, "originalPrice")) - 1) * -100), 2) AS "officialSale"
    , "itemImage"
    , "date"
    , "inStock"
from "shop_w"
;
--next_querry
create or replace table "shop_02_refprices" as
select "itemId"
    , "commonPrice"
    , "minPrice"
    , "date"
from "shop_new"
;
--next_querry
create or replace table "shop_03_complete" as
select "shop"
    , "p_key"
    , "itemId"
    , "itemName"
    , "itemUrl"
    , "slug"
    , "currentPrice"
    , iff(try_to_number("originalPrice",12,2) <= to_number("currentPrice",12,2), null, try_to_number("originalPrice",12,2)) as "originalPrice"
    , round(((to_number("currentPrice",12,2) / nullifzero(iff(try_to_number("originalPrice",12,2) <= to_number("currentPrice",12,2), null, "originalPrice")) - 1) * -100), 2) AS "officialSale"
    , "date"
    , "itemImage"
    , "inStock"
    , "commonPrice"
    , "minPrice"
    , "originalPriceHS"
    , "newSale"
from "shop_clean"
;