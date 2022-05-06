set ref_date = (select max("date") from "shop_03_complete")
;
--next_querry

set sleva_hranice = 3
;
--next_querry

set tolerance = 3
;
--next_querry


set aktualizace = (select left(max("date"),16) from "shop_raw")
;
--next_querry

set days_back = 30
;
--next_querry

create or replace table "shop_current" as
select "shop"
    , "itemId"
    , "itemName"
    , "currentPrice" as "prodejni_cena"
    , "originalPrice" as "refCena_dle_shopu"
    , "originalPriceHS" as "refCena_dle_Hlidace"
    , case
        when round(("currentPrice" / nullifzero(try_to_number("originalPrice",12,2)) - 1) * -100, 2) is null then 0
        else round(("currentPrice" / nullifzero(try_to_number("originalPrice",12,2)) - 1) * -100, 2)
      end as "sleva_dle_shopu"
    , "newSale" as "sleva_dle_Hlidace"
    , "date" as "datum"
    , "itemUrl"
from "shop_03_complete"
where "date" = $ref_date
;
--next_querry

create or replace table "shop_30d" as
select "shop"
    , "itemId"
    , "itemName"
    , "currentPrice" as "prodejni_cena"
    , "originalPrice" as "refCena_dle_shopu"
    , "originalPriceHS" as "refCena_dle_Hlidace"
    , case
        when round(("currentPrice" / nullifzero(try_to_number("originalPrice",12,2)) - 1) * -100, 2) is null then 0
        else round(("currentPrice" / nullifzero(try_to_number("originalPrice",12,2)) - 1) * -100, 2)
      end as "sleva_dle_shopu"
    , "newSale" as "sleva_dle_Hlidace"
    , "date" as "datum"
    , "itemUrl"
from "shop_03_complete"
where "date" >= DATEADD("d", - $days_back, $ref_date)
;
--next_querry

create or replace table "shop_HS_differences" as
select *
from "shop_current"
where "sleva_dle_shopu" != try_to_number("sleva_dle_Hlidace",2) and abs("sleva_dle_shopu" - try_to_number("sleva_dle_Hlidace",2)) >= $tolerance
	and "sleva_dle_shopu" != 0
order by "sleva_dle_shopu" desc
;
--next_querry

create or replace table "count_items" as
select distinct("shop")
    , count(distinct("itemId")) over () as "Produktu_celkem" --"count_clean"
    , "datum" as "aktualizace"
from "shop_current"
;
--next_querry

create or replace table "count_disc_shop" as
select distinct(count(distinct("itemId")) over ()) as "Produktu_ve_sleve_(shop)"--"count_bf"
from "shop_current"
having "sleva_dle_shopu" > $sleva_hranice
;
--next_querry

create or replace table "count_disc_HS" as
select distinct(count(distinct("itemId")) over ()) as "Produktu_ve_sleve_(HS)"
from "shop_current"
having try_to_number("sleva_dle_Hlidace",2) > $sleva_hranice
;
--next_querry

create or replace table "count_diff" as
select count(distinct("itemId")) over () as "Neshoda_ve_sleve_(all)"
from "shop_HS_differences"
;
--next_querry

create or replace table "count_diff_shop-only" as
select distinct(count(distinct("itemId")) over ()) as "Neshoda_ve_sleve"
from 
(    select "diff".*
    from "shop_HS_differences" "diff"
    inner join 
        (select * from "shop_HS_differences" where "sleva_dle_shopu" > $sleva_hranice) "shop"
    on "diff"."itemId" = "shop"."itemId"
)
;
--next_querry

create or replace table "avg_disc_shop" as
select round(avg("sleva_dle_shopu") over (),2) as "Prumerna_uvadena_sleva"
from "shop_current"
having "sleva_dle_shopu" > $sleva_hranice
;
--next_querry

create or replace table "avg_disc_HS" as
select round(avg("sleva_dle_Hlidace") over (),2) as "Prumerna_realna_sleva"
from "shop_current"
having "sleva_dle_shopu" > $sleva_hranice
;
--next_querry

// spreadsheet - 1 záložka všechny neshody + označení typu neshody
create or replace table "shop_false_discounts" as
select case
        when ("diff"."sleva_dle_shopu" > "diff"."sleva_dle_Hlidace" and "diff"."sleva_dle_Hlidace" < 0) then '1 - shop sleva, Hlídač zdražení'
        when ("diff"."sleva_dle_shopu" > "diff"."sleva_dle_Hlidace" and "diff"."sleva_dle_Hlidace" = 0) then '2 - shop sleva, Hlídač bez slevy'
        when ("diff"."sleva_dle_shopu" > "diff"."sleva_dle_Hlidace" and "diff"."sleva_dle_Hlidace" > 0) then '3 - shop sleva vyšší než sleva Hlídač'
        when ("diff"."sleva_dle_shopu" < "diff"."sleva_dle_Hlidace" and "diff"."sleva_dle_Hlidace" > 0) then '4 - shop sleva nižší než sleva Hlídač'
end as "typ_neshody"
    , "diff".*
    from "shop_HS_differences" "diff"
    inner join 
        (select * from "shop_HS_differences" where "sleva_dle_shopu" > $sleva_hranice) "shop"
    on "diff"."itemId" = "shop"."itemId"
order by 1 asc
;
--next_querry

// spreadsheet - 2 záložka navýšené originalPrice (za posledních 30 dní)
// omezeno na: změna o víc jak 3% a pouze tam, kde se neshodneme na slevě
create or replace table "shop_incr_origPrice" as
select "shop"
    , "itemId"
    , "itemName"
    , "prodejni_cena"
    , "refCena_dle_shopu" as "refCena_aktualni"
    , "refCena_puv" as "refCena_puvodni"
    , "datum" as "datum_zmeny_refCeny"
    , "refCena_dle_Hlidace"
    , "sleva_dle_shopu"
    , "sleva_dle_Hlidace"
    , "itemUrl"
    , row_number() over (partition by "itemId" order by "datum" desc) as "row_num"
from
(select *
    , (lag("refCena_dle_shopu") over (partition by "itemId" order by "datum" asc)) as "refCena_puv"
    , case 
        when (lag("refCena_dle_shopu") ignore nulls over (partition by "itemId" order by "datum" asc))  < "refCena_dle_shopu" then 'incr'
        when (lag("refCena_dle_shopu") ignore nulls over (partition by "itemId" order by "datum" asc))  > "refCena_dle_shopu" then 'decr'
        else ''
      end as "incr"
from "shop_30d"
qualify "refCena_puv" != '' and "refCena_dle_shopu" != '' and "incr" != '')
qualify "row_num" = 1 and "incr" = 'incr' 
    and ("refCena_aktualni"/"refCena_puvodni")*100-100 > 3
    and abs("sleva_dle_shopu" - try_to_number("sleva_dle_Hlidace",2)) >= $tolerance
;
--next_querry

alter table "shop_incr_origPrice" drop column "row_num";
--next_querry

create or replace table "count_incr_origPrice" as
select count(distinct("itemId")) as "Produktu_s_navysenou_refCenou"
from "shop_incr_origPrice"
;
--next_querry

create or replace table "shop_dashboard" as
select distinct("shop"."shop") as "Shop"
    , "shop"."Produktu_celkem" as "Produktu_celkem" -- celkový počet produktů shopu k poslednímu dni
    , "disc_shop"."Produktu_ve_sleve_(shop)" as "Produktu_ve_sleve" -- počet produktů ve slevě dle shopu (hranice slevy viz $sleva_hranice)
    --, "disc_HS"."Produktu_ve_sleve_(HS)" as "Produktu_ve_sleve_(HS)"
    , "diff_shop"."Neshoda_ve_sleve" as "Produktu_s_chybnou_slevou" -- produkty dle shopu ve slevě, kde se s HS neshodne o 3 a více %
--    , "diff".*
    , "incr_orig".* -- produkty s navýšenou originalPrice během posledních 30 dní
    , "avg_shop".*
    , "avg_HS".*
    , $aktualizace as "Aktualizace"
from "count_items" "shop"
left join "count_disc_shop" "disc_shop"
left join "count_disc_HS" "disc_HS"
--left join "count_diff" "diff"
left join "count_diff_shop-only" "diff_shop"
left join "avg_disc_shop" "avg_shop"
left join "avg_disc_HS" "avg_HS"
left join "count_incr_origPrice" "incr_orig"
;