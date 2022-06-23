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
set shop = (select distinct("shopOrigin") from "shop_raw" where left("date",16) = $aktualizace)
;
--next_querry
create or replace table "shop_current" as
select $shop as "shop"
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
select $shop as "shop"
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
select case
        when (to_number("sleva_dle_shopu") > to_number("sleva_dle_Hlidace") and to_number("sleva_dle_Hlidace") < 0) then '1 - shop sleva, Hlídač zdražení'
        when (to_number("sleva_dle_shopu") > to_number("sleva_dle_Hlidace") and to_number("sleva_dle_Hlidace") = 0) then '2 - shop sleva, Hlídač bez slevy'
        when (to_number("sleva_dle_shopu") > to_number("sleva_dle_Hlidace") and to_number("sleva_dle_Hlidace") > 0) then '3 - shop sleva vyšší než sleva Hlídač'
        when (to_number("sleva_dle_shopu") < to_number("sleva_dle_Hlidace") and to_number("sleva_dle_Hlidace") > 0) then '4 - shop sleva nižší než sleva Hlídač'
    end as "typ_neshody"
    , *
from "shop_current"
where abs(to_number("sleva_dle_shopu", 12,2) - try_to_number("sleva_dle_Hlidace",12, 2)) >= $tolerance
	and to_number("sleva_dle_shopu") != 0
order by abs(to_number("sleva_dle_shopu", 12,2) - try_to_number("sleva_dle_Hlidace",12, 2)) desc
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
select 
    case
        when count("itemId") is null then 0
        else count(distinct("itemId"))
    end as "Produktu_ve_sleve_(shop)"--"count_bf"
from (select * from "shop_current" where to_number("sleva_dle_shopu",12,2) > $sleva_hranice)
;
--next_querry
create or replace table "count_diff_shop-only" as
select case
        when count("itemId") = 0 then null
        else count(distinct("itemId"))
    end as "Neshoda_ve_sleve"
from 
(    select "diff".*
    from "shop_HS_differences" "diff"
    inner join 
        (select * 
         from "shop_HS_differences" 
         where "sleva_dle_shopu" > $sleva_hranice and "typ_neshody" != '4 - shop sleva nižší než sleva Hlídač'
    ) "shop"
    on "diff"."itemId" = "shop"."itemId"
)
;
--next_querry
create or replace table "avg_disc_shop" as
select distinct(round(avg("sleva_dle_shopu") over (),2)) as "Prumerna_uvadena_sleva"
      from "shop_current" where to_number("sleva_dle_shopu",12,2) > $sleva_hranice
;
--next_querry
create or replace table "avg_disc_HS" as
select distinct(round(avg(try_to_number("sleva_dle_Hlidace",12,2)) over (),2)) as "Prumerna_realna_sleva"
from "shop_current"
having to_number("sleva_dle_shopu") > $sleva_hranice
;
--next_querry
// spreadsheet - 1 záložka všechny neshody + označení typu neshody
create or replace table "shop_false_discounts" as
select "diff".*
    , abs(to_number("diff"."sleva_dle_shopu", 12,2) - try_to_number("diff"."sleva_dle_Hlidace",12, 2)) as "rozdil_slev"
    , row_number () over (order by "rozdil_slev" desc) as "row_num"
    from "shop_HS_differences" "diff"
    inner join 
        (select * from "shop_HS_differences" where to_number("sleva_dle_shopu") > $sleva_hranice) "shop"
    on "diff"."itemId" = "shop"."itemId"
qualify "row_num" <= 400000
order by "rozdil_slev" desc
;
--next_querry
alter table "shop_false_discounts" drop column "row_num"
;
--next_querry
// spreadsheet - 2 záložka navýšené originalPrice (za posledních 30 dní)
// omezeno na: změna o víc jak 3% a pouze tam, kde se neshodneme na slevě
create or replace table "shop_incr_origPrice" as
select "shop"
    , "incr"."itemId"
    , "itemName"
    , "prodejni_cena"
    , "refCena_dle_shopu" as "refCena_aktualni"
    , "refCena_puv" as "refCena_puvodni"
    , "datum" as "datum_zmeny_refCeny"
    , "refCena_dle_Hlidace"
    , "sleva_dle_shopu"
    , "sleva_dle_Hlidace"
    , "itemUrl"
    , (to_number("refCena_dle_shopu",12,2) - to_number("refCena_puv",12,2)) as "rozdil_refCen"
    , row_number() over (partition by "incr"."itemId" order by "datum" desc) as "row_num"
from
(select *
    , (lag("refCena_dle_shopu") over (partition by "itemId" order by "datum" asc)) as "refCena_puv"
    , case 
        when (lag("refCena_dle_shopu") ignore nulls over (partition by "itemId" order by "datum" asc))  < "refCena_dle_shopu" then 'incr'
        when (lag("refCena_dle_shopu") ignore nulls over (partition by "itemId" order by "datum" asc))  > "refCena_dle_shopu" then 'decr'
        else ''
      end as "incr"
from "shop_30d"
qualify "refCena_puv" != '' and "refCena_dle_shopu" != '' and "incr" != '') "incr"
inner join 
    (select "itemId"
    from "shop_current"
    where "sleva_dle_shopu" >= $tolerance) "disc"
on "incr"."itemId" = "disc"."itemId"
qualify "row_num" = 1 and "incr" = 'incr' 
    and ("refCena_aktualni"/"refCena_puvodni")*100-100 > 3
    and abs(to_number("sleva_dle_shopu", 12,2) - try_to_number("sleva_dle_Hlidace",12, 2)) >= $tolerance
order by "rozdil_refCen" desc
;
--next_querry
alter table "shop_incr_origPrice" drop column "row_num"
;
--next_querry
create or replace table "count_incr_origPrice" as
select case
        when count("itemId") = 0 then 0
        else count(distinct("itemId"))
    end as "Produktu_s_navysenou_refCenou"
from "shop_incr_origPrice"
;
--next_querry
alter session set timezone = 'Europe/Prague'
;
--next_querry
create or replace table "shop_dashboard" as
select distinct("shop"."shop") as "Shop"
    , "shop"."Produktu_celkem" as "Produktu_celkem" -- celkový počet produktů shopu k poslednímu dni
    , (select "Produktu_ve_sleve_(shop)" from "count_disc_shop") as "Produktu_ve_sleve" -- počet produktů ve slevě dle shopu (hranice slevy viz $sleva_hranice)
    , iff("Produktu_ve_sleve" = 0, null, (select * from "count_diff_shop-only")) as "Produktu_s_chybnou_slevou" -- produkty dle shopu ve slevě, kde se s HS neshodne o 3 a více %
    , iff("Produktu_ve_sleve" = 0, null, (select * from "count_incr_origPrice")) as "Produktu_s_navysenou_refCenou" -- produkty s navýšenou originalPrice během posledních 30 dní
    , iff("Produktu_ve_sleve" = 0, null, (select * from "avg_disc_shop")) as "Prumerna_uvadena_sleva"
    , iff("Produktu_ve_sleve" = 0, null, (select * from "avg_disc_HS")) as "Prumerna_realna_sleva"
    , to_timestamp_tz(CONVERT_TIMEZONE('UTC' , 'Europe/Prague' ,  $aktualizace)) as "Aktualizace"
from "count_items" "shop"
;