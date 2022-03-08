set ref_date = DATEADD("d", - 7, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;
--next_querry
create or replace table "shop_s3_metadata" as
select case
        when "shop" like '%_cz' then replace("shop",'_cz','.cz')
        when "shop" like '%.cz' then "shop"
        when "shop" like '%_sk' then replace("shop",'_sk','.sk')
        when "shop" like '%.sk' then "shop"
        else "shop"||'.cz'
      end as "shop_id"
	, "itemUrl" as "slug"
  , "itemId"
	, "itemName"
	, "itemImage"
  , "commonPrice"
	, "minPrice"
from "shop_04_extension"
where "slug" != 'null' and "commonPrice" != 'null' and "pkey" in
    (   select distinct("pkey")
        from "shop_04_extension"
        where left("_timestamp",10) > $ref_date
    )
;
--next_querry
create or replace table "slug" as
select distinct("p_key" )
    , case
        when "shop" like '%_cz' then replace("shop",'_cz','.cz')
        when "shop" like '%.cz' then "shop"
        when "shop" like '%_sk' then replace("shop",'_sk','.sk')
        when "shop" like '%.sk' then "shop"
        else "shop"||'.cz'
      end as "shop_id"
    , last_value("slug") ignore nulls over (partition by "itemId" order by "date" desc) as "slug"
from "shop_03_complete"
where "slug" != ''
;
--next_querry
create or replace table "shop_s3_pricehistory" as
select "all".*
from (select "s"."shop_id"
    , "s"."slug"
    , "ph"."json"
from "shop_05_final_s3" "ph"
left join "slug" "s"
on "ph"."p_key" = "s"."p_key"
where "slug" is not null) "all"
inner join "shop_s3_metadata" "m"
on "m"."slug"="all"."slug"
;