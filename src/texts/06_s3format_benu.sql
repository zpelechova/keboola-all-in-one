/* ===== BLOCK: Codeblock - 06_s3format ===== */

/* ===== CODE: Shop 06_s3format ===== */

set ref_date = DATEADD("d", - 2, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;

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

create or replace table "shop_s3_pricehistory" as
select "shop_id"
    , "slug"
    , "json"
from "shop_05_final_s3"
where left("_timestamp",10) > $ref_date and "slug" is not null and "slug" != ''
;

