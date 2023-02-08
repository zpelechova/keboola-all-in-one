set ref_date = DATEADD('d', - {{days_back}}, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
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
	, "slug"
  , "itemId"
	, "itemName"
	, "itemImage"
from "shop_04_extension"
where "slug" != 'null' and "slug" in
    (   select distinct("slug")
        from "shop_04_extension"
        where left("_timestamp",10) > $ref_date
    )
;
--next_querry
create or replace table "shop_s3_pricehistory" as
select "shop_id"
    , "slug"
    ,to_char(object_construct_KEEP_NULL(
                    'entries', parse_json("json"),
                    'commonPrice', try_to_number("commonPrice",12,2),
                    'minPrice', try_to_number("minPrice",12,2)
                ))
        AS "json"
from "shop_05_final_s3" "ph"
where left("_timestamp",10) > $ref_date and
    "slug" is not null and "slug" != ''
GROUP BY
        "itemId", "slug", "shop_id", "json", "commonPrice", "minPrice"
;

