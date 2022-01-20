[`
create or replace table "shop_s3_metadata" as
select case
        when "shop" like '%_cz' then replace("shop",'_cz','.cz')
        when "shop" like '%.cz' then "shop"
        when "shop" like '%_sk' then replace("shop",'_sk','.sk')
        when "shop" like '%.sk' then "shop"
        else "shop"||'.cz'
      end as "shop_id"
	, "parsedUrl" as "slug"
    , "itemId"
	, "itemName"
	, "itemImage"
    , "commonPrice"
	, "minPrice"
from "url_shop"
where "slug" != 'null' and "commonPrice" != 'null'
;
`,`
create or replace table "slug" as
select distinct("p_key" )
    , case
        when "shop" like '%_cz' then replace("shop",'_cz','.cz')
        when "shop" like '%.cz' then "shop"
        when "shop" like '%_sk' then replace("shop",'_sk','.sk')
        when "shop" like '%.sk' then "shop"
        else "shop"||'.cz'
      end as "shop_id"
    , last_value("parsedUrl") ignore nulls over (partition by "itemId" order by "date" desc) as "slug"
from "shop_clean"
where "parsedUrl" != ''
;
`,`
create or replace table "shop_s3_pricehistory" as
select "s"."shop_id"
    , "s"."slug"
    , replace("f"."json",'""','null') as "json"
from "shop_final" "f"
left join "slug" "s"
on "f"."p_key" = "s"."p_key"
where "slug" is not null
;
`]