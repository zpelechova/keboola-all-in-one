-- NECHÁVÁM V KÓDU TAKÉ ZAKOMENTOVANÉ ŘÁDKY PŮVODNÍ QUERY OD PADÁKA, pro případ, že by bylo potřeba reverzovat úpravy.
set ref_date = DATEADD("d", - 2000, CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE)
;
--next_querry
-- NECHÁVÁM V KÓDU TAKÉ ZAKOMENTOVANÉ ŘÁDKY PŮVODNÍ QUERY OD PADÁKA, pro případ, že by bylo potřeba reverzovat úpravy.

/*
    - shopy a produkty mohou mít duplicitní informace o denních cenách
    - v takovém případě mám vzít tu nejnižší
    - řeším to pomocí row_number()
 */
CREATE or replace TABLE "all_shops_dedupe_days" AS
/*
 tahle query si jen vezme result svojí sub-query a vyfiltroje čísla řádků = 1
 - k tomu chceme, aby 1240,00 bylo 1240 a aby null byly '' (protože by mi dělaly bordel
   u windows fcí lead/lag, který maj null na detekci prvního a posledního kousku
 */
SELECT
    "t0"."itemId"                                                                    AS "itemId",
    "t0"."date"                                                                     AS "date",
    REPLACE(IFF("t0"."currentPrice" IS NULL, '', "t0"."currentPrice"), '.00', '')   AS "currentPrice",
    REPLACE(IFF("t0"."originalPrice" IS NULL, '', "t0"."originalPrice"), '.00', '') AS "originalPrice"
    FROM
        (
            /*
            - tohle pro každé itemId+date očísluje řádky podle currentPrice
            - je to pro situaci, kdy je v jeden den víc cen - pak nejnižší cena má číslo "1"
            */
            SELECT
                "itemId"                                                                                     AS "itemId",
                "date"                                                                                      AS "date",
                to_char(try_to_number("currentPrice", 20, 2))                                               AS "currentPrice",
                to_char(try_to_number("originalPrice", 20, 2))                                              AS "originalPrice",
                row_number()
                OVER (PARTITION BY "itemId", "date"::DATE ORDER BY try_to_number("currentPrice", 20, 2) ASC) AS "row_number"
                FROM
                    "shop_03_complete"
        ) "t0"
    WHERE
        "t0"."row_number" = 1
;
--next_querry
/*
    - tohle vyrobí tabulku kde je důležitý sloupec "type"
    - slouží pro "emulaci" stavového stroje
    - běží to poměrně dlouho (2min)
 */
CREATE or replace TABLE "produkty" AS
SELECT
    "a"."itemId"          AS "itemId",
    "dd"."min_d"         AS "min_d", --pro budoucí omezení kartézáku
    "dd"."max_d"         AS "max_d", --pro budoucí omezení kartézáku
    "a"."d"              AS "d",
    "a"."o"              AS "o",
    "a"."c"              AS "c"
    --"a"."bothPrice"      AS "bothPrice",
    --"a"."lead"           AS "lead",
    --"a"."lag"            AS "lag",
    /*
     - předchozí is null == je to první řádek
     - následující is null == je to poslední řádek
     - nezměnilo se to...
     - vše ostatní se změnilo
     */
    --CASE
    --    WHEN "a"."lag" IS NULL THEN 'první'
    --    WHEN "a"."lead" IS NULL THEN 'poslední'
    --    WHEN "a"."bothPrice" = "a"."lag" THEN 'beze zmeny'
    --    ELSE 'zmena' END AS "type"
    FROM
        (
            /*
             - tady si připravuju půdo pro detekci změny ceny
             - dělám to tak, že posunu window fcí lead/lag "originaprice+currenPrice" o jedno tam a zpět
             - výsledek pak budu porovnávat
             */
            SELECT DISTINCT
                "itemId"                                                                                        AS "itemId",
                "date"::DATE                                                                                   AS "d",
                "originalPrice"                                                                                AS "o",
                "currentPrice"                                                                                 AS "c"
                --"originalPrice" || '-' || "currentPrice"                                                       AS "bothPrice"
                --LEAD("bothPrice") OVER (PARTITION BY "itemId" ORDER BY "date"::DATE ASC)                        AS "lead",
                --LAG("bothPrice")
                --OVER (PARTITION BY "itemId" ORDER BY "date"::DATE ASC,try_to_number("currentPrice", 20, 2) ASC) AS "lag"
--                object_construct('d', "date", 'o', "originalPrice", 'c', "currentPrice") AS "json"
                FROM "all_shops_dedupe_days"
        ) "a"
            LEFT JOIN (
            /*
             - tady si vyrobím itemId a minimální a maximální datum
             - pokud je max day < current date, přidám 1 den, abych poslala cenu platnou dál "null"
             - pomocí tohohle rozsahu pak vyrobím efektivněji kartézský součin pro gap filling
             */
            SELECT
                "itemId",
                MIN("date") AS "min_d",
                case
                    when to_date(max("date")) < CONVERT_TIMEZONE('Europe/Prague', CURRENT_TIMESTAMP)::DATE then DATEADD("d", +1, max("date"))::date
                else max("date")                                                                                              
                end AS "max_d"  
            FROM "all_shops_dedupe_days"
                GROUP BY
                    "itemId") "dd" ON "dd"."itemId" = "a"."itemId"
;
--next_querry
/*
    - tohle mi generuje sekvenci datumů
    - základní účel je gap filling prázdných datumů
*/
CREATE or replace TABLE "sekvence" AS
SELECT
    "t1"."DateSeq"
    FROM
        (
            /* vyrobím řadu 20000 datumů od 2017-01-01 do 2071-10-31 */
            SELECT
                DATEADD("DAY", seq2(), '2017-01-01') :: DATE AS "DateSeq"
                FROM table(generator(rowcount => 20000))) "t1"
    WHERE
        /* a tady tu řadu zredukuju na nutné minimum */
        ("t1"."DateSeq" >= (SELECT
                                MIN("d")
                                FROM "produkty") AND
         "t1"."DateSeq" <= (SELECT
                                MAX("d")
                                FROM "produkty"))
;
--next_querry
/*
    tady mám tabulku všech datumů a všech itemId a na ně joinuju reálné produkty
    abych tím získal díry a poznal chybějící datumy
*/

create or replace table "temp_final" as
            /*
             - tady si přidám 2 pomocné sloupce, kde:
             - type_lag je pomocný k detekci jestli je řádek opakující se v sekvenci cen nebo první
             - podle "type2" pak řádky odstraníme nebo pustíme do dynamoDB
             - known issue je že pokud první 2 dny je stejná cena, tak se druhý den itemId vždycky nechá
             */
            SELECT *
                   --LAG("type") OVER (PARTITION BY "itemId" ORDER BY "d"::DATE ASC) AS "type_lag",
                   --iff("type" = "type_lag", 'smazat', 'nechat')                   AS "type2_padak"
                   , case
                    when lag("bothPrice") over (partition by "itemId" order by "d" asc) = "bothPrice" then 'smazat'
                    else 'nechat'
                    end as "type2"
                FROM
                    (
                        /*
                         - tabulka t1 je sekvenční list datumů pro itemId
                         - tabulka t2 je list denních cen produktů
                         - t2 obsahuje díry, které se tímhle joinem projeví
                         - sloupeček "type" označuje moment, kdy v apify datech není pro daný den cena
                         */
                        SELECT
                            "t1"."itemId"                                         AS "itemId",
                            "t1"."DateSeq"                                       AS "d",
                            /*
                             - dohoda z whatsapp je, že <null> bude prázdný string
                             */
                            CASE
                                WHEN "t2"."o" IS NULL THEN ''
                                WHEN "t2"."o" = '' THEN ''
                                ELSE "t2"."o" END::VARCHAR(50)                   AS "origP",
                            CASE
                                WHEN "t2"."c" = '' THEN ''
                                WHEN "t2"."c" IS NULL THEN ''
                                ELSE "t2"."c" END::VARCHAR(50)                   AS "currP",
                            --iff("t2"."type" IS NULL, 'nemame data', "t2"."type") AS "type",
                            "origP" || '-' || "currP"                            AS "bothPrice"
                            FROM
                                (
                                    /*
                                     - k sekvenci datumů KARTÉZÁKEM najoinujeme list itemId
                                     - omezený na hranici datumů ve kterých máme pro itemId ceny
                                     */
                                    SELECT
                                        "s"."DateSeq",
                                        "p"."itemId"
                                        FROM
                                            "sekvence" "s"
                                                FULL JOIN (
                                                /*
                                                 - list itemId a hraničních datumů ve kterých máme ceny
                                                 */
                                                SELECT DISTINCT
                                                    "itemId",
                                                    "min_d",
                                                    "max_d"
                                                    FROM "produkty") "p"
                                                          ON "s"."DateSeq" >= "p"."min_d" AND "s"."DateSeq" <= "p"."max_d"
                                ) "t1"
                                    LEFT JOIN "produkty" "t2"
                                              ON "t1"."itemId" = "t2"."itemId" AND "t2"."d" = "t1"."DateSeq"
                    ) "tf"
                ORDER BY
                    "itemId", "d"
;
--next_querry
-- Pro doplnění do výstupních tabulek zjišťuji last_valu of slug a last_valu of p_key, rovnou přeformátovávám "shop"
create or replace table "slug" as
select distinct("itemId" )
    , case
        when "shop" like '%_cz' then replace("shop",'_cz','.cz')
        when "shop" like '%.cz' then "shop"
        when "shop" like '%_sk' then replace("shop",'_sk','.sk')
        when "shop" like '%.sk' then "shop"
        else "shop"||'.cz'
      end as "shop_id"
    , last_value("slug") ignore nulls over (partition by "itemId" order by "date" asc) as "slug"
    , last_value("p_key") ignore nulls over (partition by "itemId" order by "date" asc) as "p_key"
    , last_value("commonPrice") over (partition by "itemId" order by "date" asc) as "commonPrice"
    , last_value("minPrice") over (partition by "itemId" order by "date" asc) as "minPrice"
from "shop_03_complete"
where "slug" != ''
;
--next_querry
CREATE or replace TABLE "shop_05_final_s3" AS
SELECT
    "tof"."itemId"                AS "itemId"
    , "slug"                      AS "slug"
    , "shop_id"                   AS "shop_id"
    ,to_char(array_agg(
            object_construct_KEEP_NULL(
                    'd', "tof"."d",
                    'o', try_to_number("tof"."origP",12,2),
                    'c', try_to_number("tof"."currP",12,2)
                )
        ) WITHIN GROUP (ORDER BY "tof"."d"::DATE ASC))  AS "json"
    , try_to_number("s"."commonPrice",12,2)             AS "commonPrice"
    , try_to_number("s"."minPrice",12,2)                AS "minPrice"
    FROM "temp_final" "tof"
    left join "slug" "s"
    on "s"."itemId" = "tof"."itemId"
    WHERE
        "type2" = 'nechat'  and "slug" != '' and "slug" is not null
        and "tof"."itemId" in (select distinct("itemId")
                    from "temp_final"
                    where "type2" = 'nechat' and "d" > $ref_date)
    GROUP BY
        "tof"."itemId", "slug", "shop_id", "commonPrice", "minPrice"
;