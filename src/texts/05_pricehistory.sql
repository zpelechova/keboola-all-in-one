/*
    - shopy a produkty mohou mít duplicitní informace o denních cenách
    - v takovém případě mám vzít tu nejnižší
    - řeším to pomocí row_number()
 */
CREATE TABLE "all_shops_dedupe_days" AS
/*
 tahle query si jen vezme result svojí sub-query a vyfiltroje čísla řádků = 1
 - k tomu chceme, aby 1240,00 bylo 1240 a aby null byly '' (protože by mi dělaly bordel
   u windows fcí lead/lag, který maj null na detekci prvního a posledního kousku
 */
SELECT
    "t0"."p_key"                                                                    AS "p_key",
    "t0"."date"                                                                     AS "date",
    REPLACE(IFF("t0"."currentPrice" IS NULL, '', "t0"."currentPrice"), '.00', '')   AS "currentPrice",
    REPLACE(IFF("t0"."originalPrice" IS NULL, '', "t0"."originalPrice"), '.00', '') AS "originalPrice"
    FROM
        (
            /*
            - tohle pro každé p_key+date očísluje řádky podle currentPrice
            - je to pro situaci, kdy je v jeden den víc cen - pak nejnižší cena má číslo "1"
            */
            SELECT
                "p_key"                                                                                     AS "p_key",
                "date"                                                                                      AS "date",
                to_char(try_to_number("currentPrice", 20, 2))                                               AS "currentPrice",
                to_char(try_to_number("originalPrice", 20, 2))                                              AS "originalPrice",
                row_number()
                OVER (PARTITION BY "p_key", "date"::DATE ORDER BY try_to_number("currentPrice", 20, 2) ASC) AS "row_number"
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
CREATE TABLE "produkty" AS
SELECT
    "a"."p_key"          AS "p_key",
    "dd"."min_d"         AS "min_d", --pro budoucí omezení kartézáku
    "dd"."max_d"         AS "max_d", --pro budoucí omezení kartézáku
    "a"."d"              AS "d",
    "a"."o"              AS "o",
    "a"."c"              AS "c",
    "a"."bothPrice"      AS "bothPrice",
    "a"."lead"           AS "lead",
    "a"."lag"            AS "lag",
    /*
     - předchozí is null == je to první řádek
     - následující is null == je to poslední řádek
     - nezměnilo se to...
     - vše ostatní se změnilo
     */
    CASE
        WHEN "a"."lag" IS NULL THEN 'první'
        WHEN "a"."lead" IS NULL THEN 'poslední'
        WHEN "a"."bothPrice" = "a"."lag" THEN 'beze zmeny'
        ELSE 'zmena' END AS "type"
    FROM
        (
            /*
             - tady si připravuju půdo pro detekci změny ceny
             - dělám to tak, že posunu window fcí lead/lag "originaprice+currenPrice" o jedno tam a zpět
             - výsledek pak budu porovnávat
             */
            SELECT DISTINCT
                "p_key"                                                                                        AS "p_key",
                "date"::DATE                                                                                   AS "d",
                "originalPrice"                                                                                AS "o",
                "currentPrice"                                                                                 AS "c",
                "originalPrice" || '-' || "currentPrice"                                                       AS "bothPrice",
                LEAD("bothPrice") OVER (PARTITION BY "p_key" ORDER BY "date"::DATE ASC)                        AS "lead",
                LAG("bothPrice")
                OVER (PARTITION BY "p_key" ORDER BY "date"::DATE ASC,try_to_number("currentPrice", 20, 2) ASC) AS "lag"
--                object_construct('d', "date", 'o', "originalPrice", 'c', "currentPrice") AS "json"
                FROM "all_shops_dedupe_days"
        ) "a"
            LEFT JOIN (
            /*
             - tady si vyrobím p_key a minimální a maximální datum
             - pomocí tohohle rozsahu pak vyrobím efektivněji kartézský součin pro gap filling
             */
            SELECT
                "p_key",
                MIN("date") AS "min_d",
                max("date") AS "max_d"
                FROM "all_shops_dedupe_days"
                GROUP BY
                    "p_key") "dd" ON "dd"."p_key" = "a"."p_key"
;
--next_querry
/*
    - tohle mi generuje sekvenci datumů
    - základní účel je gap filling prázdných datumů
*/
CREATE TABLE "sekvence" AS
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
    tady mám tabulku všech datumů a všech p_key a na ně joinuju reálné produkty
    abych tím získal díry a poznal chybějící datumy
*/

create or replace table "temp_final" as
            /*
             - tady si přidám 2 pomocné sloupce, kde:
             - type_lag je pomocný k detekci jestli je řádek opakující se v sekvenci cen nebo první
             - podle "type2" pak řádky odstraníme nebo pustíme do dynamoDB
             - known issue je že pokud první 2 dny je stejná cena, tak se druhý den p_key vždycky nechá
             */
            SELECT *,
                   LAG("type") OVER (PARTITION BY "p_key" ORDER BY "d"::DATE ASC) AS "type_lag",
                   iff("type" = "type_lag", 'smazat', 'nechat')                   AS "type2"
                FROM
                    (
                        /*
                         - tabulka t1 je sekvenční list datumů pro p_key
                         - tabulka t2 je list denních cen produktů
                         - t2 obsahuje díry, které se tímhle joinem projeví
                         - sloupeček "type" označuje moment, kdy v apify datech není pro daný den cena
                         */
                        SELECT
                            "t1"."p_key"                                         AS "p_key",
                            "t1"."DateSeq"                                       AS "d",
                            /*
                             - dohoda z whatsapp je, že <null> bude prázdný string
                             */
                            CASE
                                WHEN "t2"."o" IS NULL THEN ''
                                WHEN "t2"."o" = '' THEN ''
                                ELSE "t2"."o" END::VARCHAR(50)                   AS "o",
                            CASE
                                WHEN "t2"."c" = '' THEN ''
                                WHEN "t2"."c" IS NULL THEN ''
                                ELSE "t2"."c" END::VARCHAR(50)                   AS "c",
                            iff("t2"."type" IS NULL, 'nemame data', "t2"."type") AS "type"
                            FROM
                                (
                                    /*
                                     - k sekvenci datumů KARTÉZÁKEM najoinujeme list p_key
                                     - omezený na hranici datumů ve kterých máme pro p_key ceny
                                     */
                                    SELECT
                                        "s"."DateSeq",
                                        "p"."p_key"
                                        FROM
                                            "sekvence" "s"
                                                FULL JOIN (
                                                /*
                                                 - list p_key a hraničních datumů ve kterých máme ceny
                                                 */
                                                SELECT DISTINCT
                                                    "p_key",
                                                    "min_d",
                                                    "max_d"
                                                    FROM "produkty") "p"
                                                          ON "s"."DateSeq" >= "p"."min_d" AND "s"."DateSeq" <= "p"."max_d"
                                ) "t1"
                                    LEFT JOIN "produkty" "t2"
                                              ON "t1"."p_key" = "t2"."p_key" AND "t2"."d" = "t1"."DateSeq"
                    ) "tf"
                ORDER BY
                    "p_key", "d"
;
--next_querry
CREATE TABLE "final" AS
/*
 - tohle už je jen očištění a filtrace
 - asi by to mohlo být všechno dohromady v jedné query, ale líp se mi to v DataGripu čte
 - poslední co tady vyrobím, je json pro DynamoDB
   - sestavím objekty pro každý den a zagreguju je do pole
   - je důležité, aby to předtím bylo seřazené - viz ORDER BY dole
 */
SELECT
    "tof"."p_key"                                      AS "p_key",
    /*
     - objekt obsahuje date,originalPrice,currentPrice - seknuté na první písmenka, aby byl json menší
     - array to groupne pro p_key a WITHIN GROUP je tady jen pro pořadí datumů
     - na char to převádím proto, že v Storage API není podpora VARIANTu
     */
    to_char(array_agg(
            object_construct(
                    'd', "tof"."d",
                    'o', "tof"."o",
                    'c', "tof"."c"
                )
        ) WITHIN GROUP (ORDER BY "tof"."d"::DATE ASC)) AS "json"
    FROM "temp_final" "tof"
    WHERE
        "type2" = 'nechat'
    GROUP BY
        "p_key"
;
--next_querry
CREATE or replace TABLE "final_s3" AS
SELECT
    "tof"."p_key"                                      AS "p_key",
    to_char(array_agg(
            object_construct_KEEP_NULL(
                    'd', "tof"."d",
                    'o', try_to_number("tof"."o",12,2),
                    'c', try_to_number("tof"."c",12,2)
                )
        ) WITHIN GROUP (ORDER BY "tof"."d"::DATE ASC)) AS "json"
    FROM "temp_final" "tof"
    WHERE
        "type2" = 'nechat'
    GROUP BY
        "p_key"
;