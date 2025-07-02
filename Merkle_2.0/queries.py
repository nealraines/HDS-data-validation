material_data = """
SELECT
    LTRIM(mara.matnr, 0) AS "material_number",
    product.prodcat AS "product_category",
    mara.meins AS "base_uom",
    marm.meinh AS "alt_uom",
    marm.umrez AS "conversion_numerator",
    marm.umren AS "conversion_denominator",
    marm.ean11 AS "upc",
    marm.laeng AS "length",
    marm.breit AS "width",
    marm.hoehe AS "height",
    marm.volum AS "volume",
    marm.brgew AS "gross_weight"
FROM
    edp.std_ecc.mara mara
LEFT OUTER JOIN
    edp.std_ecc.eina eina
        ON LTRIM(mara.matnr, 0) = LTRIM(eina.matnr, 0)
LEFT OUTER JOIN
    edp.std_ecc.marm marm
        ON LTRIM(mara.matnr, 0) = LTRIM(marm.matnr, 0)
LEFT OUTER JOIN
    EDP.STD_ENABLE.VW_EW_MATERIAL_PROD product
        ON LTRIM(eina.matnr, 0) = product.material_number
WHERE
    mara.mtpos_mara = 'ZNOR'
    AND mara.mstae = 'RL'
    AND mara.mstav IN ('RL', 'NW')
    AND mara.mtart IN ('HAWA', 'HALB')
    AND eina.relif = 'X'
    AND eina.lifnr <> '2000500754'
ORDER BY
    LTRIM(mara.matnr, 0),
    marm.umrez
"""


duplicate_upc = """
WITH CTE_DUPLICATE_UPC
AS
(
SELECT
    EAN11
FROM
    edp.std_ecc.mean
GROUP BY
    EAN11
HAVING
    COUNT(*) > 1
)

SELECT
    CONCAT(LTRIM(mean.matnr, 0),' - ', mean.meinh) AS "error_message",
    mean.ean11 AS "upc"
FROM
    edp.std_ecc.mean mean
INNER JOIN
    CTE_DUPLICATE_UPC dupe
        ON mean.ean11 = dupe.ean11
ORDER BY
    mean.ean11
"""
