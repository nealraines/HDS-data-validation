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
    AND marm.ZSOURCE_FLAG IS NULL
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12
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


cubiscan_material_data = """
SELECT
    cubiscan.material AS "material_number",
    product.prodcat AS "product_category",
    mara.meins AS "base_uom",
    cubiscan.uom AS "alt_uom",
    cubiscan.quantity AS "conversion_numerator",
    1 AS "conversion_denominator",
    cubiscan.upc AS "upc",
    cubiscan.length AS "length",
    cubiscan.width AS "width",
    cubiscan.height AS "height",
    ROUND(cubiscan.length * cubiscan.width * cubiscan.height / 1728, 3) AS "volume",
    cubiscan.weight AS "gross_weight"
FROM
    EDP.STD_CUBISCAN.SRC_CUBISCAN_AUDIT_DATA cubiscan
LEFT OUTER JOIN
    EDP.STD_ENABLE.VW_EW_MATERIAL_PROD product
        ON cubiscan.material = product.material_number
LEFT OUTER JOIN
    edp.std_ecc.mara mara
        ON cubiscan.material = LTRIM(mara.matnr, 0)
        AND mara.mtpos_mara = 'ZNOR'
        AND mara.mstae = 'RL'
        AND mara.mstav IN ('RL', 'NW')
        AND mara.mtart IN ('HAWA', 'HALB')
WHERE
    cubiscan.date_time >= '2025-06-01'
    AND cubiscan.date_time = (SELECT
                                  MAX(date_time)
                              FROM
                                  EDP.STD_CUBISCAN.SRC_CUBISCAN_AUDIT_DATA
                              WHERE
                                  material = cubiscan.material
                                  AND uom = cubiscan.uom)
ORDER BY
    1, 5
"""


adhoc_material_data = """
SELECT
    ven_col.material_number AS "material_number",
    product.prodcat AS "product_category",
    ven_col.base_uom AS "base_uom",
    ven_col.alt_uom AS "alt_uom",
    ven_col.conversion_numerator AS "conversion_numerator",
    ven_col.conversion_denominator AS "conversion_denominator",
    ven_col.upc AS "upc",
    ven_col.length AS "length",
    ven_col.width AS "width",
    ven_col.height AS "height",
    ven_col.volume AS "volume",
    ven_col.gross_weight AS "gross_weight"
FROM
    dm_data_governance.patrol_tower.vendor_collection ven_col
LEFT OUTER JOIN
    EDP.STD_ENABLE.VW_EW_MATERIAL_PROD product
        ON ven_col.material_number = product.material_number
WHERE
    ven_col.report_date = (SELECT
                               MAX(report_date)
                           FROM
                               dm_data_governance.patrol_tower.vendor_collection
                           WHERE
                               ven_col.material_number = material_number
                               AND ven_col.alt_uom = alt_uom)
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY
    ven_col.material_number,
    ven_col.conversion_denominator DESC,
    ven_col.conversion_numerator
"""
