package alpha;

public class AlphaSql {

    public static final String QRY =
        "SELECT " +
        "    TRADER, " +

        "    CASE CUR_NO " +
        "        WHEN 1 THEN CURRENCY1 " +
        "        WHEN 2 THEN CURRENCY2 " +
        "    END AS CURRENCY, " +

        "    SUM( " +
        "        CASE " +
        "            WHEN CUR_NO = 1 AND ((DEAL_TYPE = 'Buy' AND FAR_LEG = TRUE) OR (DEAL_TYPE = 'Sell' AND FAR_LEG = FALSE)) THEN ABS(CUR1_AMOUNT) " +
        "            WHEN CUR_NO = 2 AND ((DEAL_TYPE = 'Buy' AND FAR_LEG = FALSE) OR (DEAL_TYPE = 'Sell' AND FAR_LEG = TRUE)) THEN ABS(CUR2_AMOUNT) " +
        "            ELSE 0  " +
        "        END " +
        "    ) AS OBLIGATIONS, " +

        "    SUM( " +
        "        CASE " +
        "            WHEN CUR_NO = 1 AND ((DEAL_TYPE = 'Buy' AND FAR_LEG = FALSE) OR (DEAL_TYPE = 'Sell' AND FAR_LEG = TRUE)) THEN ABS(CUR1_AMOUNT) " +
        "            WHEN CUR_NO = 2 AND ((DEAL_TYPE = 'Buy' AND FAR_LEG = TRUE) OR (DEAL_TYPE = 'Sell' AND FAR_LEG = FALSE)) THEN ABS(CUR2_AMOUNT) " +
        "            ELSE 0  " +
        "        END " +
        "    ) AS DEMANDS " +

        "FROM OrderPojo " +
        "    LEFT JOIN CONTRACTORS.Contractor as contractors ON OrderPojo.BUYER_LEGAL_ENTITY = contractors.ID_LE " +
        "    LEFT JOIN CONTRACTORS.Contractor as contractors2 ON OrderPojo.SELLER_LEGAL_ENTITY = contractors2.ID_LE " +
        "    CROSS JOIN (SELECT C1 CUR_NO FROM (VALUES (1), (2)) AS V) AS CURS " +

        "WHERE (contractors.ID_LE IS NOT NULL OR contractors2.ID_LE IS NOT NULL) " +

        "GROUP  " +
        "    BY TRADER, " +
        "    CASE CUR_NO " +
        "        WHEN 1 THEN CURRENCY1 " +
        "        WHEN 2 THEN CURRENCY2  " +
        "    END";
}