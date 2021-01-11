SELECT SUM(usuario_retenido) / COUNT(user_id) AS retention_rate
FROM (
SELECT A.user_id AS user_id, CASE WHEN B.user_id IS NOT NULL THEN 1 ELSE 0 END AS usuario_retenido
FROM
(
    SELECT DISTINCT `User ID` AS user_id
    FROM cashins
    WHERE `Created At` < DATE_SUB((SELECT MAX(`Created At`) FROM cashins),INTERVAL 4 WEEK) -- Si los datos son actuales, cambiamos por DATE_SUB(CURDATE(),INTERVAL 4 WEEK)
) A
LEFT JOIN
(
    SELECT DISTINCT `User ID` AS user_id
    FROM cashins
    WHERE `Created At` >= DATE_SUB((SELECT MAX(`Created At`) FROM cashins),INTERVAL 4 WEEK) -- Si los datos son actuales, cambiamos por DATE_SUB(CURDATE(),INTERVAL 4 WEEK)
) B
ON A.user_id = B.user_id) tmp;