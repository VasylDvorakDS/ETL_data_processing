WITH tb AS (
    SELECT 
        objectid, 
        CONCAT(DATE_FORMAT(restime, '%Y'), ' ', 
               CASE 
                   WHEN Статус = 'Зарегистрирован' THEN 'ЗР' 
                   ELSE 'ПР' 
               END, 
               ' ', Группа) AS con 
    FROM tasketl3b
)
SELECT 
    objectid, 
    GROUP_CONCAT(con SEPARATOR '\r\n') AS con 
FROM tb
GROUP BY objectid;
