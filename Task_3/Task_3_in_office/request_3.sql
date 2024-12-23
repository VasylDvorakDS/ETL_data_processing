WITH tb AS (
    SELECT 
        objectid, 
        CONCAT(DATE_FORMAT(StatusTime, '%Y'), ' ', 
		case when Статус='Зарегистрирован' then 'ЗР' , ' ', Группа) AS con 
    FROM tasketl3b
)
SELECT 
    objectid, 
    GROUP_CONCAT(con SEPARATOR '\r\n') AS con 
FROM tb
GROUP BY 1
