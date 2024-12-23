spark_task_3SELECT 
    ID_тикета, 
    from_unixtime(StatusTime) AS StatusTime,
    (lead(StatusTime) OVER (PARTITION BY ID_тикета ORDER BY StatusTime) - StatusTime) / 3600 AS Длительность_статуса_на_текущей_группе,
    CASE 
        WHEN Статус IS NULL THEN @PREV1
        ELSE @PREV1 := Статус 
    END AS Статус,
    CASE 
        WHEN Группа IS NULL THEN @PREV2
        ELSE @PREV2 := Группа 
    END AS Группа,
    Назначения
FROM (
    SELECT 
        ID_тикета, 
        StatusTime, 
        Статус,
        IF(ROW_NUMBER() OVER (PARTITION BY ID_тикета ORDER BY StatusTime) = 1 AND Назначения IS NULL, '', Группа) AS Группа,
        Назначения
    FROM (
        SELECT 
            DISTINCT a.objectid AS ID_тикета, 
            a.restime AS StatusTime, 
            Статус, 
            Группа, 
            Назначения
        FROM (
            SELECT DISTINCT restime, objectid 
            FROM spark.tasketl3a 
            WHERE fieldname IN ('Status', 'GNAME2')
        ) a
    LEFT JOIN 
    (
        SELECT DISTINCT objectid, restime, fieldvalue AS Статус 
        FROM spark.tasketl3a a 
        WHERE fieldname IN ('Status')
    ) a1 
    ON a.OBJECTID = a1.objectid AND a.restime = a1.restime
    LEFT JOIN 
    (
    SELECT DISTINCT objectid, restime, fieldvalue AS Группа, 1 AS Назначения 
    FROM spark.tasketl3a a 
    WHERE fieldname IN ('GNAME2')
    ) a2 
    ON a.OBJECTID = a2.objectid AND a.restime = a2.restime
    )b1)b2