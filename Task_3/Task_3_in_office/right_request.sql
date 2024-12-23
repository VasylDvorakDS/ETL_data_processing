SELECT objectid,FROM_UNIXTIME (restime)restime,(LEAD( restime) OVER(PARTITION BY objectid ORDER BY restime)-
restime)/3600 Длительность,
case when Статус IS NULL then @Prev_1 ELSE @Prev_1:=Статус END Статус,  
case when Группа IS NULL then @Prev_2 ELSE @Prev_2:=Группа END Группа  
FROM (SELECT objectid, restime, Статус, if(ROW_NUMBER()OVER(PARTITION 
BY  objectid ORDER BY restime)=1 AND Назначение IS NULL, '', Группа)Группа, Назначение  
FROM (SELECT a.objectid, a.restime, Статус, Группа, Назначение, (SELECT @Prev_1:=''), (SELECT @Prev_2:='') 
FROM (SELECT DISTINCT objectid, restime FROM tasketl3a WHERE fieldname IN ('Status', 'GNAme2'))a

LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Статус FROM tasketl3a WHERE fieldname IN ('Status'))a1
ON a.objectid = a1.objectid AND a.restime=a1.restime

LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Группа,1 Назначение FROM tasketl3a WHERE fieldname IN ('gname2'))a2
ON a.objectid = a2.objectid AND a.restime=a2.restime)b1)b2

