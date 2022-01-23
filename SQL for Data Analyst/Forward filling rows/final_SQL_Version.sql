WITH firstTable AS (
SELECT
deviceid,
firstTime,
Version1,
Version2,
COUNT(Version1) OVER (PARTITION BY deviceid ORDER BY firstTime) As Version1Group,
COUNT(Version2) OVER (PARTITION BY deviceid ORDER BY firstTime) As Version2Group
FROM
(
SELECT * FROM (
SELECT deviceid,
	MIN(datetime) firstTime,
  metricname,
  metricvalue
  From data
 WHERE metricname in ('Version1','Version2')
 GROUP By deviceid, metricname, metricvalue) tableToBePivoted
 PIVOT(MIN(metricvalue) FOR metricname in ([Version1], [Version2])) As pivotedData  --ORDER BY deviceid, firstTime) 
  ) innerTable
  )
  
SELECT 
  deviceid,
  firstTime,
  Version1,
  Version2,
  COUNT(Version1) OVER (PARTITION BY deviceid ORDER BY firstTime) AS Version1Grouping,
  COUNT(Version2) OVER (PARTITION BY deviceid ORDER BY firstTime) AS Version2Grouping
 FROM firstTable