--REDSHIFT PERFORMANCE QUERIES

--------------------------------------------------------------------------------
-- Issue 1: Incorrect column encoding
--------------------------------------------------------------------------------

--Redshift is column-oriented.
--Data stored by column should also be encoded, which means that it is heavily
--compressed to offer high read performance. 

--Create extended table info view

CREATE OR REPLACE VIEW admin.v_extended_table_info AS
WITH tbl_ids AS
(
  SELECT DISTINCT oid
  FROM pg_class c
  WHERE relowner > 1
  AND   relkind = 'r'
),
scan_alerts AS
(
  SELECT s.tbl AS TABLE,
         Nvl(SUM(CASE WHEN TRIM(SPLIT_PART(l.event,':',1)) = 'Very selective query filter' THEN 1 ELSE 0 END),0) AS selective_scans,
         Nvl(SUM(CASE WHEN TRIM(SPLIT_PART(l.event,':',1)) = 'Scanned a large number of deleted rows' THEN 1 ELSE 0 END),0) AS delrows_scans
  FROM stl_alert_event_log AS l
    JOIN stl_scan AS s
      ON s.query = l.query
     AND s.slice = l.slice
     AND s.segment = l.segment
     AND s.step = l.step
  WHERE l.userid > 1
  AND   s.slice = 0
  AND   s.tbl IN (SELECT oid FROM tbl_ids)
  AND   l.event_time >= Dateadd (DAY,-7,CURRENT_DATE)
  AND   TRIM(SPLIT_PART(l.event,':',1)) IN ('Very selective query filter','Scanned a large number of deleted rows')
  GROUP BY 1
),
tbl_scans AS
(
  SELECT tbl,
         MAX(endtime) last_scan,
         Nvl(COUNT(DISTINCT query || LPAD(segment,3,'0')),0) num_scans
  FROM stl_scan s
  WHERE s.userid > 1
  AND   s.tbl IN (SELECT oid FROM tbl_ids)
  GROUP BY tbl
),
rr_scans AS
(
SELECT tbl,
NVL(SUM(CASE WHEN is_rrscan='t' THEN 1 ELSE 0 END),0) rr_scans,
NVL(SUM(CASE WHEN p.info like 'Filter:%' and p.nodeid > 0 THEN 1 ELSE 0 END),0) filtered_scans,
Nvl(COUNT(DISTINCT s.query || LPAD(s.segment,3,'0')),0) num_scans
  FROM stl_scan s
  JOIN stl_plan_info i on (s.userid=i.userid and s.query=i.query and s.segment=i.segment and s.step=i.step)
  JOIN stl_explain p on ( i.userid=p.userid and i.query=p.query and i.nodeid=p.nodeid  )
  WHERE s.userid > 1
  AND s.type = 2
  AND s.slice = 0
  AND   s.tbl IN (SELECT oid FROM tbl_ids)
  GROUP BY tbl
),
pcon AS
(
  SELECT conrelid,
         CASE
           WHEN SUM(
             CASE
               WHEN contype = 'p' THEN 1
               ELSE 0
             END 
           ) > 0 THEN 'Y'
           ELSE NULL
         END pk,
         CASE
           WHEN SUM(
             CASE
               WHEN contype = 'f' THEN 1
               ELSE 0
             END 
           ) > 0 THEN 'Y'
           ELSE NULL
         END fk
  FROM pg_constraint
  WHERE conrelid > 0
  AND   conrelid IN (SELECT oid FROM tbl_ids)
  GROUP BY conrelid
),
colenc AS
(
  SELECT attrelid,
         SUM(CASE WHEN a.attencodingtype = 0 THEN 0 ELSE 1 END) AS encoded_cols,
         COUNT(*) AS cols
  FROM pg_attribute a
  WHERE a.attrelid IN (SELECT oid FROM tbl_ids)
  AND   a.attnum > 0
  GROUP BY a.attrelid
),
stp AS
(
  SELECT id,
         SUM(ROWS) sum_r,
         SUM(sorted_rows) sum_sr,
         MIN(ROWS) min_r,
         MAX(ROWS) max_r,
         Nvl(COUNT(DISTINCT slice),0) pop_slices
  FROM stv_tbl_perm
  WHERE id IN (SELECT oid FROM tbl_ids)
  AND   slice < 6400
  GROUP BY id
),
cluster_info AS
(
  SELECT COUNT(DISTINCT node) node_count FROM stv_slices
)
SELECT ti.database,
       ti.table_id,
       ti.SCHEMA || '.' || ti."table" AS tablename,
       colenc.encoded_cols || '/' || colenc.cols AS "columns",
       pcon.pk,
       pcon.fk,
       ti.max_varchar,
       CASE
         WHEN ti.diststyle NOT IN ('EVEN','ALL') THEN ti.diststyle || ': ' || ti.skew_rows
         ELSE ti.diststyle
       END AS diststyle,
       CASE
         WHEN ti.sortkey1 IS NOT NULL AND ti.sortkey1_enc IS NOT NULL THEN ti.sortkey1 || '(' || nvl (skew_sortkey1,0) || ')'
         WHEN ti.sortkey1 IS NOT NULL THEN ti.sortkey1
         ELSE NULL
       END AS "sortkey",
       ti.size || '/' || CASE
         WHEN stp.sum_r = stp.sum_sr OR stp.sum_sr = 0 THEN
           CASE
             WHEN "diststyle" = 'EVEN' THEN (stp.pop_slices*(colenc.cols + 3))
             WHEN SUBSTRING("diststyle",1,3) = 'KEY' THEN (stp.pop_slices*(colenc.cols + 3))
             WHEN "diststyle" = 'ALL' THEN (cluster_info.node_count*(colenc.cols + 3))
           END 
         ELSE
           CASE
             WHEN "diststyle" = 'EVEN' THEN (stp.pop_slices*(colenc.cols + 3)*2)
             WHEN SUBSTRING("diststyle",1,3) = 'KEY' THEN (stp.pop_slices*(colenc.cols + 3)*2)
             WHEN "diststyle" = 'ALL' THEN (cluster_info.node_count*(colenc.cols + 3)*2)
           END 
         END|| ' (' || ti.pct_used || ')' AS size,
         ti.tbl_rows,
         ti.unsorted,
         ti.stats_off,
         Nvl(tbl_scans.num_scans,0) || ':' || Nvl(rr_scans.rr_scans,0) || ':' || Nvl(rr_scans.filtered_scans,0) || ':' || Nvl(scan_alerts.selective_scans,0) || ':' || Nvl(scan_alerts.delrows_scans,0) AS "scans:rr:filt:sel:del",tbl_scans.last_scan 
FROM svv_table_info ti 
LEFT JOIN colenc ON colenc.attrelid = ti.table_id 
LEFT JOIN stp ON stp.id = ti.table_id 
LEFT JOIN tbl_scans ON tbl_scans.tbl = ti.table_id 
LEFT JOIN rr_scans ON rr_scans.tbl = ti.table_id
LEFT JOIN pcon ON pcon.conrelid = ti.table_id 
LEFT JOIN scan_alerts ON scan_alerts.table = ti.table_id 
CROSS JOIN cluster_info 
WHERE ti.SCHEMA NOT IN ('pg_internal') 
ORDER BY ti.pct_used DESC;

--Determine tables with no encoding
SELECT database, tablename, columns 
FROM admin.v_extended_table_info 
ORDER BY database;

--review tables and column which aren't encoded
SELECT trim(n.nspname || '.' || c.relname) AS "table",
  trim(a.attname) AS "column",
  format_type(a.atttypid, a.atttypmod) AS "type", 
  format_encoding(a.attencodingtype::integer) AS "encoding", 
  a.attsortkeyord AS "sortkey"
FROM pg_namespace n, pg_class c, pg_attribute a 
WHERE n.oid = c.relnamespace 
AND c.oid = a.attrelid 
AND a.attnum > 0 
AND NOT a.attisdropped and n.nspname NOT IN ('information_schema','pg_catalog','pg_toast') 
AND format_encoding(a.attencodingtype::integer) = 'none' 
AND c.relkind='r' 
AND a.attsortkeyord != 1 
ORDER BY n.nspname, c.relname, a.attnum;


--------------------------------------------------------------------------------
-- Issue 2: Skewed Data
--------------------------------------------------------------------------------

--Amazon Redshift is a distributed, shared nothing database architecture where each
--node in the cluster stores a subset of the data. Each node is further subdivided
--into slices, with each slice having one or more dedicated cores.
--We want uniform distribution/low skew : Ideally, each unique value in the distribution
--key column should occur in the table about the same number of times. This allows
--Amazon Redshift to put the same number of records on each slice in the cluster.
--A skewed distribution key results in slices not working equally hard as each other
--during query execution, requiring unbalanced CPU or memory, and ultimately only 
--running as fast as the slowest slice.
--Indicative Behavior: you see uneven node performance on the cluster. 

-- Check how data blocks in a distribution key map to the slices and nodes in the cluster

SELECT SCHEMA schemaname,
       "table" tablename,
       table_id tableid,
       size size_in_mb,
       CASE
         WHEN diststyle NOT IN ('EVEN','ALL') THEN 1
         ELSE 0
       END has_dist_key,
       CASE
         WHEN sortkey1 IS NOT NULL THEN 1
         ELSE 0
       END has_sort_key,
       CASE
         WHEN encoded = 'Y' THEN 1
         ELSE 0
       END has_col_encoding,
       ROUND(100*CAST(max_blocks_per_slice - min_blocks_per_slice AS FLOAT) / GREATEST(NVL (min_blocks_per_slice,0)::int,1),2) ratio_skew_across_slices,
       ROUND(CAST(100*dist_slice AS FLOAT) /(SELECT COUNT(DISTINCT slice) FROM stv_slices),2) pct_slices_populated
FROM svv_table_info ti
  JOIN (SELECT tbl,
               MIN(c) min_blocks_per_slice,
               MAX(c) max_blocks_per_slice,
               COUNT(DISTINCT slice) dist_slice
        FROM (SELECT b.tbl,
                     b.slice,
                     COUNT(*) AS c
              FROM STV_BLOCKLIST b
              GROUP BY b.tbl,
                       b.slice)
        WHERE tbl IN (SELECT table_id FROM svv_table_info)
        GROUP BY tbl) iq ON iq.tbl = ti.table_id
ORDER BY SCHEMA, "table";

--If you have tables with skewed distribution keys, lconsider changing the distribution key to  column 
--that exhibits high cardinality and uniform distribution. 
-- To evaluate a candidate column as a distribution key , create a new table using CTAS

CREATE TABLE my_test_table DISTKEY (<column name>) AS SELECT <column name> FROM <table name>;

--Recheck the data blocks using query in line 202.

--------------------------------------------------------------------------------
-- Issue 3: Queries not benefiting from sort keys
--------------------------------------------------------------------------------

--Amazon Redshift tables can have a sort key column identified, which acts like an index in other 
--databases, but which does not incur a storage cost as with other platforms (for more information,
--see Choosing Sort Keys). A sort key should be created on those columns which are most commonly 
--used in WHERE clauses.
--Compound keys: columns queried unevenly, Interleaved: evenly queried columns
--If using compound sort keys, review your queries to ensure that their WHERE clauses specify 
--the sort columns in the same order they were defined in the compound key.

-- Determine tables  that don't have sort keys (query against the view created in issue 1, line 12)	
SELECT * FROM admin.v_extended_table_info WHERE sortkey IS null;


--Generate a list of recommened sort keys based on query activity

SELECT ti.schemaname||'.'||ti.tablename AS "table", 
  ti.tbl_rows, 
  avg(r.s_rows_pre_filter) avg_s_rows_pre_filter, 
  round(1::float - avg(r.s_rows_pre_filter)::float/ti.tbl_rows::float,6) avg_prune_pct, 
  avg(r.s_rows) avg_s_rows, 
  round(1::float - avg(r.s_rows)::float/avg(r.s_rows_pre_filter)::float,6) avg_filter_pct, 
  ti.diststyle, 
  ti.sortkey_num, 
  ti.sortkey1, 
  trim(a.typname) "type", 
  count(distinct i.query) * avg(r.time) AS total_scan_secs, 
  avg(r.time) AS scan_time, 
  count(distinct i.query) AS num, 
  max(i.query) AS query, 
  trim(info) AS filter 
FROM stl_explain p 
JOIN stl_plan_info i 
ON (i.userid=p.userid AND i.query=p.query AND i.nodeid=p.nodeid ) 

--------------------------------------------------------------------------------
-- Issue 4: Tables without statistics or which need vacuum
--------------------------------------------------------------------------------

--Table statistics are used by the query optimizer during execution planning. 
--Absent or stale statistics may result in inefficient query execution.

--Determine tables with missing statistics

SELECT substring(trim(plannode),1,100) AS plannode
       ,COUNT(*)
FROM stl_explain
WHERE plannode LIKE '%missing statistics%'
AND plannode NOT LIKE '%redshift_auto_health_check_%'
GROUP BY plannode
ORDER BY 2 DESC;

--Determine tables with stale statistics

SELECT database, schema || '.' || "table" AS "table", stats_off 
FROM svv_table_info 
WHERE stats_off > 5 
ORDER BY 2;

--Rows deletions imply only logical deletion. That means that "deleted" data
--are stored in the disk until a vacuum operation is perfomed. 

-- Identifying tables that scan a large number of deleted rows in the last seven days 

select trim(s.perm_table_name) as table , (sum(abs(datediff(seconds, coalesce(b.starttime,d.starttime,s.starttime), case when coalesce(b.endtime,d.endtime,s.endtime) > coalesce(b.starttime,d.starttime,s.starttime) THEN coalesce(b.endtime,d.endtime,s.endtime) ELSE coalesce(b.starttime,d.starttime,s.starttime) END )))/60)::numeric(24,0) as minutes,
       sum(coalesce(b.rows,d.rows,s.rows)) as rows, trim(split_part(l.event,':',1)) as event,  substring(trim(l.solution),1,60) as solution , max(l.query) as sample_query, count(distinct l.query)
from stl_alert_event_log as l
left join stl_scan as s on s.query = l.query and s.slice = l.slice and s.segment = l.segment
left join stl_dist as d on d.query = l.query and d.slice = l.slice and d.segment = l.segment
left join stl_bcast as b on b.query = l.query and b.slice = l.slice and b.segment = l.segment
where l.userid >1
and  l.event_time >=  dateadd(day, -7, current_Date)
-- and s.perm_table_name not like 'volt_tt%'
group by 1,4,5 order by 2 desc,6 desc;



--------------------------------------------------------------------------------
-- Issue 5: Tables with large varchar columns
--------------------------------------------------------------------------------

--During query execution intermediate tables created during calculation are stored.
--These tables remain uncompressed and thus consume excessive memory. 

--Determine tables that need maximum column width review
SELECT database, schema || '.' || "table" AS "table", max_varchar 
FROM svv_table_info 
WHERE max_varchar > 150 
ORDER BY 2;

--Determine columns with wide varchar columns along with the maximum width of each wide column
SELECT max(len(rtrim(column_name))) 
FROM table_name;

--------------------------------------------------------------------------------
-- Issue 6: Queries waiting on queue slots
--------------------------------------------------------------------------------

--Redshift runs queries using a quering system known as WLM. A user can define up to 8 queues
--and after set the concurrency to each queue with respect to overall throughput requirements,
--distribute the queries .
--If a query is assigned to a totally busy queue then the query must wait long time before been 
--executed. This is a sign that implies that you may need to increase concurrency.
--Note: Concurrency increase will allow more queries to run but will assign a smaller share of 
--memory to each one of them (possible problem - see Issue #7)


--Determine queries on a WLM Slot 
SELECT w.query
       ,substring(q.querytxt,1,100) AS querytxt
       ,w.queue_start_time
       ,w.service_class AS class
       ,w.slot_count AS slots
       ,w.total_queue_time / 1000000 AS queue_seconds
       ,w.total_exec_time / 1000000 exec_seconds
       ,(w.total_queue_time + w.total_Exec_time) / 1000000 AS total_seconds
FROM stl_wlm_query w
  LEFT JOIN stl_query q
         ON q.query = w.query
        AND q.userid = w.userid
WHERE w.queue_start_Time >= dateadd(day,-7,CURRENT_DATE)
AND   w.total_queue_Time > 0
-- and q.starttime >= dateadd(day, -7, current_Date)    
-- and ( querytxt like 'select%' or querytxt like 'SELECT%' ) 
ORDER BY w.total_queue_time DESC
,w.queue_start_time DESC limit 35

--Review maximum concurrency the cluster needed lately
WITH 
	generate_dt_series AS (select sysdate - (n * interval '1 second') as dt from (select row_number() over () as n from svl_query_report limit 604800)),
	-- generate_dt_series AS (select sysdate - (n * interval '1 second') as dt from (select row_number() over () as n from [table_with_604800_rows] limit 604800)),
	apex AS (SELECT iq.dt, iq.service_class, iq.num_query_tasks, count(iq.slot_count) as service_class_queries, sum(iq.slot_count) as service_class_slots
		FROM  
		(select gds.dt, wq.service_class, wscc.num_query_tasks, wq.slot_count
		FROM stl_wlm_query wq
		JOIN stv_wlm_service_class_config wscc ON (wscc.service_class = wq.service_class AND wscc.service_class > 4)
		JOIN generate_dt_series gds ON (wq.service_class_start_time <= gds.dt AND wq.service_class_end_time > gds.dt)
		WHERE wq.userid > 1 AND wq.service_class > 4) iq
	GROUP BY iq.dt, iq.service_class, iq.num_query_tasks),
	maxes as (SELECT apex.service_class, max(service_class_slots) max_service_class_slots 
			from apex group by apex.service_class),
	queued as (	select service_class, max(queue_end_time) max_queue_end_time from stl_wlm_query where total_queue_time > 0 GROUP BY service_class)
select apex.service_class, apex.num_query_tasks as max_wlm_concurrency, apex.service_class_slots as max_service_class_slots, max(apex.dt) max_slots_ts, queued.max_queue_end_time last_queued_time
FROM apex
JOIN maxes ON (apex.service_class = maxes.service_class AND apex.service_class_slots = maxes.max_service_class_slots)
LEFT JOIN queued ON queued.service_class = apex.service_class
GROUP BY  apex.service_class, apex.num_query_tasks, apex.service_class_slots, queued.max_queue_end_time
ORDER BY apex.service_class;

--------------------------------------------------------------------------------
-- Issue 7: Disk-based queries
--------------------------------------------------------------------------------

--During execution a query may need to use temporary disk-based storage if the available 
--memory is not sufficient. This leads to increased I/O and slows down query execution.

--Determine if any queries have been writing to disk
SELECT q.query, trim(q.cat_text)
FROM (
  SELECT query, 
    replace( listagg(text,' ') WITHIN GROUP (ORDER BY sequence), '\\n', ' ') AS cat_text 
    FROM stl_querytext 
    WHERE userid>1 
    GROUP BY query) q
JOIN (
  SELECT distinct query 
  FROM svl_query_summary 
  WHERE is_diskbased='t' 
  AND (LABEL LIKE 'hash%' OR LABEL LIKE 'sort%' OR LABEL LIKE 'aggr%') 
  AND userid > 1) qs 
ON qs.query = q.query;

--------------------------------------------------------------------------------
-- Issue 8: Commit queue waits
--------------------------------------------------------------------------------

--Amazon Redshift is designed for analytics queries rather than transaction processing. 
--Due to the high cost of commit, excessive use will result in increased execution time
--and wait in commit queue.

--Determine how often you commit (last 2 days)
select startqueue,node, datediff(ms,startqueue,startwork) as queue_time, datediff(ms, startwork, endtime) as commit_time, queuelen 
from stl_commit_stats 
where startqueue >=  dateadd(day, -2, current_Date)
order by queuelen desc , queue_time desc;

--------------------------------------------------------------------------------
-- Issue 9: Inefficient data loads
--------------------------------------------------------------------------------

--For data loading the COPY command is recommended. In order to load data as efficiently as possible you should:
--1. Compress the files to be loaded
--2. Load a large number of smaller files instead of a big one file
--3. The number of files ideally must be multiple of the slice count. The number of slices per node
--	 depends on the node size.

--Calculate loading statistics
SELECT a.tbl,
  trim(c.nspname) AS "schema", 
  trim(b.relname) AS "tablename", 
  sum(a.rows_inserted) AS "rows_inserted", 
  sum(d.distinct_files) AS files_scanned,  
  sum(d.MB_scanned) AS MB_scanned, 
  (sum(d.distinct_files)::numeric(19,3)/count(distinct a.query)::numeric(19,3))::numeric(19,3) AS avg_files_per_copy, 
  (sum(d.MB_scanned)/sum(d.distinct_files)::numeric(19,3))::numeric(19,3) AS avg_file_size_mb, 
  count(distinct a.query) no_of_copy, 
  max(a.query) AS sample_query, 
  (sum(d.MB_scanned)*1024*1000000/SUM(d.load_micro)) AS scan_rate_kbps, 
  (sum(a.rows_inserted)*1000000/SUM(a.insert_micro)) AS insert_rate_rows_ps 
FROM 
  (SELECT query, 
    tbl, 
    sum(rows) AS rows_inserted, 
    max(endtime) AS endtime, 
    datediff('microsecond',min(starttime),max(endtime)) AS insert_micro 
  FROM stl_insert 
  GROUP BY query, tbl) a,      
  pg_class b, 
  pg_namespace c,                 
  (SELECT b.query, 
    count(distinct b.bucket||b.key) AS distinct_files, 
    sum(b.transfer_size)/1024/1024 AS MB_scanned, 
    sum(b.transfer_time) AS load_micro 
  FROM stl_s3client b 
  WHERE b.http_method = 'GET' 
  GROUP BY b.query) d 
WHERE a.tbl = b.oid AND b.relnamespace = c.oid AND d.query = a.query 
GROUP BY 1,2,3 
ORDER BY 4 desc;

--Determine the time taken to load a table along with the time taken to update the statistics table as a time percentage
--of the overall load process.
SELECT a.userid, 
  a.query, 
  round(b.comp_time::float/1000::float,2) comp_sec, 
  round(a.copy_time::float/1000::float,2) load_sec, 
  round(100*b.comp_time::float/(b.comp_time + a.copy_time)::float,2) ||'%' pct_complyze, 
  substring(q.querytxt,1,50) 
FROM (
  SELECT userid, 
    query, 
    xid, 
    datediff(ms,starttime,endtime) copy_time 
  FROM stl_query q 
  WHERE (querytxt ILIKE 'copy %from%') 
  AND exists (
    SELECT 1 
    FROM stl_commit_stats cs 
    WHERE cs.xid=q.xid) 
  AND exists (
    SELECT xid 
    FROM stl_query 
    WHERE query IN (
      SELECT distinct query 
      FROM stl_load_commits))) a 
LEFT JOIN (
  SELECT xid, 
    sum(datediff(ms,starttime,endtime)) comp_time 
  FROM stl_query q 
  WHERE (querytxt LIKE 'COPY ANALYZE %' OR querytxt LIKE 'analyze compression phase %') 
  AND exists (
    SELECT 1 
    FROM stl_commit_stats cs 
    WHERE cs.xid=q.xid) 
  AND exists (
    SELECT xid 
    FROM stl_query 
    WHERE query IN (
      SELECT distinct query 
      FROM stl_load_commits)) 
  GROUP BY 1) b 
ON b.xid = a.xid 
JOIN stl_query q 
ON q.query = a.query 
WHERE (b.comp_time IS NOT null) 
ORDER BY 6,5;

--------------------------------------------------------------------------------
-- Issue 10: Inefficient use of temporary tables
--------------------------------------------------------------------------------

--These default storage properties may cause issues if not carefully considered. 
--Amazon Redshift’s default table structure is to use EVEN distribution with no 
--column encoding. This is a sub-optimal data structure for many types of queries, 
--and if you are using the SELECT…INTO syntax you cannot set the column encoding 
--or distribution and sort keys.
--It is highly recommended that you convert all SELECT…INTO syntax to use the 
--CREATE statement. This ensures that your temporary tables have column encodings 
--and are distributed in a fashion that is sympathetic to the other entities that 
--are part of the workflow. 


--Example of statement conversion
--Initial statement
SELECT column_a, column_b INTO #my_temp_table FROM my_table;

--Converted statement
BEGIN;
CREATE TEMPORARY TABLE my_temp_table(
column_a varchar(128) encode lzo,
column_b char(4) encode bytedict)
distkey (column_a) -- Assuming you intend to join this table on column_a
sortkey (column_b); -- Assuming you are sorting or grouping by column_b
 
INSERT INTO my_temp_table SELECT column_a, column_b FROM my_table;
COMMIT;

--Analyze temporary table statistics
ANALYZE my_temp_table;

--This way  you retain the functionality of using temp tables but control
--data placement on the cluster through distribution keu assignment and take
--advantage of the columnar nature of Amazon Redshift

--------------------------------------------------------------------------------
-- Issue 11: Nested loops - Cross joins
--------------------------------------------------------------------------------

--If a nested loop is present, a nested loop alert will occur. To fix this, review the
--query cross-join and remove them if possible (cross-joins are joins without a join 
--condition that results in the cartesian product of two tables). They are executed as 
--nested loop and are the most time consuming of all joins. 

--Determine  queries that raise nested loop alerts
select query, trim(querytxt) as SQL, starttime 
from stl_query 
where query in (
select distinct query 
from stl_alert_event_log 
where event like 'Nested Loop Join in the query plan%') 
order by starttime desc;

--------------------------------------------------------------------------------
-- Issue 12: Review Query alerts
--------------------------------------------------------------------------------

--Determine tables with alerts and most common alerts for these tables
select trim(s.perm_table_name) as table, 
(sum(abs(datediff(seconds, s.starttime, s.endtime)))/60)::numeric(24,0) as minutes, trim(split_part(l.event,':',1)) as event,  trim(l.solution) as solution, 
max(l.query) as sample_query, count(*) 
from stl_alert_event_log as l 
left join stl_scan as s on s.query = l.query and s.slice = l.slice 
and s.segment = l.segment and s.step = l.step
where l.event_time >=  dateadd(day, -7, current_Date) 
group by 1,3,4 
order by 2 desc,6 desc;


--------------------------------------------------------------------------------
-- Issue 13: Time consuming queries (in general)
--------------------------------------------------------------------------------

--Determine the top 50 moste time consuming queries performed the last 7 days
select trim(database) as db, count(query) as n_qry, 
max(substring (qrytext,1,80)) as qrytext, 
min(run_minutes) as "min" , 
max(run_minutes) as "max", 
avg(run_minutes) as "avg", sum(run_minutes) as total,  
max(query) as max_query_id, 
max(starttime)::date as last_run, 
sum(alerts) as alerts, aborted
from (select userid, label, stl_query.query, 
trim(database) as database, 
trim(querytxt) as qrytext, 
md5(trim(querytxt)) as qry_md5, 
starttime, endtime, 
(datediff(seconds, starttime,endtime)::numeric(12,2))/60 as run_minutes,     
alrt.num_events as alerts, aborted 
from stl_query 
left outer join 
(select query, 1 as num_events from stl_alert_event_log group by query ) as alrt 
on alrt.query = stl_query.query
where userid <> 1 and starttime >=  dateadd(day, -7, current_date)) 
group by database, label, qry_md5, aborted
order by total desc limit 50;


--------------------------------------------------------------------------------
-- Issue 14: Run out of disk space
--------------------------------------------------------------------------------

--If the cluster is full , queries will start to fail as there won't be available
--space for creating intermediate tables during query execution. Vaccums will also 
--fail if there isn't enough free space to store intermediate data.

--Determine available space
select sum(used)::float / sum(capacity) as pct_full
from stv_partitions

--Keeping track of individual table size
select t.name, count(tbl) / 1000.0 as gb
from (
    select distinct datname id, name
    from stv_tbl_perm 
        join pg_database on pg_database.oid = db_id
    ) t
join stv_blocklist on tbl=t.id
group by t.name order by gb desc

--Based on this you can drop unnecessary tables or resize the cluster in order to 
--have more capacity


--------------------------------------------------------------------------------
-- Issue 15: Monitoring CPU usage
--------------------------------------------------------------------------------

--In order to check if our joined tables are well distributed we can use the svv_diskusage table
--which gives us a good sense of how our data is distibuted among the slices.
--Monitor CPU usage

select slice, col, num_values, maxvalue
from svv_diskusage
where name= 'real_time_data' and col = 0
order by slice, col;

--Data is not well distributed if the num_values column doesn't contain relatively similar values. 