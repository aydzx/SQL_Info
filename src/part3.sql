-- Active: 1695727197563@@127.0.0.1@5432@pallaaxe

------------------1--------------------
CREATE OR REPLACE FUNCTION part3_task1() 
RETURNS TABLE (Peer1 varchar, Peer2 varchar, PointsAmount bigint) AS $$
BEGIN
RETURN QUERY
SELECT tp1.checkingpeer AS peer1, tp1.checkedpeer AS peer2, COALESCE(tp1.points_amount - tp2.points_amount, tp1.points_amount) AS points_amount 
FROM transferred_points tp1
LEFT JOIN transferred_points tp2 ON tp1.checkingpeer = tp2.checkedpeer AND  tp1.checkedpeer = tp2.checkingpeer;
END;
$$ LANGUAGE PLPGSQL;

SELECT* FROM part3_task1();
------------------2--------------------
CREATE OR REPLACE FUNCTION part3_task2() RETURNS TABLE(nickname VARCHAR,task VARCHAR, xp INTEGER ) AS $$
SELECT checks.peer,checks.task,xp.xp_amount
FROM checks
JOIN xp ON xp."check" = checks.id
$$ LANGUAGE SQL;


SELECT* FROM part3_task2(); 

------------------3--------------------

CREATE OR REPLACE FUNCTION part3_task3(Date_parameter date) RETURNS TABLE (nickname VARCHAR) AS $$
BEGIN
RETURN QUERY
SELECT peer 
FROM time_tracking
GROUP BY peer, date
HAVING COUNT(*) < 3 AND "date" = Date_parameter;
END;
$$ LANGUAGE PLPGSQL;


SELECT * FROM part3_task3('2023-03-22');

------------------4--------------------

CREATE OR REPLACE FUNCTION part3_task4() RETURNS TABLE (peer VARCHAR , point_change NUMERIC ) AS $$
BEGIN 
RETURN QUERY 
WITH earned AS (SELECT checkingpeer,SUM(points_amount) AS e FROM transferred_points GROUP BY checkingpeer),
spent AS (SELECT checkedpeer, SUM(points_amount) AS s  FROM transferred_points GROUP BY  checkedpeer)
SELECT nickname AS Peer, earned.e - spent.s AS PointsChange FROM peers
JOIN earned ON earned.checkingpeer = nickname
JOIN spent ON spent.checkedpeer = nickname
ORDER BY PointsChange DESC;
END;
$$ LANGUAGE PLPGSQL;


SELECT * FROM part3_task4();



------------------5--------------------

CREATE OR REPLACE FUNCTION part3_task5()
RETURNS TABLE (peer VARCHAR, summa NUMERIC)
AS $$
BEGIN RETURN QUERY
    with tmp as (
    SELECT Peer1, sum(PointsAmount) as sm FROM part3_task1() 
    group by Peer1
    union
    SELECT Peer2, -sum(PointsAmount) as sm FROM part3_task1() 
    group by Peer2)
    select Peer1, sum(sm) FROM tmp
    group by Peer1
    ORDER BY 2 DESC;
END;
$$ LANGUAGE plpgsql;




SELECT * FROM part3_task5();



------------------6--------------------
CREATE OR REPLACE FUNCTION part3_task6() RETURNS TABLE ("date" DATE, task VARCHAR) AS $$
BEGIN
RETURN QUERY
WITH ct AS  (SELECT checks."date", checks.task , COUNT(*) AS count_task 
FROM checks 
JOIN p2p ON checks.id = p2p."check"
GROUP BY checks.date,checks.task ),
mt AS (
    SELECT ct."date", MAX(count_task) AS max_check
    FROM ct
    GROUP BY ct."date"
)
SELECT  mt."date" ,ct.task
FROM mt
JOIN ct ON ct."date" = mt."date" AND  ct.count_task  = mt.max_check;
END;
$$ LANGUAGE PLPGSQL;


SELECT *
FROM  part3_task6()

------------------7--------------------


CREATE OR REPLACE PROCEDURE part3_task7(IN res refcursor, IN block text) AS $$
BEGIN
OPEN res FOR
WITH group_peer_task AS (
SELECT peer,task
FROM checks 
JOIN xp ON xp."check" = checks.id
GROUP BY peer,task),
max_task AS (
    SELECT  MAX(SUBSTRING(title FROM '\D+\d')) AS block_name
    FROM tasks
    WHERE SUBSTRING(title FROM '\D+') = block
),peers_max_task AS (
    SELECT peer AS Peer, MAX(SUBSTRING(group_peer_task.task FROM '\D+\d')) AS max_missing_task
    FROM group_peer_task
    JOIN max_task ON SUBSTRING(group_peer_task.task FROM '\D+\d') = block_name
    WHERE SUBSTRING(group_peer_task.task from '\D+') = block 
    GROUP BY peer
),result AS (
SELECT peers_max_task.peer AS Peer ,checks."date" AS "Day"
FROM peers_max_task 
JOIN checks ON peers_max_task.Peer = checks.peer AND SUBSTRING(checks.task from '\D+\d') = peers_max_task.max_missing_task
)
SELECT DISTINCT ON (Peer) Peer, "Day"
FROM result ORDER BY Peer, "Day" DESC;
END;
$$ LANGUAGE PLPGSQL;

CALL part3_task7('res', 'C');
FETCH ALL "res";


------------------8--------------------


CREATE OR REPLACE PROCEDURE part3_task8(IN res refcursor) AS $$
BEGIN
OPEN res FOR
WITH peer_rec_friend AS(
SELECT peers.nickname AS Peer,recommendations.recommendedpeer AS RecommendedPeer
FROM peers
JOIN friends ON peers.nickname = friends.peer1
JOIN  recommendations ON friends.peer2 = recommendations.peer AND peers.nickname <> recommendations.recommendedpeer
),count_recomendation AS 
(SELECT Peer, RecommendedPeer, COUNT(*) AS count_recomendation_peer
FROM peer_rec_friend
GROUP BY Peer, RecommendedPeer
), max_recomendation AS (SELECT Peer, MAX(count_recomendation_peer) AS max_recomendation_peer
FROM count_recomendation
GROUP BY Peer
)
SELECT max_recomendation.Peer,RecommendedPeer
FROM count_recomendation
JOIN max_recomendation ON count_recomendation.count_recomendation_peer = max_recomendation.max_recomendation_peer
AND  max_recomendation.Peer = count_recomendation.Peer;
END;
$$ LANGUAGE PLPGSQL;

CALL part3_task8('res');
FETCH ALL "res";


------------------9--------------------
CREATE OR REPLACE PROCEDURE part3_task9(IN res refcursor, block1 text, block2 text) AS $$
BEGIN
OPEN res FOR
WITH block_1 AS (
SELECT peer , SUBSTRING(task FROM '\D+') AS block_task
FROM checks
WHERE SUBSTRING(task FROM '\D+') = block1
GROUP BY peer, SUBSTRING(task FROM '\D+')
), block_2 AS (
    SELECT  peer, SUBSTRING(task FROM '\D+') AS block_task
    FROM checks
    WHERE SUBSTRING(task FROM '\D+') =block2
    GROUP BY peer, SUBSTRING(task FROM '\D+')
),both_blocks AS (
    SELECT peer
    FROM block_1
    INTERSECT
    SELECT peer
    FROM block_2
), neither_block AS(
    SELECT nickname
    FROM peers
    EXCEPT
    (SELECT peer
    FROM block_1
    UNION DISTINCT
    SELECT peer
    FROM block_2)
)
SELECT ROUND(((SELECT COUNT(*) FROM block_1)::numeric/(SELECT COUNT(*) FROM peers) * 100),0 )AS StartedBlock1,
ROUND(((SELECT COUNT(*) FROM block_2)::numeric/(SELECT COUNT(*) FROM peers) * 100),0) AS StartedBlock2,
ROUND(((SELECT COUNT(*) FROM both_blocks)::numeric/(SELECT COUNT(*) FROM peers) * 100),0) AS StartedBothBlocks,
ROUND(((SELECT COUNT(*) FROM neither_block)::numeric/(SELECT COUNT(*) FROM peers) * 100),0) AS DidntStartAnyBlock;
END;
$$ LANGUAGE PLPGSQL;



CALL part3_task9('res', 'C', 'DO');
FETCH ALL "res";


------------------10-------------------

CREATE OR REPLACE FUNCTION part3_task10()
RETURNS TABLE (SuccessfulChecks NUMERIC, UnsuccessfulChecks NUMERIC)
AS
$$
BEGIN
RETURN QUERY
WITH check_in_birthday AS (
SELECT id,peer , "date"
FROM checks
JOIN peers ON checks.peer = peers.nickname 
AND EXTRACT(DAY FROM checks."date") = EXTRACT(DAY FROM peers.birthday ) 
AND EXTRACT(MONTH FROM checks."date") = EXTRACT(MONTH FROM peers.birthday )
), success_check AS (
    SELECT peer
    FROM  check_in_birthday 
    JOIN xp ON check_in_birthday.id = xp."check"
),failure_check AS (
      SELECT peer
    FROM  check_in_birthday 
    EXCEPT
    SELECT peer
    FROM  success_check
)
SELECT ROUND((SELECT COUNT(*) FROM success_check)::NUMERIC /(SELECT COUNT(*) FROM peers) * 100,0) AS SuccessfulChecks,
ROUND((SELECT COUNT(*) FROM failure_check)::NUMERIC /(SELECT COUNT(*) FROM peers) * 100,0) AS UnsuccessfulChecks;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM part3_task10()

------------------11-------------------

CREATE OR REPLACE FUNCTION part3_task11(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR)
    RETURNS SETOF VARCHAR
AS
$$
BEGIN
    RETURN QUERY
        WITH success AS (SELECT peer, count(peer)
                         FROM (SELECT peer, task
                               FROM ((SELECT *
                                      FROM checks
                                               JOIN xp ON checks.id = xp."check"
                                      WHERE task = task1)
                                     UNION
                                     (SELECT *
                                      FROM checks
                                               JOIN xp ON checks.id = xp."check"
                                      WHERE task = task2)) t1
                               GROUP BY peer, task) t2
                         GROUP BY peer
                         HAVING count(peer) = 2)

            (SELECT peer
             FROM success)
        EXCEPT
        (SELECT success.peer
         FROM success
                  JOIN checks ON checks.peer = success.peer
                  JOIN XP ON checks.id = XP."check"
         WHERE task = task3);
END;
$$ LANGUAGE plpgsql;


SELECT * FROM part3_task11('C2_SimpleBashUtils', 'C3_s21_string+', 'CPP1_s21_matrix+');



------------------12-------------------

CREATE OR REPLACE FUNCTION part3_task12() 
RETURNS TABLE("Task" VARCHAR, "PrevCount" INT) AS $$
BEGIN
	RETURN QUERY
	WITH RECURSIVE TaskCTE AS (
		SELECT Title, 0 AS PrevCount
		FROM Tasks
		WHERE ParentTask IS NULL

		UNION ALL

		SELECT t.Title, TaskCTE.PrevCount + 1
		FROM Tasks t
		INNER JOIN TaskCTE ON t.ParentTask = TaskCTE.Title
	)
	SELECT Title AS Task, PrevCount
	FROM TaskCTE;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM  part3_task12();


------------------13-------------------


CREATE OR REPLACE FUNCTION part3_task13(N int)
RETURNS TABLE ("Day" date) AS $$
BEGIN
	RETURN QUERY
		WITH  total_checks AS (
			SELECT c.id, c.date, p2p.time, p2p.state, xp.xp_amount
			FROM checks c, p2p, xp
			WHERE c.id = p2p.check AND (p2p.state = 'Success' OR p2p.state = 'Failure')
				AND c.id = xp.check AND xp_amount >= (SELECT tasks.max_xp
														 FROM tasks
														 WHERE tasks.title = c.task) * 0.8
			ORDER BY c.date, p2p.time),
		 succes_in_a_row AS (
			SELECT id, date, time, state,
			(CASE WHEN state = 'Success' THEN row_number() over (partition by state, date ORDER BY "time") ELSE 0 END) AS amount
												 FROM total_checks ORDER BY date
		 ),
		 max_in_day AS (SELECT s.date, MAX(amount) amount FROM succes_in_a_row s GROUP BY date)

		 SELECT date AS day FROM max_in_day WHERE amount >= N;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM part3_task13(3);

------------------14-------------------

CREATE OR REPLACE FUNCTION part3_task14 ()
RETURNS TABLE ("peer" VARCHAR, "XP" NUMERIC)
AS $$
BEGIN
	RETURN QUERY
    	SELECT ch.peer AS peer, SUM(xp.xp_amount)::NUMERIC AS XP
    	FROM checks AS ch
    	    INNER JOIN xp
    	        ON (xp."check" = ch.ID)
    	GROUP BY ch.peer
    	ORDER BY 2 DESC
    	LIMIT 1;
END;
$$ LANGUAGE PLPGSQL;

SELECT * FROM part3_task14();


------------------15-------------------

CREATE OR REPLACE FUNCTION part3_task15 (find_time TIME, N INT)
RETURNS TABLE (Peer VARCHAR)
AS $$
BEGIN
	RETURN QUERY
		SELECT tt.Peer 
		FROM time_tracking AS tt
		WHERE tt."time" < find_time AND tt.state = '1'
		GROUP BY tt.Peer
		HAVING COUNT(*) >= N;
END;
$$ LANGUAGE PLPGSQL;

SELECT * FROM part3_task15(TIME '10:08:52', 2);



------------------16-------------------
CREATE OR REPLACE FUNCTION part3_task16 (N INT, M INT)
RETURNS TABLE (peer VARCHAR)
AS $$
BEGIN
	RETURN QUERY
		WITH FindPeer AS (
		SELECT *
		FROM (SELECT tt.peer AS peer, "date", COUNT(state) AS out_count
				FROM time_tracking tt
			  WHERE state = '2'
				GROUP BY tt.peer, "date") AS out
		WHERE (current_date - "date") < N AND out_count >= M
		)
		SELECT FindPeer.peer AS Peer FROM FindPeer
		ORDER BY FindPeer.peer;
END;
$$ LANGUAGE PLPGSQL;

SELECT * FROM part3_task16(190, 1);



------------------17-------------------

CREATE OR REPLACE FUNCTION part3_task17 ()
RETURNS TABLE ("Month" VARCHAR, EarlyEntries NUMERIC)
AS $$
BEGIN
	RETURN QUERY
		WITH dayEntry AS (
		 SELECT peer, "date", MIN("time") AS "time"
		 FROM time_tracking 
		 GROUP BY peer , "date"
		), totalEntry AS (
		SELECT TO_CHAR(dayEntry."date", 'TMMONTH') AS "Month", COUNT(*) AS te
		FROM dayEntry
		JOIN peers ON dayEntry.peer = peers.nickname
		WHERE EXTRACT(MONTH FROM peers.birthday) = EXTRACT(MONTH FROM dayEntry."date")
		GROUP BY "Month"
		), earlyEntry AS (
		SELECT TO_CHAR(dayEntry."date", 'TMMONTH') AS "Month", COUNT(*) AS ee
		FROM dayEntry
		JOIN peers ON dayEntry.peer = peers.nickname
		WHERE EXTRACT(MONTH FROM peers.birthday) = EXTRACT(MONTH FROM dayEntry."date") AND EXTRACT(HOUR FROM dayEntry."time") < 12
		GROUP BY "Month"
		)
		SELECT totalEntry."Month"::VARCHAR, ROUND(COALESCE(earlyEntry.ee,0)/totalEntry.te::NUMERIC*100,0)::NUMERIC
		FROM totalEntry
		LEFT JOIN earlyEntry ON earlyEntry."Month" = totalEntry."Month";
END;
$$ LANGUAGE PLPGSQL;

SELECT * FROM part3_task17();









