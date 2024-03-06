-- Active: 1695727197563@@127.0.0.1@5432@pallaaxe
---------------------------------------------------
-- TASK 1
---------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_p2p_review(IN nick_checkedpeer varchar, IN nick_checkingpeer varchar, IN task_name varchar, IN status_review check_status, IN check_time time) AS $$
    BEGIN
        IF status_review = 'Start' THEN
            IF ((SELECT COUNT(*) FROM p2p
                 JOIN checks ON p2p."check" = checks.id
                 WHERE p2p.checkingpeer = nick_checkingpeer AND checks.peer = nick_checkedpeer AND checks.task = task_name) % 2 = 1) THEN
                 RAISE EXCEPTION 'Unfinished p2p stage';
            ELSE
                INSERT INTO checks VALUES (COALESCE((SELECT MAX(id) FROM checks),0 )+ 1, nick_checkedpeer, task_name, NOW());
                INSERT INTO p2p VALUES (COALESCE((SELECT MAX(id) FROM p2p),0) + 1, (SELECT MAX(id) FROM checks), nick_checkingpeer, status_review, check_time);
            END IF;
        ELSE 
			IF ((SELECT COUNT(*) FROM p2p
                 JOIN checks ON p2p."check" = checks.id
                 WHERE p2p.checkingpeer = nick_checkingpeer AND checks.peer = nick_checkedpeer AND checks.task = task_name) % 2 = 1) THEN
            INSERT INTO p2p VALUES (COALESCE((SELECT MAX(id) FROM p2p),0 )+ 1, 
                                    (SELECT "check" FROM p2p
                                    JOIN checks ON p2p."check" = checks.id
                                    WHERE p2p.checkingpeer = nick_checkingpeer AND checks.peer = nick_checkedpeer AND checks.task = task_name
									ORDER BY time DESC
									LIMIT 1),
                                    nick_checkingpeer, status_review, check_time);
			ELSE 
				RAISE EXCEPTION 'Undefined p2p stage';
        	END IF;        
		END IF;
    END;
$$ LANGUAGE plpgsql;

CALL proc_p2p_review('Pizza', 'Pelmen', 'C2_SimpleBashUtils', 'Start', localtime(0));
CALL proc_p2p_review('Pizza', 'Pelmen', 'C2_SimpleBashUtils', 'Success', localtime(0));  
CALL proc_p2p_review('Pizza', 'Pelmen', 'C2_SimpleBashUtils', 'Failure', localtime(0));

-------------------->>> END <<<--------------------

---------------------------------------------------
-- TASK 2
---------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_add_verter(IN nick_checkedpeer varchar, IN task_name varchar, IN status_verter check_status, IN check_time time) AS $$
	BEGIN
		IF ((SELECT COUNT(*) FROM p2p
			JOIN checks ON checks.task = task_name AND checks.id = p2p."check"
			WHERE checks.peer = nick_checkedpeer) % 2 = 0)
			THEN
			IF EXISTS (SELECT * FROM p2p
			JOIN checks ON checks.id = p2p.check AND checks.task = task_name WHERE checks.peer = nick_checkedpeer 
				ORDER BY time DESC
				LIMIT 1) THEN
				INSERT INTO verter VALUES (
					(SELECT MAX(id) + 1 FROM verter), (SELECT "check" FROM p2p
			JOIN checks ON checks.task = task_name AND checks.id = p2p."check"
			WHERE checks.peer = nick_checkedpeer AND p2p.state = 'Success' ORDER BY time DESC LIMIT 1), status_verter, check_time
				);
			ELSE 
			RAISE EXCEPTION 'p2p check - failure';
			END IF;
		ELSE 
		RAISE EXCEPTION 'Unfinished P2P check';
		END IF;
    END;
$$ LANGUAGE plpgsql;

CALL proc_add_verter('Pizza', 'C2_SimpleBashUtils', 'Success', localtime(0)); 

-------------------->>> END <<<--------------------

---------------------------------------------------
-- TASK 3
---------------------------------------------------

CREATE OR REPLACE FUNCTION fnc_trg_p2p_transfer_point()
RETURNS TRIGGER AS $$
BEGIN
	IF (NEW.state = 'Start')
	THEN
		IF EXISTS (SELECT * FROM transferred_points WHERE transferred_points.checkingpeer = NEW.checkingpeer AND transferred_points.checkedpeer = 
		(SELECT checks.peer FROM checks WHERE checks.id = NEW."check" ))
		THEN 
		UPDATE transferred_points
		SET points_amount = points_amount + 1
		WHERE checkingpeer = NEW.checkingpeer
        AND checkedpeer = (
            SELECT peer
            FROM checks
            WHERE NEW."check" = checks.id
        );
		ELSE 
		INSERT INTO transferred_points VALUES (
		(SELECT MAX(id) + 1 FROM transferred_points), NEW.checkingpeer, (SELECT DISTINCT peer FROM checks WHERE checks.id = NEW."check"), 1
		);
	END IF;
	END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE  OR REPLACE TRIGGER trg_p2p_transfer_point 
AFTER INSERT ON p2p
FOR EACH ROW EXECUTE FUNCTION fnc_trg_p2p_transfer_point();


INSERT INTO p2p
VALUES(97,14,'Shi','Start','10:00:00');


-- DROP TRIGGER IF EXISTS trg_p2p_transfer_point ON p2p;

-- DROP FUNCTION fnc_trg_insert_p2p();

-------------------->>> END <<<--------------------

---------------------------------------------------
-- TASK 4
---------------------------------------------------
CREATE OR REPLACE FUNCTION fnc_trg_xp_validate()
RETURNS TRIGGER AS $$
BEGIN
	IF (SELECT max_xp FROM tasks JOIN checks ON checks.id = NEW.check AND checks.task = tasks.title) < NEW.xp_amount 
	THEN 
	RAISE EXCEPTION 'Uncorrect XP';
	ELSEIF (SELECT state FROM P2P WHERE NEW."check" = p2p."check" AND p2p.state = 'Failure') = 'Failure'
	THEN
  RAISE EXCEPTION 'Failure from peer';
	ELSEIF (SELECT state FROM verter WHERE NEW."check" = verter."check" AND verter.state = 'Failure') = 'Failure'
	THEN 
	RAISE EXCEPTION 'Verter check Failure';
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER trg_xp_validate
BEFORE INSERT ON xp
FOR EACH ROW EXECUTE FUNCTION fnc_trg_xp_validate();


INSERT INTO XP VALUES (44, 19, 100);





