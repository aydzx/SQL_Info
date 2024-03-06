-- Active: 1695727197563@@127.0.0.1@5432@pallaaxe
CREATE TYPE check_status AS ENUM (
'Start',
'Success',
'Failure'
);

CREATE TYPE check_tracking AS ENUM (
'1',
'2'
);

CREATE TABLE tasks (
    title VARCHAR PRIMARY KEY,
    parenttask VARCHAR,
    max_xp INTEGER
);

CREATE TABLE peers (
    nickname VARCHAR PRIMARY KEY NOT NULL ,
    birthday DATE NOT NULL
);

CREATE TABLE time_tracking (
    id BIGINT PRIMARY KEY,
    peer VARCHAR,
    CONSTRAINT fk_time_tracking_peers FOREIGN KEY (peer) REFERENCES peers(nickname),
    "date" DATE,
    "time" TIME,
    "state" check_tracking
);

CREATE TABLE recommendations (
    id BIGINT PRIMARY KEY,
    peer VARCHAR,
    CONSTRAINT fk_recommendation_peers FOREIGN KEY (peer) REFERENCES peers(nickname),
    recommendedpeer VARCHAR
);

CREATE TABLE friends (
    id BIGINT PRIMARY KEY,
    peer1 VARCHAR,
    CONSTRAINT fk_friends_1_peers FOREIGN KEY (peer1) REFERENCES peers(nickname),
    peer2 VARCHAR,
    CONSTRAINT fk_friends_2_peers FOREIGN KEY (peer2) REFERENCES peers(nickname)
);

CREATE TABLE transferred_points (
    id BIGINT PRIMARY KEY,
    checkingpeer VARCHAR,
    CONSTRAINT fk_transferred_points_1_peers FOREIGN KEY (checkingpeer) REFERENCES peers(nickname),
    checkedpeer VARCHAR,
    CONSTRAINT fk_transferred_points_2_peers FOREIGN KEY (Checkedpeer) REFERENCES peers(nickname),
    points_amount BIGINT
);

CREATE TABLE p2p (
    id BIGINT PRIMARY KEY,
    "check" BIGINT NOT NULL,
    checkingpeer VARCHAR,
    CONSTRAINT fk_p2p_peers FOREIGN KEY (checkingpeer) REFERENCES peers(nickname),
    "state" check_status,
    "time" TIME
);

CREATE TABLE checks (
    id BIGINT PRIMARY KEY,
    peer VARCHAR,
    CONSTRAINT fk_checks_peer FOREIGN KEY (peer) REFERENCES peers(nickname),
    task VARCHAR,
    CONSTRAINT fk_checks_tasks FOREIGN KEY (task) REFERENCES tasks(title),
    "date" DATE
);

CREATE TABLE verter (
    id BIGINT PRIMARY KEY,
    "check" BIGINT,
    "state" check_status,
    "time" TIME
);

CREATE TABLE xp (
    id BIGINT PRIMARY KEY,
    "check" BIGINT,
    xp_amount INTEGER
);


CREATE OR REPLACE PROCEDURE import_from_csv(table_name  VARCHAR , path VARCHAR, delimiter_data VARCHAR) AS $$
BEGIN
EXECUTE FORMAT ('COPY %s FROM ''%s'' DELIMITER ''%s'' CSV HEADER;', table_name , path,delimiter_data);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE export_to_csv(table_name  VARCHAR , path VARCHAR, delimiter_data VARCHAR) AS $$
BEGIN
EXECUTE FORMAT ('COPY %s TO''%s'' DELIMITER ''%s'' CSV HEADER;', table_name , path,delimiter_data);
END
$$ LANGUAGE plpgsql;

DROP PROCEDURE import_from_csv(table_name  VARCHAR , path VARCHAR, delimiter_data VARCHAR);
DROP PROCEDURE export_to_csv(table_name  VARCHAR , path VARCHAR, delimiter_data VARCHAR);



CALL export_to_csv('peers', '/Users/pallaaxe/Desktop/peers.csv', ',');
CALL export_to_csv('tasks', '/Users/pallaaxe/Desktop/tasks.csv', ',');
CALL export_to_csv('Checks', '/Users/pallaaxe/Desktop/checks.csv', ',');
CALL export_to_csv('P2P', '/Users/pallaaxe/Desktop/p2p.csv', ',');
CALL export_to_csv('verter', '/Users/pallaaxe/Desktop/verter.csv', ',');
CALL export_to_csv('transferred_points', '/Users/pallaaxe/Desktop/transferred_points.csv', ',');
CALL export_to_csv('friends', '/Users/pallaaxe/Desktop/friends.csv', ',');
CALL export_to_csv('recommendations', '/Users/pallaaxe/Desktop/recommendations.csv', ',');
CALL export_to_csv('xp', '/Users/pallaaxe/Desktop/xp.csv', ',');
CALL export_to_csv('time_tracking', '/Users/pallaaxe/Desktop/time_tracking.csv', ',');



CALL import_from_csv('peers', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/peers.csv', ',');
CALL import_from_csv('tasks', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/tasks.csv', ',');
CALL import_from_csv('checks', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/checks.csv', ',');
CALL import_from_csv('p2p', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/p2p.csv', ',');
CALL import_from_csv('verter', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/verter.csv', ',');
CALL import_from_csv('xp', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/xp.csv', ',');
CALL import_from_csv('friends', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/friends.csv', ',');
CALL import_from_csv('transferred_points', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/transferred_points.csv', ',');
CALL import_from_csv('time_tracking', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/time_tracking.csv', ',');
CALL import_from_csv('recommendations', '/Users/pallaaxe/Desktop/project/SQL2_info21_v1.0-1/src/recommendations.csv', ',');

