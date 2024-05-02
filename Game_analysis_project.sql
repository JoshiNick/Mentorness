Game_analysis_Project

#Data imported # Using database DS

USE Ds

#player_ details table name is PD
SELECT * FROM pd
#level_details2 table name is LD
SELECT * FROM ld

alter table pd modify L1_Status varchar(30);
alter table pd modify L2_Status varchar(30);
alter table pd modify P_ID int primary key;
alter table pd drop myunknowncolumn;

alter table ld drop myunknowncolumn;
alter table ld change timestamp start_datetime datetime;
alter table ld modify Dev_Id varchar(10);
alter table ld modify Difficulty varchar(15);
alter table ld add primary key(P_ID,Dev_id,start_datetime);

-- pd (P_ID,PName,L1_status,L2_Status,L1_code,L2_Code)
-- ld (P_ID,Dev_ID,start_time,stages_crossed,level,difficulty,kill_count,
-- headshots_count,score,lives_earned)

-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players 
-- at level 0

SELECT pd.P_ID, ld.Dev_ID, pd.PName, Ld.Difficulty
FROM pd
JOIN Ld ON pd.P_ID = Ld.P_ID
WHERE Ld.level = 0;

-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast 3 stages are crossed
SELECT * FROM pd
SELECT * FROM ld

SELECT Pd.l1_code AS Level1_code, AVG(Ld.kill_count) AS Avg_Kill_Count
FROM Pd
JOIN Ld ON Pd.P_Id = Ld.p_id
WHERE Ld.lives_earned = 2 AND Ld.stages_crossed >= 3
GROUP BY Pd.l1_code;

-- Q3) Find the total number of stages crossed at each diffuculty level
-- where for Level2 with players use zm_series devices. Arrange the result in decsreasing order of total number of stages crossed



SELECT Ld.difficulty AS Difficulty_Level, SUM(Ld.stages_crossed) AS Total_Stages_Crossed
FROM Pd
JOIN Ld ON Pd.P_Id = Ld.p_id
WHERE ld.`level` = 2 
  AND ld.Dev_Id like 'zm%'
GROUP BY Ld.difficulty
ORDER BY Total_Stages_Crossed DESC;


-- Q4) Extract P_ID and the total number of unique dates for those players who have played games on multiple days


SELECT p_id, COUNT(DISTINCT CAST(start_datetime AS DATE)) AS Total_Unique_Dates
FROM Ld
GROUP BY p_id
HAVING COUNT(DISTINCT CAST(start_datetime AS DATE)) > 1;

-- Q5) Find P_ID and level wise sum of kill_counts where kill_count is greater than avg kill count for the Medium difficulty.
SELECT * FROM pd
SELECT * FROM ld

SELECT ld.p_id,ld.`level`,SUM(ld.kill_count) AS total_kill_count FROM ld
INNER JOIN
(SELECT AVG(kill_count) AS avg_kill_count FROM ld WHERE difficulty = 'Medium') AS AVG_Tab ON 
ld.kill_count> avg_tab.avg_kill_count
GROUP BY ld.p_id,ld.Level


-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.

SELECT level, l1_code, SUM(lives_earned) AS Total_Lives_Earned
FROM Pd
JOIN Ld ON Pd.P_Id = Ld.p_id
WHERE LEVEL <> 0
GROUP BY level, l1_code
ORDER BY level ASC;

-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order using Row_Number. Display difficulty as well. 

SELECT p_id, Dev_id, score, difficulty,
       ROW_NUMBER() OVER (PARTITION BY Dev_id ORDER BY score ASC) AS Score_Rank
FROM Ld
WHERE (Dev_id, score) IN (
    SELECT Dev_id, score
    FROM (
        SELECT Dev_id, score,
               ROW_NUMBER() OVER (PARTITION BY Dev_id ORDER BY score DESC) AS Score_Rank
        FROM Ld
    ) AS ranked_scores
    WHERE Score_Rank <= 3
);

-- Q8) Find first_login datetime for each device id

SELECT Dev_id, MIN(start_datetime) AS first_login
FROM Ld
GROUP BY Dev_id;

-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.

SELECT Dev_id, score, difficulty,RANK() OVER (PARTITION BY difficulty ORDER BY score ASC) AS Score_Rank
FROM Ld WHERE (difficulty, score) IN (SELECT difficulty, score FROM 
(SELECT difficulty, score, RANK() OVER (PARTITION BY difficulty ORDER BY score DESC) AS Score_Rank
FROM Ld) AS ranked_scores WHERE Score_Rank <= 5);

-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(p_id). Output should contain player id, device id and first login datetime.

SELECT p_id, Dev_id, MIN(start_datetime) AS first_login_datetime
FROM Ld
GROUP BY p_id;


-- Q11) For each player and date, how many kill_count played so far by the player. That is, the total number of games played -- by the player until that date.
-- a) window function
-- b) without window function

SELECT * FROM pd
SELECT * FROM LD

A) SELECT p_id, start_datetime, SUM(kill_count) OVER (PARTITION BY p_id ORDER BY start_datetime) AS total_kill_count
FROM Ld;

B) SELECT ld.p_id, ld.start_datetime, 
 SUM(ld2.kill_count) AS total_kill_count
FROM Ld JOIN (SELECT p_id, start_datetime, kill_count FROM Ld
) ld2 ON ld.p_id = ld2.p_id AND ld.start_datetime >= ld2.start_datetime
GROUP BY ld.p_id, ld.start_datetime;


-- Q12) Find the cumulative sum of stages crossed over a start_datetime 

SELECT ld.start_datetime, ld.p_id, ld.stages_crossed,SUM(ld.stages_crossed) OVER (ORDER BY ld.start_datetime) AS cumulative_stages_crossed
FROM Ld 

-- Q13. Extract the top 3 highest sums of scores for each `Dev_ID` and the corresponding `P_ID`.


WITH RankedScores AS (SELECT p_id, Dev_id, SUM(score) AS total_score,
RANK() OVER (PARTITION BY Dev_id ORDER BY SUM(score) DESC) AS Score_Rank
FROM Ld GROUP BY p_id, Dev_id)
SELECT p_id, Dev_id, total_score
FROM RankedScores
WHERE Score_Rank <= 3;

-- 14. Find players who scored more than 50% of the average score, scored by the sum of scores for each `P_ID`.

SELECT ld.p_id, SUM(ld.score) AS total_score
FROM Ld 
GROUP BY ld.p_id
HAVING total_score > (
SELECT AVG(sum_score) * 0.5
FROM (SELECT SUM(score) AS sum_score
FROM Ld GROUP BY p_id) AS avg_scores);


-- Q15) Create a function to return sum of Score for a given player_id.

DELIMITER $$

CREATE FUNCTION GetTotalScore(p_id INT) RETURNS INT
BEGIN
   DECLARE total_score INT;
    
   SELECT SUM(score) INTO total_score
   FROM Ld
   WHERE p_id = GetTotalScore.p_id;
    
   RETURN total_score;
END$$

DELIMITER ;

# Call the function with specific player Id (p_id) to see the output:
SELECT GetTotalScore(211) AS TotalScoreForPlayer;
