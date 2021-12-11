df = LOAD '/lab/kredit/input/kredit_nohead.csv' USING PigStorage(',') AS (occupation:int, salary:int, installment:int, tenor:int, usia:int, ketr:chararray);

group_ketr = GROUP df BY ketr;

count_avg_per_ketr = foreach group_ketr GENERATE group, (chararray)COUNT(df.ketr) AS total, (chararray)AVG(df.salary) AS average;
DUMP count_avg_per_ketr;