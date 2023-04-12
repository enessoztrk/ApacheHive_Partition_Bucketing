Apache Hive Partition vs Bucketing

Task 1: Upload the u.data and u.item datasets from the Github repo as tables to Hive.

u.data
wget -P ~/datasets/ https://raw.githubusercontent.com/enessoztrk/u.data

head ~/datasets/u.data

u.item
wget -P ~/datasets/ https://raw.githubusercontent.com/enessoztrk/u.item

head ~/datasets/u.item


Step 2: Open Beeline and create a new database called movielens.

beeline -u jdbc:hive2://localhost:10000
CREATE DATABASE movielens;


Step 3: Examine the content of u.data and create a table called ratings that is appropriate for the data.

create table if not exists movielens.ratings (
user_id int,
item_id int,
rating int,
rating_time bigint)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile
tblproperties('skip.header.line.count'='1');


Step 4: Load the data from the local to the ratings table and examine the table.

load data local inpath '/home/train/datasets/u.data' into table movielens.ratings;

select * from movielens.ratings limit 4;
+------------------+------------------+-----------------+----------------------+
| ratings.user_id  | ratings.item_id  | ratings.rating  | ratings.rating_time  |
+------------------+------------------+-----------------+----------------------+
| 196              | 242              | 3               | 881250949            |
| 186              | 302              | 3               | 891717742            |
| 22               | 377              | 1               | 878887116            |
| 244              | 51               | 2               | 880606923            |
+------------------+------------------+-----------------+----------------------+

select count(1) from movielens.ratings;
+---------+
|   _c0   |
+---------+
|  100000 |
+---------+

select count(distinct user_id) from movielens.ratings;
+------+
|  _c0 |
+------+
|  943 |
+------+


Step 5 Create a table named movies suitable for u.item data.

create table if not exists movielens.movies (
movieid int,
movietitle string,
releasedate string,
videoreleasedate string,
IMDbURL string,
unknown tinyint,
Action tinyint,
Adventure tinyint,
Animation tinyint,
Childrens tinyint,
Comedy tinyint,
Crime tinyint,
Documentary tinyint,
Drama tinyint,
Fantasy tinyint,
FilmNoir tinyint,
Horror tinyint,
Musical tinyint,
Mystery tinyint,
Romance tinyint,
SciFi tinyint,
Thriller tinyint,
War tinyint,
Western tinyint)
row format delimited
fields terminated by '|'
lines terminated by '\n'
stored as textfile
tblproperties('skip.header.line.count'='1');


Step 6: Load the data from local to movies table and examine the table.

load data local inpath '/home/train/datasets/u.item' into table movielens.movies;

select movieid, movietitle, releasedate from movielens.movies limit 5;
+----------+------------------+--------------+
| movieid  | movietitle       | releasedate  |
+----------+------------------+--------------+
| 1        | ToyStory(1995)   | 01-Jan-1995  |
| 2        | GoldenEye(1995)  | 01-Jan-1995  |
| 3        | FourRooms(1995)  | 01-Jan-1995  |
| 4        | GetShorty(1995)  | 01-Jan-1995  |
| 5        | Copycat(1995)    | 01-Jan-1995  |
+----------+------------------+--------------+

select count(1) from movielens.movies;
+-------+
|  _c0  |
+-------+
|  1682 |
+-------+


Task 2: Business users want to make some queries and want to get the results of these queries as soon as possible. 
Make the necessary data organization on Hive to meet the needs of business users.

Step 1: We want to determine the most popular (most voted, highest average score) movies on a monthly basis. 
Design and create the table (partition and bucketing) accordingly.

Since business users query movies on a monthly basis, we can increase query performance 
if I make partitions by year and month and bucket movie names.

create table if not exists movielens.movie_ratings (
user_id int,
rating int,
rating_time bigint,
movieid int,
movietitle string,
videoreleasedate string,
imdburl string)
partitioned by (review_year int, review_month int)
clustered by (movietitle) into 4 buckets
stored as orc;


Step 2: Set Dynamic Partitioning.

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing=true;


Step 3: Load the data from the two tables into the table you designed.

insert overwrite table movielens.movie_ratings PARTITION(review_year, review_month)
select user_id,
rating,
rating_time,
movieid,
movietitle,
videoreleasedate,
imdburl,
YEAR(from_unixtime(rating_time, 'yyyy-MM-dd')) as review_year,
MONTH(from_unixtime(rating_time, 'yyyy-MM-dd')) as review_month
from movielens.ratings r join movielens.movies m on r.item_id = m.movieid;


Step 4: Check the table you created.

What is the number of observations?

select count(1) from movielens.movie_ratings;
+---------+
|  _c0    |
+---------+
| 100000  |
+---------+


List partitions.

show partitions movielens.movie_ratings;
+-----------------------------------+
|           partition               |
+-----------------------------------+
| review_year=1997/review_month=10  |
| review_year=1997/review_month=11  |
| review_year=1997/review_month=12  |
| review_year=1997/review_month=9   |
| review_year=1998/review_month=1   |
| review_year=1998/review_month=2   |
| review_year=1998/review_month=3   |
| review_year=1998/review_month=4   |
+-----------------------------------+


List table properties

describe movielens.movie_ratings;
+--------------------------+------------+----------+
| col_name                 | data_type  | comment  |
+--------------------------+------------+----------+
| user_id                  | int        |          |
| rating                   | int        |          |
| rating_time              | bigint     |          |
| movieid                  | int        |          |
| movietitle               | string     |          | 
| videoreleasedate         | string     |          |
| imdburl                  | string     |          |
| review_year              | int        |          |
| review_month             | int        |          |
|                          | NULL       | NULL     |
| # Partition Information  | NULL       | NULL     |
| # col_name               | data_type  | comment  |
| review_year              | int        |          |
| review_month             | int        |          |
+--------------------------+------------+----------+


How many unique values are there for Review Year and

select distinct (review_year, review_month) from movielens.movie_ratings;
+--------------------------+
|         _c0              |
+--------------------------+
| {"col1":1997,"col2":9}   |
| {"col1":1997,"col2":10}  |
| {"col1":1997,"col2":11}  |
| {"col1":1997,"col2":12}  |
| {"col1":1998,"col2":1}   |
| {"col1":1998,"col2":2}   |
| {"col1":1998,"col2":3}   |
| {"col1":1998,"col2":4}   |
+--------------------------+


Task 3: Create and interpret the necessary queries for the requested analyses.

Step 1: Find the top 20 rated movies in April 1998.

select count(*) total_count, movietitle
from movielens.movie_ratings
where review_year=1998 AND review_month=4
group by movietitle order by total_count desc limit 20;
+--------------+------------------------------+
|  total_count |           movietitle         |
+--------------+------------------------------+
| 63           | Titanic(1997)                |
| 52           | AirForceOne(1997)            |
| 50           | Contact(1997)                |
| 49           | FullMonty,The(1997)          |
| 49           | StarWars(1977)               |
| 42           | GoodWillHunting(1997)        |
| 41           | LiarLiar(1997)               |
| 41           | EnglishPatient,The(1996)     |
| 39           | AsGoodAsItGets(1997)         |
| 39           | ConspiracyTheory(1997)       |
| 37           | Scream(1996)                 |
| 36           | ToyStory(1995)               |
| 36           | Fargo(1996)                  |
| 36           | ReturnoftheJedi(1983)        |
| 35           | L.A.Confidential(1997)       |
| 34           | ChasingAmy(1997)             |
| 34           | Godfather,The(1972)          |
| 33           | Braveheart(1995)             |
| 33           | StarshipTroopers(1997)       |
| 33           | SilenceoftheLambs,The(1991)  |
+--------------+------------------------------+


Step 2: Find the 20 movies with the highest average score from the movies voted in April 1998.

select avg(rating) as avg_rating, count(*) total_count, movietitle
from movielens.movie_ratings
where review_year=1998 AND review_month=4
group by movietitle order by avg_rating desc limit 20;
+-------------+--------------+-------------------------------------------+
| avg_rating  |  total_count |               movietitle                  |
+-------------+--------------+-------------------------------------------+
| 5.0         | 3            | CelluloidCloset,The(1995)                 |
| 5.0         | 1            | Boys,Les(1997)                            |
| 5.0         | 1            | Flirt(1995)                               |
| 5.0         | 1            | FreeWilly2:TheAdventureHome(1995)         |
| 5.0         | 1            | DeltaofVenus(1994)                        |
| 5.0         | 1            | CutthroatIsland(1995)                     |
| 5.0         | 1            | DunstonChecksIn(1996)                     |
| 5.0         | 2            | Diexueshuangxiong(Killer,The)(1989)       |
| 5.0         | 1            | Lassie(1994)                              |
| 5.0         | 1            | Innocents,The(1961)                       |
| 5.0         | 1            | Stalingrad(1993)                          |
| 5.0         | 1            | FearofaBlackHat(1993)                     |
| 5.0         | 1            | Trust(1990)                               |
| 5.0         | 1            | BoxingHelena(1993)                        |
| 5.0         | 1            | DavyCrockett,KingoftheWildFrontier(1955)  |
| 5.0         | 1            | BitterSugar(AzucarAmargo)(1996)           |
| 5.0         | 1            | BlueSky(1994)                             |
| 5.0         | 1            | Daylight(1996)                            |
| 5.0         | 2            | Prefontaine(1997)                         |
| 5.0         | 1            | 8Seconds(1994)                            |
+-------------+--------------+-------------------------------------------+
