Set up and Running Python Spark Application
---------------------------

    git clone https://github.com/automattic-hiring/pre-data-code-test-ajiks143.git
    cd <repo>
    
    # Setting up python path for the project
    export PYTHONPATH=$PWD

    #Run the Python spark application
    $SPARK_HOME/bin/spark-submit jobs/postanalysis.py <input json path> <Output path for writing files>

Question 1: Posts Per Blog
--------------------------
Function named **posts_per_blog** executes the logic for finding the metrics from the input blogs json dataset
**Output:**
|  minimum_posts| mean_posts |maximum_posts |total_blogs|
|--|--|--|--|
| 1 | 157.13036303630363 | 42762 |11514

**Bonus:**
 - Top 3 preferred language for posts
	 *English followed by Spanish and Indonesian*
	 
| lang | count |
|--|--|
| en | 1663330 |
|es |20058|
|id|14404|

 - Author with maximum posts and the post languages
 *Eli Nathanael has 42760 posts spanned across languages English, Norwegian, Spanish, Korean, Indonesian, Russian, Polish, Farsi*
 
| author_id | author |lang|post_counts|
|--|--|--|--|
| 70065670|Eli Nathanael| en| 42742|
| 70065670|Eli Nathanael| no| 7|
| 70065670|Eli Nathanael| es| 4|
| 70065670|Eli Nathanael| ko| 3|
| 70065670|Eli Nathanael| ru| 1|
| 70065670|Eli Nathanael| id| 1|
| 70065670|Eli Nathanael| fa| 1|
| 70065670|Eli Nathanael| pl| 1|

 - Author and post with maximum likes
 
|post_id|author_id|author |lang|title |like_count|
|--|--|--|--|--|--|
|41923 |49069439 |Mitch Zeissler|en |My Best Images From 2014|1063 |




Question 2: Who Likes Posts?
----------------------------
Function named **post_likes** executes the logic for finding the percentage of likes coming from people who were NOT authors in this dataset
|non_author_likers_percentage  |  
|--|
| 2.37 | 


Challenge 3: Writing a Table
----------------------------
Function named **table_write** executes the logic for writing out some files to be loaded into HDFS in an optimized manner 
**Consideration for defining file format and data distribution**

 - Since its anticipated that **READ** load is going to be heavy than **WRITE**, the ideal data format would be **PARQUET** with **Snappy** compression, it comes with following benefits
	 - Reduces IO operations
	 - Since its an Columnar storage format, we access specific columns without scanning the entire dataset
	 - Consumes less space
	 - Support type-specific encoding
 - Partitioning is another key aspect for performance so that we scan only the relevant folder as per our filter predicate.
	 - Analyzing the dataset, the ideal candidate for Partitioning is date (trimming the timestamp) and lang 
	 - Parquet Partition creates a folder hierarchy for each spark partition, in this case we will date folder and under each date folder we will have folders for languages pertaining to that date.
 - DDL to create hive table from the files created by Spark application
 

	    CREATE EXTERNAL TABLE datasets (
	    blog_id BIGINT,
	    post_id BIGINT,
	    url STRING,
	    date_gmt TIMESTAMP,
	    title STRING,
	    content STRING,
	    author STRING,
	    author_login STRING,
	    author_id BIGINT,
	    liker_ids ARRAY <BIGINT>,
	    like_count INT,
	    commenter_ids ARRAY <BIGINT>,
	    comment_count INT
	    ) 
	    PARTITIONED BY (date_ymd DATE, lang STRING) 
	    STORED AS PARQUET 
	    LOCATION  '<output path>';


 
