Project 2A CS 143
=================

Question 1:

Functional dependencies:
input_id -> labeldem
input_id -> labelgop
input_id -> labeldjt

Question 2:

Take a look at the schema for comments.
Forget BCNF and 3NF.
Does the data frame look normalized?
In other words, is the data frame free of redundancies that might affect insert/update integrity?If not, how would we decompose it?
Why do you believe the collector of the data stored it in this way?

Due to my unfamiliarity with Reddit, I will say that some of this does not look very normalized,
for example: can_gild and can_mod_post I would believe both are related to how old the post is on
Reddit, since posts older than 6 months can't be upvoted or gilded. But then I am not sure if
those columns are even usefull if created_utc exists. My only theory for why they might have
done this is because you can store all these values precomputed instead of having to determine in
real time if a post is gilded or not which could save load time. The only other column that I
really don't understand is why subreddit exists if subreddit id already exists.
If I was to normalize it, I woudld make a table with subreddit, subreddit_type and subreddit_id
so that I can join on subreddit_id. Additionally I would split permalink and link_id into their
own separate table.

Question 3:

Pick one of the joins that you execute for this project. Rerun the join with .explain() attached 
to it. Include the output. What do you notice? Explain what Spark SQL is doing during the join. 
Which join algorithm does Spark seem to be using?

== Physical Plan ==                                                                                                                                                                                                                                                                                                         
*(10) Project [comment_id#276, timestamp#278L, state#279, title#106, if ((cast(pythonUDF0#407 as double) > 0.2)) 1 else 0 AS pos#377, if ((cast(pythonUDF1#408 as double) > 0.25)) 1 else 0 AS neg#378]                                                                                                                     
+- BatchEvalPython [first_element(probability#309), first_element(probability#345)], [comment_id#276, probability#309, probability#345, state#279, timestamp#278L, title#106, pythonUDF0#407, pythonUDF1#408]                                                                                                                  
    +- *(9) Project [comment_id#276, probability#309, probability#345, state#279, timestamp#278L, title#106]                                                                                                                                                                                                                      
        +- *(9) SortMergeJoin [comment_id#276], [comment_id#392], Inner                                                                                                                                                                                                                                                                
            :- *(4) Sort [comment_id#276 ASC NULLS FIRST], false, 0                                                                                                                                                                                                                                                                     
            :  +- Exchange hashpartitioning(comment_id#276, 200)                                                                                                                                                                                                                                                                        
            :     +- *(3) Project [id#14 AS comment_id#276, created_utc#10L AS timestamp#278L, author_flair_text#3 AS state#279, title#106, UDF(UDF(UDF(pythonUDF0#405))) AS probability#309]                                                                                                                                           
            :        +- BatchEvalPython [sanitize(body#4)], [author_flair_text#3, body#4, created_utc#10L, id#14, title#106, pythonUDF0#405]                                                                                                                                                                                            
            :           +- *(2) Project [author_flair_text#3, body#4, created_utc#10L, id#14, title#106]                                                                                                                                                                                                                                
            :              +- *(2) BroadcastHashJoin [ltrim(link_id#16, Some(t3_))], [id#69], Inner, BuildRight                                                                                                                                                                                                                         
            :                 :- *(2) Filter (((isnotnull(body#4) && NOT Contains(body#4, /s)) && NOT StartsWith(body#4, &gt)) && isnotnull(id#14))                                                                                                                                                                                                     
            :                 :  +- *(2) Sample 0.0, 0.2, false, -5607937167649919925                                                                                                                                                                                                                                                                    
            :                 :     +- *(2) FileScan parquet [author_flair_text#3,body#4,created_utc#10L,id#14,link_id#16] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/media/sf_vm-shared/CS-143-Project-2b/comments.parquet], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<author_flair_text:string,body:string,created_utc:bigint,id:string,link_id:string>                                                                                                                                                                                                                                                                  
            :                 +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]))                                                                                                                                                                                                                           
            :                    +- *(1) Filter isnotnull(id#69)                                                                                                                                                                                                                                                                        
            :                       +- *(1) Sample 0.0, 0.2, false, -6303060203833664997                                                                                                                                                                                                                                                
            :                          +- *(1) FileScan parquet [id#69,title#106] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/media/sf_vm-shared/CS-143-Project-2b/submissions.parquet], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:string,title:string>                                      
            +- *(8) Sort [comment_id#392 ASC NULLS FIRST], false, 0                                                                                                                                                                                                                                                                        
                +- Exchange hashpartitioning(comment_id#392, 200)                                                                                                                                                                                                                                                                              
                    +- *(7) Project [id#14 AS comment_id#392, UDF(UDF(UDF(pythonUDF0#406))) AS probability#345]                                                                                                                                                                                                                                    
                        +- BatchEvalPython [sanitize(body#4)], [body#4, id#14, pythonUDF0#406]                                                                                                                                                                                                                                                         
                            +- *(6) Project [body#4, id#14]                                                                                                                                                                                                                                                                                                
                                +- *(6) BroadcastHashJoin [ltrim(link_id#16, Some(t3_))], [id#69], Inner, BuildRight                                                                                                                                                                                                                                           
                                    :- *(6) Filter (((isnotnull(body#4) && NOT Contains(body#4, /s)) && NOT StartsWith(body#4, &gt)) && isnotnull(id#14))                                                                                                                                                                                                       
                                    :  +- *(6) Sample 0.0, 0.2, false, -5607937167649919925                                                                                                                                                                                                                                                                     
                                    :     +- *(6) FileScan parquet [body#4,id#14,link_id#16] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/media/sf_vm-shared/CS-143-Project-2b/comments.parquet], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<body:string,id:string,link_id:string>                                        
                                    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]))                                                                                                                                                                                                                                                
                                        +- *(5) Filter isnotnull(id#69)                                                                                                                                                                                                                                                                                                
                                            +- *(5) Sample 0.0, 0.2, false, -6303060203833664997                                                                                                                                                                                                                                                                           
                                                +- *(5) FileScan parquet [id#69] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/media/sf_vm-shared/CS-143-Project-2b/submissions.parquet], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:string>                                                    
---------------------------------------------------------------------------

threshold_sql = """
SELECT
    a.comment_id AS comment_id,
    a.timestamp AS timestamp,
    a.state AS state,
    a.title AS title,
    if (first_element(a.probability) > 0.2, 1, 0) AS pos,
    if (first_element(b.probability) > 0.25, 1, 0) AS neg
FROM pos_result a
INNER JOIN neg_result b ON a.comment_id = b.comment_id
"""