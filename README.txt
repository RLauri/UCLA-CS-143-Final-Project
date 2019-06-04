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



