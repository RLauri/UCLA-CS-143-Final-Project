Project 2A CS 143
=================

This project was about implementing sanitize in cleantext.py,
the way we solved most of the problems presented by the implementation
of sanitize() was with regular expressions, we are not sure if this works
100% perfectly, but on most test strings we tried it seem to handle them fine

For the n-grams, we ended up implementing a sliding window function where
you can specify the window size and then go through it iteratively, so for
bigrams we used a window size of 2 and for trigrams we used a window size of 3
