#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import sys
import json


__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}


# You may need to write regular expressions.
def sliding_window(sequence, window_size=10):
    """
    Returns a generator that will iterate through
    the defined chunks of input sequence.  Input sequence
    must be iterable.

    :param sequence: iterable sequence for the sliding window
    :param window_size: how large the window will be, ideally it
                        should be less than the sequence length
    """
    num_chunks = int(len(sequence)-window_size+1)
    for i in range(0, num_chunks):
        yield sequence[i:i+window_size]


def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """
    # 1. Replace new lines and tab characters with a single space.
    text = text.replace('\t', ' ').replace('\n', ' ')

    # 2. Remove URLs.
    text = re.sub(r"\[.*\]\(https?://\S+\b|www\.(\w +\.)+\S*\)", "", text)

    # 3. Remove all links to subreddits and reddit users
    text = re.sub(r"\/(r|u)\/[a-zA-Z0-9_-]+", "", text)

    # 4-5. Tokenize Text
    tokens = re.findall(r"[\w']+|[.,!?;:]", text)

    # 6. Remove special characters
    temp_tokens = list()
    for token in tokens:
        if token == '-':
            continue
        # Get rid of wierd characters
        token = re.sub(r"[^0-9a-zA-Z.,!?;:\-']+", "", token)
        token = re.sub(r"--", "", token)
        temp_tokens.append(token)
    tokens = temp_tokens

    # 7. Convert to lowercase
    tokens = list(map(lambda x: x.lower(), tokens))

    parsed_text = ' '.join(tokens)
    symbols = {"'", "-", "!", ".", "?", ":", ";", ","}
    grams = list(filter(lambda x: x not in symbols, tokens))
    unigrams = ' '.join(grams)

    bigrams = list()
    for window in sliding_window(grams, 2):
        bigrams.append('_'.join(window))
    bigrams = ' '.join(bigrams)

    trigrams = list()
    for window in sliding_window(grams, 3):
        trigrams.append('_'.join(window))
    trigrams = ' '.join(trigrams)
    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.

    # We are "requiring" your write a main function so you can
    # debug your code. It will not be graded.
    file_name = sys.argv[1]
    print(file_name)
    with open(file_name, 'r') as f:
        for line in f.readlines():
            d = json.loads(line)
            print(sanitize(d["body"]))
