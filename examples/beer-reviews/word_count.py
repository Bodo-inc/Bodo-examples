"""
Word count benchmark.
"""

import os
import time
import itertools
import argparse
import pandas as pd
import bodo


@bodo.jit
def bodo_read_csv(fname):
    """
    Paralellized version of pandas read_csv by only adding @bodo.jit decorator
    :param fname: file name in csv format
    :return: distributed pandas dataframe
    """

    print("Reading input data started ...")
    t1 = time.time()
    cols = {'beer_id': 'int',
            'username': 'str',
            'date': 'str',
            'text': 'str',
            'look': 'float',
            'smell': 'float',
            'taste': 'float',
            'feel': 'float',
            'overall': 'float',
            'score': 'float'}
    df = pd.read_csv(fname, dtype=cols, names=cols.keys())
    print("Finished IO", time.time() - t1)
    return df


@bodo.jit
def lexi_main(case_notes):
    """
    Simple word count module which returns a sorted count of words in "text" column of a dataframe
    :param case_notes: Dataframe containing a column named "text" which includes text data
    :return: a pandas data series with containing how many times each word is repeated in the "text" column of dataframe
    """

    t0 = time.time()
    print("Code execution timer started...")
    case_notes["text"] = case_notes["text"].str.lower()
    text_split_series = case_notes["text"].str.split()
    str_series = text_split_series.explode()
    str_series = str_series[str_series.str.len() < 100]
    word_count = str_series.value_counts()

    print("Finished counts after", time.time() - t0)
    return bodo.gatherv(word_count)


def main():
    """
    Main module. Reads in the input file from terminal arguments, passes it to bodo_read_csv() and carries out the count
    :return: word count data series
    """

    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    parser = argparse.ArgumentParser(description='word count')
    parser.add_argument('--file', dest='file', type=str, default="reviews.csv")
    args = parser.parse_args()

    datafile = args.file
    notes = bodo_read_csv(datafile)
    # notes = bodo_read_csv('../beers-breweries-and-beer-reviews/breweries.csv')
    # notes = bodo_read_csv('s3://bodo-poc/data/reviews50GB.csv')
    w_c = lexi_main(notes)
    
    if bodo.get_rank() == 0:
        print(w_c)
    
    return w_c


if __name__ == '__main__':
    main()
