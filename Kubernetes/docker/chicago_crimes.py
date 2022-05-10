import numpy as np
import pandas as pd
import time
import bodo
import json
import os

@bodo.jit(cache=True)
def load_chicago_crimes():
    t1 = time.time()
    crimes = pd.read_parquet('s3://bodo-example-data/chicago-crimes/Chicago_Crimes_2012_to_2017.pq')
    crimes = crimes.sort_values(by="ID")    
    print("Reading time: ", ((time.time() - t1) * 1000), " (ms)")    
    return crimes

@bodo.jit(distributed=["crimes"], cache=True)
def data_cleanup(crimes):
    t1 = time.time()    
    crimes = crimes.drop_duplicates()    
    crimes.drop(['Unnamed: 0', 'Case Number', 'IUCR','Updated On','Year', 'FBI Code', 'Beat','Ward','Community Area', 'Location'], inplace=True, axis=1)
    crimes.Date = pd.to_datetime(crimes.Date, format='%m/%d/%Y %I:%M:%S %p')
    crimes["dow"] = crimes["Date"].dt.dayofweek
    crimes["date only"] = crimes["Date"].dt.floor('D')
    crimes = crimes.sort_values(by="ID")    
    print("Data cleanup time: ", ((time.time() - t1) * 1000), " (ms)")
    return crimes


@bodo.jit(cache=True)
def get_top_crime_types(crimes):
    t1 = time.time()
    top_crime_types = crimes['Primary Type'].value_counts().index[0:10]
    print("Getting top crimes Time: ", ((time.time() - t1) * 1000), " (ms)")
    return top_crime_types

@bodo.jit(cache=True)
def filter_crimes(crimes, top_crime_types):
    t1 = time.time()
    top_crimes = crimes[crimes['Primary Type'].isin(top_crime_types)]
    print("Filtering crimes Time: ", ((time.time() - t1) * 1000), " (ms)")
    return top_crimes


@bodo.jit(cache=True)
def get_crimes_count_date(crimes):
    t1 = time.time()
    crimes_count_date = crimes.pivot_table(index='date only', columns='Primary Type', values='ID', aggfunc="count")
    print("Computing Crime Pattern Time: ", ((time.time() - t1) * 1000), " (ms)")
    return crimes_count_date

@bodo.jit
def get_crimes_type_date(crimes_count_date):
    t1 = time.time()
    crimes_count_date.index = pd.DatetimeIndex(crimes_count_date.index)
    result = crimes_count_date.fillna(0).rolling(365).sum()
    result = result.sort_index(ascending=False)
    print("Computing Crime Pattern Time: ", ((time.time() - t1) * 1000), " (ms)")
    return result

@bodo.jit(distributed=['crimes', 'crimes_days'], cache=True)
def get_crimes_by_days(crimes):
    t1 = time.time()
    crimes_days = crimes.groupby('dow', as_index=False)['ID'].count().sort_values(by='dow')
    print("Group by days Time: ", ((time.time() - t1) * 1000), " (ms)")
    return crimes_days

@bodo.jit(distributed=['crimes', 'crimes_months'], cache=True)
def get_crimes_by_months(crimes):
    t1 = time.time()
    crimes['month'] = crimes["Date"].dt.month
    crimes_months = crimes.groupby('month', as_index=False)['ID'].count().sort_values(by='month')
    print("Group by days Time: ", ((time.time() - t1) * 1000), " (ms)")
    return crimes_months

@bodo.jit(distributed=['crimes', 'crimes_type'], cache=True)
def get_crimes_by_type(crimes):
    t1 = time.time()
    crimes_type = crimes.groupby('Primary Type', as_index=False)['ID'].count().sort_values(by='ID', ascending=False)
    print("Group by days Time: ", ((time.time() - t1) * 1000), " (ms)")
    return crimes_type

@bodo.jit(distributed=['crimes', 'crimes_location'], cache=True)
def get_crimes_by_location(crimes):
    t1 = time.time()
    crimes_location = crimes.groupby('Location Description', as_index=False)['ID'].count().sort_values(by='ID', ascending=False)
    print("Group by days Time: ", ((time.time() - t1) * 1000), " (ms)")
    return crimes_location


def main():
    print(f"Hello World from rank {bodo.get_rank()}. Total ranks={bodo.get_size()}")

    print("Load Crimes Data in Chicago 2012_to_2017")
    crimes1 = load_chicago_crimes()
    if bodo.get_rank()==0:
        print(crimes1.head())

    print("Preprocessing and Cleaning")
    crimes = data_cleanup(crimes1)
    if bodo.get_rank()==0:
        print(crimes.head())

    top_crime_types = get_top_crime_types(crimes)
    top_crime_types = bodo.allgatherv(top_crime_types)
    if bodo.get_rank()==0:
        print(top_crime_types)

    crimes = filter_crimes(crimes, top_crime_types)
    if bodo.get_rank()==0:
        print(crimes.head())

    print("Crime Analysis\n")
    print("Find Pattern of each crime over the years\n")

    crimes_count_date = get_crimes_count_date(crimes)

    get_crimes_type_dates= get_crimes_type_date(crimes_count_date)
    if bodo.get_rank()==0:
        print(get_crimes_type_dates.head())

    print("A general view of crime records by time, type and location")
    print("Determining the pattern on daily basis")

    crimes_days = get_crimes_by_days(crimes)
    if bodo.get_rank()==0:
        print(crimes_days.head())

    print("Determining the pattern on monthly basis\n")

    crimes_months = get_crimes_by_months(crimes)
    if bodo.get_rank()==0:
        print(crimes_months.head())


    print("Determining the pattern by crime type\n")

    crimes_type = get_crimes_by_type(crimes)
    if bodo.get_rank()==0:
        print(crimes_type.head())

    print("Determining the pattern by location\n")
    crimes_location = get_crimes_by_location(crimes)
    if bodo.get_rank()==0:
        print(crimes_location.head())


if __name__ == '__main__':
    main()