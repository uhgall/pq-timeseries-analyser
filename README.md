# pq-timeseries-analyser

A simple tool for looking at parquet files with time series data. 

I wrote it for a friend's electric hydrofoil surfboard prototype that logged so much data that it was hard to find what matters and was producing bandwidth issues, so part of the point was to identify how to clean up the data logging. 

It could be helpful for any device that logs data with time stamps. 

It automatically figures out which variables hardly ever change (software version, other more or less static attributes), and calculates how much storage/bandwidth is wasted on reporting these over and over again. 

It also looks at the set of possible value for each variable so that they can be visualized appropriately - for example, booleans are shown like this:

<img width="585" alt="image" src="https://user-images.githubusercontent.com/84516/206874245-59a9bb0d-52b0-4790-904c-039f2c61779f.png">

