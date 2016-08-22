# Replication files for An Analysis of the Militarized Interstate Dispute (MID) Dataset, 1816-2001

This repository contains R scripts to generate non-directed/directed dyad-year MID data derived from Gibler, Miller and Little's MID replication project forthcoming in *International Studies Quarterly*. [Doug Gibler](http://dmgibler.people.ua.edu) has [a dedicated section of his website](http://dmgibler.people.ua.edu/mid-data.html) for the project. This Github repository houses relevant scripts to generate data for dyad-year analyses using our corrected MID data. This repo includes:

1. Stata .dta files for non-directed and directed dyad-year observations from 1816 to 2008. I generated these files from [EUGene](http://eugenesoftware.org/) (v. 3.204). The R scripts I provide extend these observations from 2008 to 2010.
2. Corrected dispute-level and participant-level data sets similar to what the Correlates of War project provides as "MID_A" and "MID_B", respectively. These .csv files contain our corrected data from 1816 to 2001 as well as version 4.01 of the CoW-MID data for new disputes from 2001 to 2010.
3. Two R scripts, intuitively titled, to generate non-directed dyad-year (`NDY`) and directed dyad-year (`DDY`) data sets. These R scripts require the `DataCombine`, `foreign`, and `reshape2` packages in R and also lean heavily on data transformation via `sqldf` and `plyr`.
4. Two finished non-directed dyad-year and directed dyad-year data sets for interested users who do not use the R programming language.

Please contact me at svmille@clemson.edu with any inquiries about these scripts.

