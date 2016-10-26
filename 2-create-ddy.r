setwd("~/Dropbox/projects/mid-project/replication")
library(sqldf)
library(plyr)
library(DataCombine)
library(foreign)

Part <- read.csv("~/Dropbox/projects/mid-project/clean-data-script/gml-MIDB_4.01.csv")
Disp <- read.csv("~/Dropbox/projects/mid-project/clean-data-script/gml-MIDA_4.01.csv")
COWDDY <- read.dta("~/Dropbox/data/EUGene-output/ddy-1816-2008.dta")

Part <- adply(Part, 1, summarise, year = seq(styear, endyear))[c("dispnum3", "ccode", "stabb", "year", "sidea", "revstate","revtype1","revtype2","fatality","fatalpre","hiact","hostlev","orig")]

Part <- with(Part, Part[order(dispnum3, ccode, year),])

DDY <- sqldf("select A.dispnum3 dispnum3, A.ccode ccode1, B.ccode ccode2, A.year year1, B.year year2,
      A.sidea sidea1, B.sidea sidea2, A.revstate revstate1, B.revstate revstate2, 
      A.revtype1 revtype11, B.revtype1 revtype12, A.revtype2 revtype21, B.revtype2 revtype22, 
      A.fatality fatality1, B.fatality fatality2, A.fatalpre fatalpre1, B.fatalpre fatalpre2, 
      A.hiact hiact1, B.hiact hiact2, A.hostlev hostlev1, B.hostlev hostlev2, 
      A.orig orig1, B.orig orig2
      from Part A join Part B using (dispnum3) 
      where A.ccode != B.ccode AND sidea1 != sidea2 AND year1 == year2
      order by A.ccode, B.ccode")

DDY$year2 <- NULL
DDY <- rename(DDY, c("year1" = "year"))


DDY <- with(DDY, DDY[order(dispnum3),])
# DDY[!duplicated(DDY$dispnum3, DDY$ccode1, DDY$ccode2, DDY$year), ]
DDY <- unique( DDY[ , 1:ncol(DDY) ] )

DDY$midongoing <- 1

DDY$midonset <- with(DDY, ave(year, dispnum3, FUN = function(x)
  as.integer(c(TRUE, tail(x, -1L) != head(x, -1L) + 1L))))

DDY <- MoveFront(DDY, c("dispnum3", "ccode1", "ccode2", "year", "midonset", "midongoing"), exact = FALSE)

DDYm <- merge(COWDDY, DDY, all.x=TRUE)

# write.table(DDYm,file="gml-ddy.csv",sep=",",row.names=F,na="")

Disps <- with(Disp, data.frame(dispnum3, hiact, hostlev, mindur, maxdur, outcome, settle, fatality, fatalpre, stmon))

DDY <- join(DDY, Disps, by=c("dispnum3"), type="left")




# Remove duplicates
# --------------------------------------------------
# This will start the arduous task of removing duplicate observations.

# Identify duplicates.
# R's `duplicated' function doesn't quite do what I want it to do.
# For example, France and Italy have three "new" MID onsets in 1860 (MID#0112, MID#0113, MID#0306), but only the last two are declared "duplicates" because MID#0112 comes first in the data set.
# So, here's what we'll do. 1) create a `dup` variable counting "duplicates" forward. Then, 2) create a `duprev' variable counting backward.
# This would solve our France-Italy 1860 problem.
DDY$dup <- as.numeric(duplicated(DDY[,2:4]))
DDY$duprev <- as.numeric(duplicated(DDY[,2:4], fromLast = TRUE))
DDY$multiplemids <- with(DDY, ifelse(dup == 1 | duprev == 1, 1, 0)) # It's a duplicate if either one of those is true.

# Create a `removeme` variable. Set it to 0.
DDY$removeme <- 0

# First rule
# ------------------------------
# if there are multiple dyad-years, eliminate the ones that aren't new MID onsets.

DDY$removeme <- with(DDY, ifelse(multiplemids == 1 & midonset == 0, 1, removeme))
DDY <- subset(DDY, !(removeme == 1))

# Rewrite `dup`, `duprev` and `multiplemids`
DDY$dup <- as.numeric(duplicated(DDY[,2:4]))
DDY$duprev <- as.numeric(duplicated(DDY[,2:4], fromLast = TRUE))
DDY$multiplemids <- with(DDY, ifelse(dup == 1 | duprev == 1, 1, 0)) # It's a duplicate if either one of those is true.

# Second rule
# ------------------------------
# keep the highest hostlev.

DDY <- ddply(DDY, c("ccode1", "ccode2", "year"), function(DDY) return(DDY[DDY$hostlev==max(DDY$hostlev),]))

# Rewrite `dup`, `duprev` and `multiplemids`
DDY$dup <- as.numeric(duplicated(DDY[,2:4]))
DDY$duprev <- as.numeric(duplicated(DDY[,2:4], fromLast = TRUE))
DDY$multiplemids <- with(DDY, ifelse(dup == 1 | duprev == 1, 1, 0)) # It's a duplicate if either one of those is true.

# Third rule
# ------------------------------
# keep the highest fatality

DDY <- ddply(DDY, c("ccode1", "ccode2", "year"), function(DDY) return(DDY[DDY$fatality==max(DDY$fatality),]))

# Rewrite `dup`, `duprev` and `multiplemids`
DDY$dup <- as.numeric(duplicated(DDY[,2:4]))
DDY$duprev <- as.numeric(duplicated(DDY[,2:4], fromLast = TRUE))
DDY$multiplemids <- with(DDY, ifelse(dup == 1 | duprev == 1, 1, 0)) # It's a duplicate if either one of those is true.

# Fourth rule
# ------------------------------
# Keep the highest mindur

DDY <- ddply(DDY, c("ccode1", "ccode2", "year"), function(DDY) return(DDY[DDY$mindur==max(DDY$mindur),]))

# Rewrite `dup`, `duprev` and `multiplemids`
DDY$dup <- as.numeric(duplicated(DDY[,2:4]))
DDY$duprev <- as.numeric(duplicated(DDY[,2:4], fromLast = TRUE))
DDY$multiplemids <- with(DDY, ifelse(dup == 1 | duprev == 1, 1, 0)) # It's a duplicate if either one of those is true.

# Fifth rule
# ------------------------------
# Keep whichever one came first.

DDY <- ddply(DDY, c("ccode1", "ccode2", "year"), function(DDY) return(DDY[DDY$stmon==min(DDY$stmon),]))

# Rewrite `dup`, `duprev` and `multiplemids`
DDY$dup <- as.numeric(duplicated(DDY[,2:4]))
DDY$duprev <- as.numeric(duplicated(DDY[,2:4], fromLast = TRUE))
DDY$multiplemids <- with(DDY, ifelse(dup == 1 | duprev == 1, 1, 0)) # It's a duplicate if either one of those is true.

# Unbelievably, we still have three MIDs left (MID#2163, MID#3604, and a few MID#0258s).

# Sixth rule
# Keep the highest fatalpre. This will solve the MID#2163-MID#3604 problem. MID#2163 had more fatalities.

DDY <- ddply(DDY, c("ccode1", "ccode2", "year"), function(DDY) return(DDY[DDY$fatalpre==max(DDY$fatalpre),]))

# Rewrite `dup`, `duprev` and `multiplemids`
DDY$dup <- as.numeric(duplicated(DDY[,2:4]))
DDY$duprev <- as.numeric(duplicated(DDY[,2:4], fromLast = TRUE))
DDY$multiplemids <- with(DDY, ifelse(dup == 1 | duprev == 1, 1, 0)) # It's a duplicate if either one of those is true.

# Seventh and final rule
# Get rid of it if dup == 1.

DDY <- subset(DDY, !(dup == 1))

# Rewrite `dup`, `duprev` and `multiplemids`
DDY$dup <- as.numeric(duplicated(DDY[,2:4]))
DDY$duprev <- as.numeric(duplicated(DDY[,2:4], fromLast = TRUE))
DDY$multiplemids <- with(DDY, ifelse(dup == 1 | duprev == 1, 1, 0)) # It's a duplicate if either one of those is true.

# Success

DDY$dyad <- with(DDY, as.numeric(paste0("1",sprintf("%03d", ccode1), sprintf("%03d", ccode2))))

# Merge into full COWDDY frame.
# --------------------------------------------------
# This goes to just 2008, so we'll need to carry to 2010.
# We could add rows for South Sudan, but it begins in 2011.

COWDDY$dyad <- with(COWDDY, paste0(sprintf("%03d", ccode1), "-", sprintf("%03d", ccode2)))

Alivein2008 <- subset(COWDDY, year == 2008)

EmptyPDF <- data.frame(
        dyad =unique(Alivein2008$dyad),
	 dates = c("2009-2010"))

require(reshape2)
EmptyPDF <- cbind(EmptyPDF, colsplit(EmptyPDF$dates, "-", c("start", "end")))
EmptyPDF <- adply(EmptyPDF, 1, summarise, year = seq(start, end))[c("dyad", "year")]

EmptyPDF <- cbind(EmptyPDF, colsplit(EmptyPDF$dyad, "-", c("ccode1", "ccode2")))

EmptyPDF$dyad <- NULL
COWDDY$dyad <- NULL

COWDDY <- rbind(COWDDY, EmptyPDF)

# Merge, finally.

Data <- join(COWDDY, DDY, by=c("ccode1", "ccode2", "year"), type="left")

sqldf("select ccode1, ccode2, year, dispnum3 from Data where ccode1 == 2 AND ccode2 == 20")

write.table(Data,file="gml-ddy.csv",sep=",",row.names=F,na="")
