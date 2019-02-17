library(sqldf)
library(purrr)
library(tidyverse)
library(lubridate)
#working directory
setwd("D:/baseball/MLB/pitchrx")
library(pitchRx)


#create database and scrape
db <- src_sqlite("pitchfx.sqlite3", create = F)
files <- c("miniscoreboard.xml")
scrape(start = "2013-02-15", end = "2013-11-15", connect = db$con)
scrape(start = "2014-02-15", end = "2014-11-15", connect = db$con)
#scrape(start = "2014-07-23", end = "2014-11-15", connect = db$con)

scrape(start = "2015-02-15", end = "2015-11-15", connect = db$con)
scrape(start = "2016-02-15", end = "2016-11-15", connect = db$con)
scrape(start = "2017-02-15", end = "2017-11-15", connect = db$con)
scrape(start = "2018-02-15", end = "2018-11-15", connect = db$con)

#combine pitch and plate appearance data
pitch_data = dbGetQuery(db$con, 'select des,id,tfs,x,y,sv_id,start_speed,end_speed,sz_top,sz_bot,pfx_x,
                      pfx_z,break_angle,break_length,pitch_type,zone,spin_rate,url,num,event_num,count,gameday_link,inning_side,inning from pitch' )
ab_data = dbGetQuery(db$con, 'select pitcher,batter,num,start_tfs,stand,p_throws,event,gameday_link,date,event_num from atbat' )
unique(ab_data$event_num)
unique(ab_data$gameday_link)
unique(pitch_data$gameday_link)
head(ab_data)
head(pitch_data)
pitch_comb = sqldf("select a.*, b.pitcher,b.batter,b.start_tfs,b.stand,b.p_throws,b.event,b.date
                   from pitch_data a
                   left join ab_data b
                   on a.gameday_link = b.gameday_link
                   and a.num = b.num")
rm(ab_data)
rm(pitch_data)
pitch_comb = unique(pitch_comb)
#label seasons and spring training

pitch_comb$date = ymd(pitch_comb$date)
pitch_comb$season = year(pitch_comb$date)

#1 = spring, 2 = regular, 3 = post
pitch_comb$season_type = ifelse(pitch_comb$season == 2013 & pitch_comb$date < "2013-04-01",1,2)
pitch_comb$season_type = ifelse(pitch_comb$season == 2013 & pitch_comb$date > "2013-09-30",3,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2014 & pitch_comb$date < "2013-04-01",1,2)
pitch_comb$season_type = ifelse(pitch_comb$season == 2014 & pitch_comb$date > "2013-09-30",3,pitch_comb$season_type)

#label if pitch is a fastball (all types). FF, CU, FT, FC, SI, FS are fastballs.

pitch_comb$FB = ifelse(pitch_comb$pitch_type %in% c("FF","CU","FT","FC","SI","FS"),1,0)

#calculate woba using same scale as statcast (I made the list by hand)
#write.csv(unique(pitch_comb$event),"event_types_woba.csv")


woba_event_val = read.csv("woba_event_list.csv")

pitch_comb = sqldf("select a.*, b.woba_value from pitch_comb a
                   left join woba_event_val b
                   on a.event = b.Event_Type")

#find the pitch that closes out each at bat for calculation of PA purposes. maybe a better way to do this but I am dumb
subset(pitch_comb,season_type==1)
terminal_event = sqldf("select gameday_link,pitcher,batter,num,max(id) as last_pitch_num from pitch_comb
                       group by gameday_link,pitcher,batter,num")

pitch_comb = sqldf("select a.*, b.last_pitch_num as terminal_event from pitch_comb a
                   left join terminal_event b
                   on a.gameday_link = b.gameday_link
                   and a.pitcher = b.pitcher
                   and a.batter = b.batter
                   and a.num = b.num")

#get leagues of each team, throw out non-mlb games
pitch_comb$league_t1 = substr(pitch_comb$gameday_link,19,21)
pitch_comb$league_t2 = substr(pitch_comb$gameday_link,26,28)

pitch_comb = subset(pitch_comb, league_t1=="mlb" & league_t2 =="mlb")

pitch_comb = subset(pitch_comb, select = -c(league_t1,league_t2))
#done with the database. I like to write from a CSV here for safe keeping. now we can calculate pitcher stats by game
#write.csv(pitch_comb,"pfx_comb.csv")

Pitcher_Game_Stats = sqldf("select pitcher,date,season,season_type,
                           sum(case when FB=1 then start_speed else 0 end) as FB_V_T,
                           sum(case when FB=1 then pfx_z else 0 end) as FB_Z_T,
                           sum(case when FB=1 then pfx_x else 0 end) as FB_X,
                           sum(case when FB=1 and zone < 10 then 1 else 0 end) as FB_Strikes,
                           sum(case when FB=1 and start_speed is not null then 1 else 0 end) as FB_C,
                           sum(case when FB=0 then start_speed else 0 end) as OS_V_T,
                           sum(case when FB=0 then pfx_z else 0 end) as OS_Z_T,
                           sum(case when FB=0 then pfx_x else 0 end) as OS_X,
                           sum(case when FB=0 and zone < 10 then 1 else 0 end) as OS_Strikes,
                           sum(case when FB=0 and start_speed is not null then 1 else 0 end) as OS_C,
                           sum(case when terminal_event = id then 1 else 0 end) as PA,
                           sum(case when terminal_event = id and (event = 'Strikeout' or event = 'Strikeout - DP') then 1 else 0 end) as K,
                           sum(case when terminal_event = id and (event = 'Walk') then 1 else 0 end) as BB,
                           sum(case when terminal_event = id then woba_value else 0 end) as woba_value
                           from pitch_comb group by pitcher,date,season,season_type
                           ")

#note that not all spring games actually have pitchfx data
