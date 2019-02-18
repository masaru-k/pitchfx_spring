library(sqldf)
library(purrr)
library(tidyverse)
library(lubridate)
#working directory
setwd("D:/baseball/MLB/pitchrx")

#this script requires the game-by-game data from the other script
Pitcher_Game_Stats = read.csv("Pitcher_Game_Stats_Pfx.csv")
Pitcher_Game_Stats$month = month(Pitcher_Game_Stats$date)


Pitcher_Season_Stats = sqldf("select pitcher,season,
                             sum(FB_V_T) as FB_V_T,sum(FB_Z_T) as FB_Z_T,sum(FB_X) as FB_X,
                             sum(FB_Strikes) as FB_Strikes,sum(FB_C) as FB_C, sum(OS_V_T) as OS_V_T,
                             sum(OS_Z_T) as OS_Z_T, sum(OS_X) as OS_X, sum(OS_Strikes) as OS_Strikes,
                             sum(OS_C) as OS_C, sum(PA) as PA, sum(K) as K, sum(BB) as BB, sum(woba_value) as woba_value
                             from Pitcher_Game_Stats
                             where season_type = 2
                             group by pitcher,season")

Pitcher_Spring_Stats = sqldf("select pitcher,season,
                             sum(FB_V_T) as FB_V_T,sum(FB_Z_T) as FB_Z_T,sum(FB_X) as FB_X,
                             sum(FB_Strikes) as FB_Strikes,sum(FB_C) as FB_C, sum(OS_V_T) as OS_V_T,
                             sum(OS_Z_T) as OS_Z_T, sum(OS_X) as OS_X, sum(OS_Strikes) as OS_Strikes,
                             sum(OS_C) as OS_C, sum(PA) as PA, sum(K) as K, sum(BB) as BB, sum(woba_value) as woba_value
                             from Pitcher_Game_Stats
                             where season_type = 1
                             group by pitcher,season")

Pitcher_April_Stats = sqldf("select pitcher,season,
                             sum(FB_V_T) as FB_V_T,sum(FB_Z_T) as FB_Z_T,sum(FB_X) as FB_X,
                             sum(FB_Strikes) as FB_Strikes,sum(FB_C) as FB_C, sum(OS_V_T) as OS_V_T,
                             sum(OS_Z_T) as OS_Z_T, sum(OS_X) as OS_X, sum(OS_Strikes) as OS_Strikes,
                             sum(OS_C) as OS_C, sum(PA) as PA, sum(K) as K, sum(BB) as BB, sum(woba_value) as woba_value
                             from Pitcher_Game_Stats
                             where season_type = 2 and month = 4
                             group by pitcher,season")

Pitcher_Comb_Stats = sqldf("select a.*, b.FB_V_T as FB_V_T_LS, b.FB_Z_T as FB_Z_T_LS, b.FB_Strikes as FB_Strikes_LS, b.FB_C as FB_C_LS,
                         b.OS_V_T as OS_V_T_LS, b.OS_Z_T as OS_Z_T_LS, b.OS_Strikes as OS_Strikes_LS, b.OS_C as OS_C_LS,
                         b.K as K_LS, b.BB as BB_LS, b.PA as PA_LS, b.woba_value as woba_value_LS,
                         c.FB_V_T as FB_V_T_S, c.FB_Z_T as FB_Z_T_S, c.FB_Strikes as FB_Strikes_S, c.FB_C as FB_C_S,
                         c.OS_V_T as OS_V_T_S, c.OS_Z_T as OS_Z_T_S, c.OS_Strikes as OS_Strikes_S, c.OS_C as OS_C_S,
                         c.K as K_S, c.BB as BB_S, c.PA as PA_S, c.woba_value as woba_value_S
                         from Pitcher_April_Stats a
                         left join Pitcher_Season_Stats b
                         on a.pitcher = b.pitcher
                         and a.season = b.season + 1
                         left join Pitcher_Spring_Stats c
                         on a.pitcher = c.pitcher
                         and a.season = c.season")


Pitcher_Comb_Stats = sqldf("select a.*, b.FB_V_T as FB_V_T_SLS, b.FB_Z_T as FB_Z_T_SLS, b.FB_Strikes as FB_Strikes_SLS, b.FB_C as FB_C_SLS,
                         b.OS_V_T as OS_V_T_SLS, b.OS_Z_T as OS_Z_T_SLS, b.OS_Strikes as OS_Strikes_SLS, b.OS_C as OS_C_SLS,
                         b.K as K_SLS, b.BB as BB_SLS, b.PA as PA_SLS, b.woba_value as woba_value_SLS
                         from Pitcher_Comb_Stats a
                         left join Pitcher_Spring_Stats b
                         on a.pitcher = b.pitcher
                         and a.season = b.season + 1")

Pitcher_Comb_Stats = sqldf("select a.*, b.FB_V_T as FB_V_T_CS, b.FB_Z_T as FB_Z_T_CS, b.FB_Strikes as FB_Strikes_CS, b.FB_C as FB_C_CS,
                         b.OS_V_T as OS_V_T_CS, b.OS_Z_T as OS_Z_T_CS, b.OS_Strikes as OS_Strikes_CS, b.OS_C as OS_C_CS,
                           b.K as K_CS, b.BB as BB_CS, b.PA as PA_CS, b.woba_value as woba_value_CS
                           from Pitcher_Comb_Stats a
                           left join Pitcher_Season_Stats b
                           on a.pitcher = b.pitcher
                           and a.season = b.season")

Pitcher_Game_Stats$date = ymd(Pitcher_Game_Stats$date)

#last 20 days, regular season stats only (to be used in future articles)
Pitcher_L20_Stats = sqldf("select distinct pitcher,date,season from Pitcher_Game_Stats")
Pitcher_L20_Stats$date = ymd(Pitcher_L20_Stats$date)
Pitcher_L20_Stats = sqldf("select a.*, sum(b.FB_V_T) as FB_V_T, sum(b.FB_Z_T) as FB_Z_T,
                          sum(b.FB_Strikes) as FB_Strikes, sum(b.FB_C) as FB_C,
                          sum(b.OS_V_T) as OS_V_T, sum(b.OS_Z_T) as OS_Z_T, sum(b.OS_Strikes) as OS_Strikes,
                          sum(b.OS_C) as OS_C, sum(b.K) as K, sum(b.BB) as BB, sum(b.PA) as PA, sum(b.woba_value) as woba_value
                          from Pitcher_L20_Stats a
                          left join Pitcher_Game_Stats b
                          on a.pitcher = b.pitcher
                          and a.season = b.season
                          and a.date > b.date
                          and a.date < b.date + 21
                          and b.season_type = 2
                          group by a.pitcher,a.season,a.date")

#to-date stats, regular season stats only (to be used in future articles)
Pitcher_TD_Stats = sqldf("select distinct pitcher,date,season from Pitcher_Game_Stats")
Pitcher_TD_Stats$date = ymd(Pitcher_TD_Stats$date)
Pitcher_TD_Stats = sqldf("select a.*, sum(b.FB_V_T) as FB_V_T, sum(b.FB_Z_T) as FB_Z_T,
                          sum(b.FB_Strikes) as FB_Strikes, sum(b.FB_C) as FB_C,
                          sum(b.OS_V_T) as OS_V_T, sum(b.OS_Z_T) as OS_Z_T, sum(b.OS_Strikes) as OS_Strikes,
                          sum(b.OS_C) as OS_C, sum(b.K) as K, sum(b.BB) as BB, sum(b.PA) as PA, sum(b.woba_value) as woba_value
                          from Pitcher_TD_Stats a
                          left join Pitcher_Game_Stats b
                          on a.pitcher = b.pitcher
                          and a.season = b.season
                          and a.date > b.date
                          and b.season_type = 2
                          group by a.pitcher,a.season,a.date")

##Calculate Averages and model

Pitcher_Comb_Stats$April_WOBA = Pitcher_Comb_Stats$woba_value / Pitcher_Comb_Stats$PA
Pitcher_Comb_Stats$FS_WOBA = Pitcher_Comb_Stats$woba_value_CS / Pitcher_Comb_Stats$PA_CS
Pitcher_Comb_Stats$ROS_WOBA = (Pitcher_Comb_Stats$woba_value_CS-Pitcher_Comb_Stats$woba_value) / (-Pitcher_Comb_Stats$PA+Pitcher_Comb_Stats$PA_CS)

Pitcher_Comb_Stats$FS_FBV = Pitcher_Comb_Stats$FB_V_T_CS / Pitcher_Comb_Stats$FB_C_CS
Pitcher_Comb_Stats$April_FBV = Pitcher_Comb_Stats$FB_V_T / Pitcher_Comb_Stats$FB_C
Pitcher_Comb_Stats$April_FBZ = Pitcher_Comb_Stats$FB_Z_T / Pitcher_Comb_Stats$FB_C

Pitcher_Comb_Stats$April_K = Pitcher_Comb_Stats$K / Pitcher_Comb_Stats$PA
Pitcher_Comb_Stats$April_BB = Pitcher_Comb_Stats$BB / Pitcher_Comb_Stats$PA

Pitcher_Comb_Stats$ROS_FBV = (Pitcher_Comb_Stats$FB_V_T_CS-Pitcher_Comb_Stats$FB_V_T) / (Pitcher_Comb_Stats$FB_C_CS-Pitcher_Comb_Stats$FB_C)

Pitcher_Comb_Stats$LS_WOBA = Pitcher_Comb_Stats$woba_value_LS / Pitcher_Comb_Stats$PA_LS
Pitcher_Comb_Stats$LS_FBV= Pitcher_Comb_Stats$FB_V_T_LS / Pitcher_Comb_Stats$FB_C_LS
Pitcher_Comb_Stats$LS_FBZ= Pitcher_Comb_Stats$FB_Z_T_LS / Pitcher_Comb_Stats$FB_C_LS
Pitcher_Comb_Stats$LS_OSV= Pitcher_Comb_Stats$OS_V_T_LS / Pitcher_Comb_Stats$OS_C_LS
Pitcher_Comb_Stats$LS_OSZ= Pitcher_Comb_Stats$OS_Z_T_LS / Pitcher_Comb_Stats$OS_C_LS
Pitcher_Comb_Stats$LS_K_Rate= Pitcher_Comb_Stats$K_LS / Pitcher_Comb_Stats$PA_LS
Pitcher_Comb_Stats$LS_BB_Rate= Pitcher_Comb_Stats$BB_LS / Pitcher_Comb_Stats$PA_LS
Pitcher_Comb_Stats$LS_FB_Strike= Pitcher_Comb_Stats$FB_Strikes / Pitcher_Comb_Stats$FB_C_LS

Pitcher_Comb_Stats$Spring_FBV = Pitcher_Comb_Stats$FB_V_T_S / Pitcher_Comb_Stats$FB_C_S
Pitcher_Comb_Stats$Spring_FBZ = Pitcher_Comb_Stats$FB_Z_T_S / Pitcher_Comb_Stats$FB_C_S
Pitcher_Comb_Stats$Spring_OSV = Pitcher_Comb_Stats$OS_V_T_S / Pitcher_Comb_Stats$OS_C_S
Pitcher_Comb_Stats$Spring_OSZ = Pitcher_Comb_Stats$OS_Z_T_S / Pitcher_Comb_Stats$OS_C_S
Pitcher_Comb_Stats$Spring_FB_Strike = Pitcher_Comb_Stats$FB_Strikes_S / Pitcher_Comb_Stats$FB_C_S
Pitcher_Comb_Stats$Spring_K_Rate = Pitcher_Comb_Stats$K_S / Pitcher_Comb_Stats$PA_S
Pitcher_Comb_Stats$Spring_BB_Rate = Pitcher_Comb_Stats$BB_S / Pitcher_Comb_Stats$PA_S

Pitcher_Comb_Stats$LSpring_FBV = Pitcher_Comb_Stats$FB_V_T_SLS / Pitcher_Comb_Stats$FB_C_SLS
Pitcher_Comb_Stats$LSpring_FBZ = Pitcher_Comb_Stats$FB_Z_T_SLS / Pitcher_Comb_Stats$FB_C_SLS
Pitcher_Comb_Stats$LSpring_OSV = Pitcher_Comb_Stats$OS_V_T_SLS / Pitcher_Comb_Stats$OS_C_SLS
Pitcher_Comb_Stats$LSpring_OSZ = Pitcher_Comb_Stats$OS_Z_T_SLS / Pitcher_Comb_Stats$OS_C_SLS
Pitcher_Comb_Stats$LSpring_FB_Strike = Pitcher_Comb_Stats$FB_Strikes_SLS / Pitcher_Comb_Stats$FB_C_SLS
Pitcher_Comb_Stats$LSpring_K_Rate = Pitcher_Comb_Stats$K_SLS / Pitcher_Comb_Stats$PA_SLS
Pitcher_Comb_Stats$LSpring_BB_Rate = Pitcher_Comb_Stats$BB_SLS / Pitcher_Comb_Stats$PA_SLS

#histogram
hist(subset(Pitcher_Comb_Stats,PA_LS>200 & PA >40 &FB_C_S>10)$Spring_FBZ-subset(Pitcher_Comb_Stats,PA_LS>200 & PA >40 &FB_C_S>10)$LS_FBZ,freq=FALSE,breaks=20, main="Change in Fastball Z-Movement, Qualifying Pitchers",xlab="Difference in Inches, Spring - Last Season")

#April wOBA, K-Rate, and Velocity Models
April_WOBA = lm(April_WOBA ~ I(LS_K_Rate*100) + I(LS_BB_Rate*100) + I(Spring_FBV-LS_FBV)  , subset(Pitcher_Comb_Stats,PA_LS>200 & PA >40 &FB_C_S>10 & abs(LS_FBZ-Spring_FBZ)<5 ))
summary(April_WOBA)

April_FBV = lm(April_FBV ~ Spring_FBV + LS_FBV , subset(Pitcher_Comb_Stats,PA_LS>200 & PA>40&FB_C_S>10& abs(LS_FBZ-Spring_FBZ)<5 )   )
summary(April_FBV)

April_K = lm(April_K ~ I(LS_K_Rate) + I(Spring_FBV -LS_FBV) , subset(Pitcher_Comb_Stats,PA_LS>200 & PA>40&FB_C_S>10& abs(LS_FBZ-Spring_FBZ)<5 )   )
summary(April_K)

#Group Data
write.csv(subset(Pitcher_Comb_Stats,PA_LS>200 & PA>40&FB_C_S>10& abs(LS_FBZ-Spring_FBZ)<5 ),"Group_Study.csv")


#Rest of Season Models
ROS_FBV = lm(ROS_FBV ~ LS_FBV+Spring_FBV  , subset(Pitcher_Comb_Stats,PA_LS>200 & PA>40&FB_C_S>10& abs(LS_FBZ-Spring_FBZ)<5 ))
summary(FS_FBV)

ROS_WOBA = lm(ROS_WOBA ~  I(LS_K_Rate*100) + I(LS_BB_Rate*100) + I(Spring_FBV-LS_FBV) , subset(Pitcher_Comb_Stats,PA_LS>200 & PA>40&FB_C_S>10 &abs(LS_FBZ-Spring_FBZ)<5 ))
summary(ROS_WOBA)
