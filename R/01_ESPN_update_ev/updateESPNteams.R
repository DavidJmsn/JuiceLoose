# 
# getNBAprobs <- function(date='now') {
#   full_url <- 'http://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events?lang=en&region=us'
#   print(full_url)
#   
#   events <- GET(full_url)
#   if (status_code(events) == 200) {
#     data <- fromJSON(content(events, "text"), flatten = TRUE)
#     # return(data)
#   } else {
#     stop("Failed to fetch events: HTTP ", status_code(events))
#   }
#   
#   games_prob <- list()
#   for(g in data$items$`$ref`){
#     event_url <- g
#     game <- GET(event_url)
#     if (status_code(game) == 200) {
#       game_data <- fromJSON(content(game, "text"), flatten = TRUE)
#       teams <- game_data[["competitions"]][["competitors"]][[1]][["id"]]
#       power_index_url_1 <- paste0('http://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/',game_data$id,'/competitions/',game_data$id,'/powerindex/',teams[1],'?lang=en&region=us')
#       power_index_url_2 <- paste0('http://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/',game_data$id,'/competitions/',game_data$id,'/powerindex/',teams[2],'?lang=en&region=us')
#       print(power_index_url_1)
#       pi_1 <- GET(power_index_url_1)
#       if (status_code(pi_1) == 200) {
#         power_index_1 <- fromJSON(content(pi_1, "text"), flatten = TRUE)
#         power_index_2 <- fromJSON(content(pi_2, "text"), flatten = TRUE)
#         games_prob[[game_data$name]] <- list(power_index_1, power_index_2, game_data)
#         # return(data)
#       } else {
#         stop("Failed to fetch pi: HTTP ", status_code(pi_1))
#       }
#       # print(power_index_url_2)
#       # pi_2 <- GET(power_index_url_2)
#       # if (status_code(pi_2) == 200) {
#       #   power_index <- fromJSON(content(pi, "text"), flatten = TRUE)
#       #   games_prob[[game_data$name]] <- list(power_index, game_data)
#       #   # return(data)
#       # } else {
#       #   stop("Failed to fetch pi: HTTP ", status_code(pi_2))
#       # }
#       # return(game_data)
#       # games_prob[[game_data$name]] <- game_data
#     } else {
#       stop("Failed to fetch game: HTTP ", status_code(game))
#     }
#   }
#   
#   return(games_prob)
# }
# 
# # nba[[g]][["competitions"]][["competitors"]][[1]][["id"]]
# 
# # getNBAprobs <- function(date='now') {
# #   full_url <- 'http://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events?lang=en&region=us'
# #   print(full_url)
# # 
# #   events <- GET(full_url)
# #   if (status_code(events) == 200) {
# #     data <- fromJSON(content(events, "text"), flatten = TRUE)
# #     return(data)
# #   } else {
# #     stop("Failed to fetch events: HTTP ", status_code(events))
# #   }
# # }
# 
# 
# nba <- getNBAprobs()

# games_prob <- list()
# for(g in nba$items$`$ref`){
#   event_url <- g
#   game <- GET(event_url)
#   if (status_code(game) == 200) {
#     game_data <- fromJSON(content(game, "text"), flatten = TRUE)
#     # return(game_data)
#     games_prob[[game_data$id]] <- game_data
#   } else {
#     stop("Failed to fetch game: HTTP ", status_code(game))
#   }
# }

rm(list = ls())
gc()


# sport = 'TENNIS'
# sport = 'GOLF'
# sport = 'F1'
# sport = 'EPL'
# sport = 'MLB'
# sport = 'ERE'
# sport = 'NBA'
# sport = 'NFL'
sport = 'NHL'
# sport = 'ERE'

setwd("/Users/david/PersonalDevelopment/EVC")

library(R6)
library(jsonlite)
library(data.table)
library(DT)
library(tidyverse)
library(tools)
library(httr)
library(plotly)
library(openxlsx2)

# List of paths to create
daily_files = paste0(sport, '/dailyFiles')
history = paste0(sport, '/history')
dictionary = paste0(sport, '/Dictionary')

paths <- c(sport, daily_files, history, dictionary)

# Loop through each path and create it
for (p in paths) {
  dir.create(p, recursive = TRUE)
}

teamDict <- function(spt = 'NBA'){
  teamsJSON <- switch(EXPR = spt,
                      'NBA' = GET('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams'),
                      'NFL' = GET('https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams'),
                      'NHL' = GET('https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams'),
                      'ERE' = GET('https://site.api.espn.com/apis/site/v2/sports/soccer/ned.1/teams'),
                      'MLB' = GET('https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams'),
                      'EPL' = GET('https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams'),
                      'F1' = GET('https://site.api.espn.com/apis/site/v2/sports/racing/f1/teams'),
                      'GOLF' = GET('https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard'),
                      'TENNIS' = GET('https://site.api.espn.com/apis/site/v2/sports/tennis/atp/players'))
  
  if (status_code(teamsJSON) == 200) {
    # data <- fromJSON(content(teamsJSON, "text"))
    data <- parse_json(content(teamsJSON, "text"))
    return(data)
  } else {
    stop("Failed to fetch teamsJSON: HTTP ", status_code(teamsJSON))
  }
}

# nbaTeams <- teamDict('NBA')
# nflTeams <- teamDict('NFL')
# nhlTeams <- teamDict('NHL')
# ereTeams <- teamDict('ERE')
Teams <- teamDict(sport)

# yaml::write_yaml(nbaTeams[["sports"]][["leagues"]][[1]][["teams"]][[1]][["team"]], 'test/test.yml')
# config <- yaml::yaml.load_file('test/test.yml')
# yaml::write_yaml(nbaTeams, 'NBA/Dictionary/espnTeams.yml')
# yaml::write_yaml(nflTeams, 'NFL/Dictionary/espnTeams.yml')
# yaml::write_yaml(nhlTeams, 'NHL/Dictionary/espnTeams.yml')
yaml::write_yaml(Teams, paste0(sport, '/Dictionary/espnTeams.yml'))
# yaml::write_yaml(ereTeams, 'ERE/Dictionary/espnTeams.yml')

# nbaConfig <- yaml::yaml.load_file("NBA/Dictionary/espnTeams.yml")
# nflConfig <- yaml::yaml.load_file("NFL/Dictionary/espnTeams.yml")
# nhlConfig <- yaml::yaml.load_file("NHL/Dictionary/espnTeams.yml")
config <- yaml::yaml.load_file(paste0(sport, '/Dictionary/espnTeams.yml'))

# yaml::write_yaml(nbaConfig[["sports"]][[1]][["leagues"]][[1]][["teams"]], 'NBA/Dictionary/espnTeams.yml')
# yaml::write_yaml(nflConfig[["sports"]][[1]][["leagues"]][[1]][["teams"]], 'NFL/Dictionary/espnTeams.yml')
# yaml::write_yaml(nhlConfig[["sports"]][[1]][["leagues"]][[1]][["teams"]], 'NHL/Dictionary/espnTeams.yml')
yaml::write_yaml(config[["sports"]][[1]][["leagues"]][[1]][["teams"]], paste0(sport, '/Dictionary/espnTeams.yml'))

# nbaConfig <- yaml::yaml.load_file("NBA/Dictionary/espnTeams.yml")
# nflConfig <- yaml::yaml.load_file("NFL/Dictionary/espnTeams.yml")
# nhlConfig <- yaml::yaml.load_file("NHL/Dictionary/espnTeams.yml")
config <- yaml::yaml.load_file(paste0(sport, '/Dictionary/espnTeams.yml'))

# new_nba <- list()
# for(t in nbaConfig){
#   print(t)
#   new_nba[[t$team$abbreviation]] <- t$team
# }
# yaml::write_yaml(new_nba, 'NBA/Dictionary/espnTeams.yml')
# 
# new_nfl <- list()
# for(t in nflConfig){
#   print(t)
#   new_nfl[[t$team$abbreviation]] <- t$team
# }
# yaml::write_yaml(new_nfl, 'NFL/Dictionary/espnTeams.yml')
# 
# new_nhl <- list()
# for(t in nhlConfig){
#   print(t)
#   new_nhl[[t$team$abbreviation]] <- t$team
# }
# yaml::write_yaml(new_nhl, 'NHL/Dictionary/espnTeams.yml')

new <- list()
for(t in config){
  print(t)
  new[[t$team$abbreviation]] <- t$team
}
yaml::write_yaml(new, paste0(sport, '/Dictionary/espnTeams.yml'))

# nbaConfig <- yaml::yaml.load_file("NBA/Dictionary/espnTeams.yml")
# nflConfig <- yaml::yaml.load_file("NFL/Dictionary/espnTeams.yml")
# nhlConfig <- yaml::yaml.load_file("NHL/Dictionary/espnTeams.yml")
config <- yaml::yaml.load_file(paste0(sport, '/Dictionary/espnTeams.yml'))

# names(nbaConfig)
# names(nflConfig)
# names(nhlConfig)
names(config)


# Make teams.csv file
test1 <- lapply(config, function(x) {return(data.table(AB = x$abbreviation, Team = x$displayName))})

test2 <- rbindlist(test1)

fwrite(test2, paste0(sport,"/Dictionary/",sport,"_teams.csv"), sep = ':', col.names = FALSE)


# test <- function(spt = 'NBA'){
#   teamsJSON <- switch(EXPR = spt,
#                       'NBA' = GET('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams'),
#                       'NFL' = GET('https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams'),
#                       'NHL' = GET('https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams'))
#   return(fromJSON(teamsJSON))
# }
# 
# test()
# 
# 
# x <- jsonlite::parse_json(readLines('test/teams.json'))
# 
# x
# 
# yaml::write_yaml(x, 'test/test.yml')
# 
# teams <- yaml::yaml.load_file("test/test.yml")
# 
# teams$ARI
# 
# # which(teams$id == '17')
# newTeams <- list()
# for(t in teams){
#   print(t)
#   newTeams[[t$team$abbreviation]] <- t$team
# }
# 
# yaml::write_yaml(newTeams, 'test/test.yml')
# 
# teams <- yaml::yaml.load_file("test/test.yml")


# todays_eventsJSON <- GET(paste0(host, '/v4/sports/?apiKey=', apiKey))
# if (status_code(todays_eventsJSON) == 200) {
#   # todays_data <- parse_json(content(todays_eventsJSON, "text"))
#   todays_data <- fromJSON(content(todays_eventsJSON, "text"))
#   # return(todays_data)
# } else {
#   stop("Failed to fetch eventsJSON: HTTP ", status_code(eventsJSON))
# }


