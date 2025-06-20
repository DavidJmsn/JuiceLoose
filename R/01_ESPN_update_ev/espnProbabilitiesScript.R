
# EXPECTED VALUE CALCULATOR & BETTOR - VERSION 2  -------------------------

rm(list = ls())
gc()

setwd("/Users/david/PersonalDevelopment/EVC")

args <- commandArgs(trailingOnly = TRUE)

# Check if at least one argument is provided
if (length(args) == 0) {
  cat("No arguments provided.\n")
  sport = 'NBA'
} else {
  cat("Here's the argument you passed: ", args[1], "\n")
  sport = args[1]
}

# sport = 'NBA'
# sport = 'NFL'
# sport = 'NHL'
# sport = 'ERE'
# sport = 'MLB'
# sport = 'EPL'

# The main goal of this version is to automatically obtain win probabilities 
# from ESPN. and automatically run the script on days with games 

## Check and install needed packages
needed <- c('R6','jsonlite','data.table','DT','tidyverse','tools','httr',
            'plotly','openxlsx2')
missing <- needed[!(needed %in% utils::installed.packages()[, "Package"])]
if (length(missing)) {
  if (interactive()) {
    answer <- askYesNo(paste("Package(s)", paste(missing, collapse = " "),
                             "needed. Install from CRAN?"))
    if (answer)
      install.packages(missing, repos = "https://cloud.r-project.org/")
  } else {
    stop("Install missing packages: ", paste(missing, collapse = ", "))
  }
}

library(R6)
library(jsonlite)
library(data.table)
library(DT)
library(tidyverse)
library(tools)
library(httr)
library(plotly)
library(openxlsx2)

# 1 CHECK GAMES -----------------------------------------------------------
message(timestamp())
cat('\n')

# Here we want to call espns api to look for games. 
# If there is an event that day we proceed to step 2
config <- switch(EXPR = sport, 
                 'NBA' = yaml::yaml.load_file("NBA/Dictionary/espnTeams.yml"),
                 'NFL' = yaml::yaml.load_file("NFL/Dictionary/espnTeams.yml"),
                 'NHL' = yaml::yaml.load_file("NHL/Dictionary/espnTeams.yml"),
                 'ERE' = yaml::yaml.load_file("ERE/Dictionary/espnTeams.yml"),
                 'MLB' = yaml::yaml.load_file("MLB/Dictionary/espnTeams.yml"),
                 'EPL' = yaml::yaml.load_file("EPL/Dictionary/espnTeams.yml"))

retrieveEvents <- function(spt = 'NBA', days = c('today', 'tomorrow')){
  today <- as.numeric(gsub(pattern = '-', replacement = '', x = Sys.Date()))
  if('today' %in% days){
    todays_eventsJSON <- switch(EXPR = spt,
                                'NBA' = GET(paste0('http://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events?lang=en&region=us&dates=', today)),
                                'NFL' = GET(paste0('http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events?lang=en&region=us&dates=', today)),
                                'NHL' = GET(paste0('http://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events?lang=en&region=us&dates=', today)),
                                'ERE' = GET(paste0('http://sports.core.api.espn.com/v2/sports/soccer/leagues/ned.1/events/?lang=en&region=nl&dates=', today)),
                                'MLB' = GET(paste0('http://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/?lang=en&region=us&dates=', today)),
                                'EPL' = GET(paste0('http://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/?lang=en&region=nl&dates=', today))
    )
    if (status_code(todays_eventsJSON) == 200) {
      todays_data <- fromJSON(content(todays_eventsJSON, "text"))
    } else {
      stop("Failed to fetch eventsJSON: HTTP ", status_code(eventsJSON))
    }
  } else {
    todays_data = NULL
  }
  
  if('tomorrow' %in% days){
    tomorrow <- today + 1
    tomorrows_eventsJSON <- switch(EXPR = spt,
                                   'NBA' = GET(paste0('http://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events?lang=en&region=us&dates=', tomorrow)),
                                   'NFL' = GET(paste0('http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events?lang=en&region=us&dates=', tomorrow)),
                                   'NHL' = GET(paste0('http://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events?lang=en&region=us&dates=', tomorrow)),
                                   'ERE' = GET(paste0('http://sports.core.api.espn.com/v2/sports/soccer/leagues/ned.1/events/?lang=en&region=nl&dates=', tomorrow)),
                                   'MLB' = GET(paste0('http://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/?lang=en&region=us&dates=', tomorrow)),
                                   'EPL' = GET(paste0('http://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1/events/?lang=en&region=nl&dates=', tomorrow))
    )
    if (status_code(tomorrows_eventsJSON) == 200) {
      tomorrows_data <- fromJSON(content(tomorrows_eventsJSON, "text"))
    } else {
      stop("Failed to fetch eventsJSON: HTTP ", status_code(tomorrows_eventsJSON))
    }
  } else {
    tomorrows_data = NULL
  }
  return(c(todays_data, tomorrows_data))
}

retrieveProbabilities <- function(event){
  
  gameID = gsub('[\\/?]', '',str_extract(event, '\\/[0-9]+\\?'))
  
  predictorUrl = paste0(sub('\\?lang.*', '',event), 
                        '/competitions/', gameID, '/predictor?lang=en&region=us')
  
  oddsUrl = paste0(sub('\\?lang.*', '',event), 
                   '/competitions/', gameID, '/odds?lang=en&region=us')
  
  # if(grepl('nhl', event) | grepl('ned.1', event) | grepl('mlb', event) | grepl('eng.1', event)){
  if(grepl('nhl', event) | grepl('ned.1', event) | grepl('eng.1', event)){
    oddsJSON = GET(oddsUrl)
    if (status_code(oddsJSON) == 200) {
      odds = fromJSON(content(oddsJSON, "text"))
    } else {
      stop("Failed to fetch oddsJSON: HTTP ", status_code(oddsJSON))
    }
    game_info = list(o = odds)
    return(game_info)
  } else {
    predictorJSON = GET(predictorUrl)
    if (status_code(predictorJSON) == 200) {
      predictor = fromJSON(content(predictorJSON, "text"))
    } else {
      stop("Failed to fetch predictorJSON: HTTP ", status_code(predictorJSON))
    }
    oddsJSON = GET(oddsUrl)
    if (status_code(oddsJSON) == 200) {
      odds = fromJSON(content(oddsJSON, "text"))
    } else {
      stop("Failed to fetch oddsJSON: HTTP ", status_code(oddsJSON))
    }
  }
  game_info = list(p = predictor,  o = odds)
  return(game_info)
}

get_stats <- function(prob_list = pb){
  
  game_id = gsub('[\\/\\/]', '',str_extract(prob_list[["p"]][["$ref"]], '\\/[0-9]+\\/'))
  
  home_team_id = gsub('[\\/?]', '',
                      str_extract(prob_list[["p"]][["homeTeam"]][["team"]][["$ref"]], 
                                  '\\/[0-9]+\\?'))
  
  away_team_id = gsub('[\\/?]', '',
                      str_extract(prob_list[["p"]][["awayTeam"]][["team"]][["$ref"]], 
                                  '\\/[0-9]+\\?'))
  
  stats = data.table(prob_list[["p"]][["awayTeam"]][["statistics"]])
  st = data.table::transpose(stats[,.(displayName, value)], make.names = 'displayName')
  st[, `:=` (GAME_ID = game_id,
             HOME_TEAM_ID = home_team_id,
             AWAY_TEAM_ID = away_team_id)]
  return(st)
}

sport_to_key <- function(sp = sport){
  
  if(sp == "NBA"){
    return("basketball_nba")
  } else if(sp == "NFL"){
    return("americanfootball_nfl")
  } else if(sp == "NHL"){
    return("icehockey_nhl")
  } else if(sp == "ERE"){
    return("soccer_netherlands_eredivisie")
  } else if(sp == "MLB"){
    return("baseball_mlb")
  } else if(sp == "EPL"){
    return("soccer_epl")
  } 
  
}

sort_home_away2 <- function(dt = data){
  dt[Team == `Home Team`, `:=` (`Home/Away` = 'Home')]
  dt[Team == `Away Team`, `:=` (`Home/Away` = 'Away')]
  return(dt)
}

get_prob <- function(EV, line){
  prob <- ((EV+100)/line)
  return(prob)
}

unnest_jsonData <- function(jsonData = jsonBettingData){
  jsonData_un <- unnest_longer(jsonData, col = "bookmakers")
  jsonData_un <- as.data.table(jsonData_un)
  jsonData_un <- unnest_longer(jsonData_un, col = "bookmakers.markets", )
  jsonData_un <- as.data.table(jsonData_un)
  jsonData_un <- unnest_longer(jsonData_un, col = "bookmakers.markets.outcomes")
  jsonData_un <- as.data.table(jsonData_un)
  return(jsonData_un)
}

format_betting_data <- function(jbd = jsonBettingData_un){
  new_names <- c("id", 
                 "sport_key",
                 "Sport",
                 "Start Time",
                 "Home Team",
                 "Away Team",
                 "bookmakers.key",
                 "Book",
                 "bookmakers.last_update",
                 "Market",
                 "Last Update",
                 "Team",
                 "Price",
                 "Points")
  setnames(jbd, new_names)
  return(jbd)
}

calcExpectedValue <- function(line, winProb){
  EV = (line*winProb)-100
  return(EV)
}

calc_kelly_crit <- function(price = Price, winP = winProb){
  b = decimal_to_fraction(price)
  p = winP/100
  q = (1 - p)
  kc = p - (q/b)
  return(kc)
}

get_line <- function(EV, winProb){
  line = ((EV+100)/winProb)
  return(line)
}

decimal_to_fraction <- function(decimal_odds){
  fo = decimal_odds - 1
  return(fo)
}

set_all_columns_to_character <- function(dt) {
  # Ensure the input is a data.table
  setDT(dt)
  # Loop through each column and convert it to character
  for (col in names(dt)) {
    dt[[col]] <- as.character(dt[[col]])
  }
  return(dt)
}

# EVENT CLASS -------------------------------------------------------------

eventClass <- R6Class("eventClass",
                      public = list(
                        # Properties
                        sport = 'NBA', 
                        days = c('today', 'tomorrow'),
                        dates = Sys.Date(),
                        events = list(),
                        
                        # Constructor
                        initialize = function(sport = 'NBA', 
                                              days = c('today', 'tomorrow')) {
                          self$sport <- sport
                          self$days <- days
                          self$dates <- switch(EXPR = days,
                                               'today' = Sys.Date(),
                                               'tomorrow' = Sys.Date() + 1)
                          self$events <- retrieveEvents(sport, days = days)
                          
                          cat("Event Information -------------------------------", '\n')
                          cat("Sport:            ", self$sport, '\n')
                          cat("Day:              ", self$days, '\n')
                          cat("Dates:            ", as.character(self$dates), '\n')
                          cat("Number of events: ", self$events$count, "\n")
                          cat("Event Information -------------------------------", '\n')
                        },
                        
                        # Methods
                        numEvents = function() {
                          cat("Number of events: ", self$events$count, "\n")
                        },
                        
                        eventURLs = function() {
                          return(unlist(self$events$items, recursive = F))
                        },
                        
                        retrieve_game_Probabilities = function() {
                          return(lapply(self$events$items, retrieveProbabilities))
                        },
                        
                        gameIDs = function() {
                          lapply(self$events$items, function(event) {
                            return(gsub('[\\/?]', '',
                                        str_extract(event, '\\/[0-9]+\\?')))
                          })
                        }
                        
                      )
)

# CREATE EVENT CLASS ------------------------------------------------------

todays_events <- eventClass$new(sport, 'today')

tomorrows_events <- eventClass$new(sport, 'tomorrow')

if((todays_events[["events"]][["count"]] + 
    tomorrows_events[["events"]][["count"]]) == 0){ 
  stop('No events today')
}

# GAME CLASS --------------------------------------------------------------

gameClass <- R6Class("gameClass",
                     public = list(
                       # Properties
                       game_info = list(),
                       dates = NULL,
                       
                       # Constructor
                       initialize = function(events) {
                         self$game_info <- lapply(events$events$items$`$ref`, retrieveProbabilities)
                         self$dates <- events$dates
                         names(self$game_info) <- lapply(self$game_info, function(game){ return(game[["p"]][["shortName"]])})
                         cat("Event Information -------------------------------", '\n')
                         cat("Sport:            ", events$sport, '\n')
                         cat("Dates:            ", as.character(self$dates), '\n')
                         cat("Number of events: ", events$events$count, "\n")
                         cat("Event Information -------------------------------", '\n')
                       },
                       
                       # Methods
                       # gameIDs = function() {
                       #   # lapply(events$events$items, function(event) {return(gsub('[\\/?]', '',str_extract(event, '\\/[0-9]+\\?')))})
                       #   return(names(self$game_info))
                       # },
                       
                       gameNames = function() {
                         return(lapply(self$game_info, function(game){ return(game[["p"]][["shortName"]])}))
                       },
                       
                       getStats = function() {
                         stats = rbindlist(lapply(self$game_info, get_stats), idcol = 'GAME')
                         return(stats[,`:=` (GAME_DATE = as.character(self$dates))])
                       }
                       
                       # getBooks = function() {
                       #   books = self[["game_info"]][["ORL @ DET"]][["o"]][["items"]]
                       #   return(lapply(books,function(book){return(book$provider$name)}))
                       # }
                       
                     )
)


# CREATE GAME CLASS -------------------------------------------------------

todays_probabilities <- gameClass$new(todays_events)

tomorrows_probabilities <- gameClass$new(tomorrows_events)

todays_stats <- todays_probabilities$getStats()
tomorrows_stats <- tomorrows_probabilities$getStats()

stats <- rbindlist(list(todays_stats, tomorrows_stats), fill = TRUE)[!is.na(GAME)]

urls <- c(todays_events$eventURLs(),tomorrows_events$eventURLs())

if(sport == "MLB"){
  stats[, `:=` (HOME_TEAM = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 2),
                HOME_WIN_PROB = `Team Chance Loss`,
                AWAY_TEAM = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 1),
                AWAY_WIN_PROB = `WIN PROB`,
                `WIN PROB` = NULL,
                GAME_URL = urls)]
} else if(sport == "NFL"){
  stats[, `:=` (HOME_TEAM = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 2),
                HOME_WIN_PROB = `Team Chance Loss`,
                AWAY_TEAM = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 1),
                AWAY_WIN_PROB = `WIN PROB`,
                `WIN PROB` = NULL,
                GAME_URL = urls)]
} else {
  stats[, `:=` (HOME_TEAM = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 2),
                HOME_WIN_PROB = `Team Chance Loss`,
                HOME_PT_DIFF = -1 * `Team Pred Pt Diff`,
                HOME_EXP_PT = `Opp Expected Pts`,
                AWAY_TEAM = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 1),
                AWAY_WIN_PROB = `WIN PROB`,
                AWAY_PT_DIFF = `Team Pred Pt Diff`,
                AWAY_EXP_PT = `Team Expected Pts`,
                `WIN PROB` = NULL,
                `Team Chance Loss` = NULL,
                `Team Pred Pt Diff` = NULL,
                `Team Expected Pts` = NULL,
                `Opp Expected Pts` = NULL,
                GAME_URL = urls)]
}



stats[, HOME_CONFIG_ID := sapply(HOME_TEAM, function(x) config[[x]][['id']])]
stats[, AWAY_CONFIG_ID := sapply(AWAY_TEAM, function(x) config[[x]][['id']])]

if(sport == 'MLB'){
  home_stats <- stats[, .(GAME_ID,
                          GAME,
                          GAME_DATE,
                          # `Matchup Quality`,
                          GAME_URL,
                          TEAM = HOME_TEAM,
                          OPP = AWAY_TEAM,
                          WIN_PROB = HOME_WIN_PROB,
                          # PT_DIFF = HOME_PT_DIFF,
                          # EXP_PT = HOME_EXP_PT,
                          TEAM_ID = HOME_TEAM_ID,
                          CONFIG_ID = HOME_CONFIG_ID)]
  
  away_stats <- stats[, .(GAME_ID,
                          GAME,
                          GAME_DATE,
                          # `Matchup Quality`,
                          GAME_URL,
                          TEAM = AWAY_TEAM,
                          OPP = HOME_TEAM, 
                          WIN_PROB = AWAY_WIN_PROB,
                          # PT_DIFF = AWAY_PT_DIFF,
                          # EXP_PT = AWAY_EXP_PT,
                          TEAM_ID = AWAY_TEAM_ID,
                          CONFIG_ID = AWAY_CONFIG_ID)]
} else if(sport == "NFL"){
  home_stats <- stats[, .(GAME_ID,
                          GAME,
                          GAME_DATE,
                          # `Matchup Quality`,
                          GAME_URL,
                          TEAM = HOME_TEAM,
                          OPP = AWAY_TEAM,
                          WIN_PROB = HOME_WIN_PROB,
                          # PT_DIFF = HOME_PT_DIFF,
                          # EXP_PT = HOME_EXP_PT,
                          TEAM_ID = HOME_TEAM_ID,
                          CONFIG_ID = HOME_CONFIG_ID)]
  
  away_stats <- stats[, .(GAME_ID,
                          GAME,
                          GAME_DATE,
                          # `Matchup Quality`,
                          GAME_URL,
                          TEAM = AWAY_TEAM,
                          OPP = HOME_TEAM, 
                          WIN_PROB = AWAY_WIN_PROB,
                          # PT_DIFF = AWAY_PT_DIFF,
                          # EXP_PT = AWAY_EXP_PT,
                          TEAM_ID = AWAY_TEAM_ID,
                          CONFIG_ID = AWAY_CONFIG_ID)]
} else {
  home_stats <- stats[, .(GAME_ID,
                          GAME,
                          GAME_DATE,
                          `Matchup Quality`,
                          GAME_URL,
                          TEAM = HOME_TEAM,
                          OPP = AWAY_TEAM,
                          WIN_PROB = HOME_WIN_PROB,
                          PT_DIFF = HOME_PT_DIFF,
                          EXP_PT = HOME_EXP_PT,
                          TEAM_ID = HOME_TEAM_ID,
                          CONFIG_ID = HOME_CONFIG_ID)]
  
  away_stats <- stats[, .(GAME_ID,
                          GAME,
                          GAME_DATE,
                          `Matchup Quality`,
                          GAME_URL,
                          TEAM = AWAY_TEAM,
                          OPP = HOME_TEAM, 
                          WIN_PROB = AWAY_WIN_PROB,
                          PT_DIFF = AWAY_PT_DIFF,
                          EXP_PT = AWAY_EXP_PT,
                          TEAM_ID = AWAY_TEAM_ID,
                          CONFIG_ID = AWAY_CONFIG_ID)]
}


# Combine the home and away teams data
combined_stats <- rbind(away_stats, home_stats)

# Order by GameID and Date
if(sport == 'MLB'){
  setorder(combined_stats, GAME_ID, GAME, GAME_DATE)
}else  if (sport == "NFL"){
  setorder(combined_stats, GAME_ID, GAME, GAME_DATE)
} else {
  setorder(combined_stats, GAME_ID, GAME, GAME_DATE, `Matchup Quality`)
  
}

# 2 RETRIEVE ODDS FROM ODDS API -------------------------------------------

keys <- yaml::yaml.load_file("/Users/david/odds_api.yml")

host <- "https://api.the-odds-api.com"
regions <- "us"
oddsFormat <- "decimal"
markets <- c("h2h",
             "spreads",
             "totals")
apiKey <- keys$ODDS_API_KEY
todaysDate <- Sys.Date()
todaysTime <- gsub(pattern = ':', '', str_extract(Sys.time(), pattern = '\\d{2}:\\d{2}:\\d{2}'))
commenceTimeFrom <- Sys.time()
commenceTimeTo <- commenceTimeFrom + 86400

myURL <- paste(host, "/v4/sports/", 
               sport_to_key(sport), "/odds/?regions=", 
               regions, "&oddsFormat=", 
               oddsFormat, "&markets=", 
               paste(markets, collapse = ","), "&apiKey=", 
               apiKey, sep = "")

cat('Retrieving: ', myURL, '\n')
cat('...', '\n')
rawBettingData <- GET(myURL)

if (status_code(rawBettingData) == 200) {
  cat('Successful!', '\n')
  rawBettingDataContent <- content(rawBettingData, as = "text")
} else {
  stop("Failed to fetch rawBettingData: HTTP ", status_code(rawBettingData))
}

jsonBettingData <- fromJSON(rawBettingDataContent)
jsonBettingData_un <- unnest_jsonData(jsonBettingData)
BettingData <- format_betting_data(jbd = jsonBettingData_un)
BettingData[Market == 'h2h', `:=` (MIN = min(Price),
                                   MAX = max(Price),
                                   MEAN = mean(Price),
                                   MEDIAN = median(Price),
                                   SD = sd(Price),
                                   VAR = var(Price)), 
            by = .(id, Team)]

sort_home_away2(BettingData)

Dict <- fread(paste0(sport,"/Dictionary/",sport,"_teams.csv"),
              sep = ":",
              header = FALSE,
              col.names = c('AB', 'Team'))

BD <- Dict[BettingData, on = c("Team")
           ][Market == "h2h", .SD[which.max(Price), -c("Points")], by = .(id, Team)]

BD[, `:=` (TEAM_AB = AB)]
BD[, `:=` (AB = NULL)]
BD[Dict, on = c(`Away Team` = 'Team'), AWAY_AB := i.AB]
BD[Dict, on = c(`Home Team` = 'Team'), HOME_AB := i.AB]
BD[,`:=` (Game = paste(`AWAY_AB`, "@", `HOME_AB`, sep = " "))]
BD[,`:=`(game_date = as.POSIXct(gsub('([A-z])', ' ', `Start Time`), tz = "UTC"))]
BD[, `:=` (game_date = with_tz(game_date,  tzone = "America/New_York"))]
BD[, `:=` (game_date = as.character(date(game_date)))]

Quota = list()
Quota$used <- rawBettingData$headers$`x-requests-used`
Quota$remaining <- rawBettingData$headers$`x-requests-remaining`
cat("Quota Information -------------------------------", '\n')
cat("Queries used:           ", Quota$used, '\n') 
cat("Queries remaining:      ", Quota$remaining, '\n')
cat("Quota Information -------------------------------", '\n')

# 3 MERGE ODDS API INFO AND ESPN INFO  ------------------------------------

tape <- fread(paste0(sport, '/history/', 'history.csv'))

# Creating an empty data.table with specified column names
column_names = names(tape)  # Example column names
empty_dt <- data.table(matrix(ncol = length(column_names), nrow = 0))
colnames(empty_dt) <- column_names

dt <- rbindlist(list(empty_dt, combined_stats[BD, on = c(TEAM = 'TEAM_AB', GAME = 'Game', GAME_DATE = 'game_date')]), fill = TRUE)

setorder(dt, `Start Time`, GAME_ID, `Home/Away`)

# 4 CALCULATE EV AND KELLY CRITERION --------------------------------------

dt[, `:=` (MIN_PRICE = get_line(EV = 0.001, winProb = WIN_PROB))]
dt[, `:=` (EV = round(calcExpectedValue(line = Price,winProb = WIN_PROB),2),
           KC = round((calc_kelly_crit(price = Price, winP = WIN_PROB)), 2))]
dt[,`:=` (`Start Time` = as.POSIXct(gsub('([A-z])', ' ', `Start Time`), tz = "UTC"),
          `Last Update` = as.POSIXct(gsub('([A-z])', ' ', `Last Update`), tz = "UTC"),
          bookmakers.last_update = as.POSIXct(gsub('([A-z])', ' ', bookmakers.last_update), tz = "UTC"))]
dt[,`:=` (`Start Time` = with_tz(`Start Time`, tzone = "America/New_York"),
          `Last Update` = with_tz(`Last Update`, tzone = "America/New_York"),
          bookmakers.last_update = with_tz(bookmakers.last_update, tzone = "America/New_York"))]

if(sport == 'MLB'){
  setcolorder(dt,c('GAME',
                   'TEAM',
                   'Home/Away',
                   'Start Time',
                   'WIN_PROB',
                   'Price',
                   'EV',
                   'KC',
                   'MIN_PRICE',
                   'Book',
                   'Team',
                   'TEAM_ID',
                   'CONFIG_ID',
                   'id',
                   'sport_key',
                   'Sport',
                   'Home Team',
                   'Away Team',
                   'bookmakers.key',
                   'bookmakers.last_update',
                   'Market',
                   'Last Update',
                   'MIN',
                   'MAX',
                   'MEAN',
                   'MEDIAN',
                   'SD',
                   'VAR'))
} else if(sport == "NFL"){
  setcolorder(dt,c('GAME',
                   'TEAM',
                   'Home/Away',
                   'Start Time',
                   'WIN_PROB',
                   'Price',
                   'EV',
                   'KC',
                   'MIN_PRICE',
                   'Book',
                   'Team',
                   'TEAM_ID',
                   'CONFIG_ID',
                   'id',
                   'sport_key',
                   'Sport',
                   'Home Team',
                   'Away Team',
                   'bookmakers.key',
                   'bookmakers.last_update',
                   'Market',
                   'Last Update',
                   'MIN',
                   'MAX',
                   'MEAN',
                   'MEDIAN',
                   'SD',
                   'VAR'))
} else {
  setcolorder(dt,c('GAME',
                   'TEAM',
                   'Home/Away',
                   'Start Time',
                   'WIN_PROB',
                   'Price',
                   'EV',
                   'KC',
                   'MIN_PRICE',
                   'Book',
                   'PT_DIFF',
                   'EXP_PT',
                   'Team',
                   'TEAM_ID',
                   'CONFIG_ID',
                   'id',
                   'sport_key',
                   'Sport',
                   'Home Team',
                   'Away Team',
                   'bookmakers.key',
                   'Matchup Quality',
                   'bookmakers.last_update',
                   'Market',
                   'Last Update',
                   'MIN',
                   'MAX',
                   'MEAN',
                   'MEDIAN',
                   'SD',
                   'VAR'))
}


# 5 IDENTIFY BETS  --------------------------------------------------------

# 6 MESSAGE USER WITH BET INFO --------------------------------------------

message_bets <- function(bets = posititve_games) {
  
  string = paste0('', '# --------------------------------', '\n',
                  bets$GAME, ':           ', 'Bet $', bets$KC*100, '\n',
                  'DATE:                    ', bets$GAME_DATE, '\n',
                  'TEAM:                    ', bets$TEAM, '\n',
                  'BOOK:                    ', bets$Book, '\n',
                  'LINE:                       ', bets$Price, '\n',
                  'MINIMUM LINE:    ', round(bets$MIN_PRICE,2), '\n',
                  '# --------------------------------', '\n')
  return(string)
}

my_string = paste0('', '# --------------------------------', '\n',
                   '#------------- ',"Bets" , ' -------------#', '\n',
                   timestamp(prefix = '#-- ', suffix = ' --#'), '\n',
                   'Sport:                           ', sport, '\n',
                   'Games analyzed:        ', dt[,.N]*0.5, '\n',
                   'Bets recommended:  ', dt[KC > 0, .N], '\n', 
                   "Queries used:             ", Quota$used, '\n',
                   "Queries remaining:    ", Quota$remaining, '\n',
                   '# --------------------------------', '\n')
for(n in 1:nrow(dt[KC > 0])){
  my_string = paste0(my_string, '', n, ' of ', nrow(dt[KC > 0]), '\n')
  my_string = paste0(my_string, message_bets(bets = dt[KC > 0][n]))
}

cat("Bet Information ---------------------------------", '\n')
cat(my_string)
cat("Bet Information ---------------------------------", '\n')

# Construct the AppleScript command
appleScriptCommand <- sprintf('tell application "Messages"\n
                              set targetBuddy to "%s"\n
                              set targetService to id of 1st account whose service type = iMessage\n
                              set textMessage to "%s"\n
                              set theBuddy to participant targetBuddy of account id targetService\n
                              send textMessage to theBuddy\n
                              end tell', keys$CONTACT, my_string)

cat("Sending message ---------------------------------", '\n')

# Execute the AppleScript command
system(paste("osascript -e", shQuote(appleScriptCommand)))
cat("Message sent ------------------------------------", '\n')

# 7 SAVE DATA -------------------------------------------------------------
if(sport == 'MLB'){
  dt$CONFIG_ID <- sapply(dt$CONFIG_ID, toString)
  openxlsx2::write_xlsx(data.frame(dt), file = paste0(sport, '/dailyFiles/', todaysDate, '_', todaysTime, '_', 'espnBets.xlsx'))
} else {
  openxlsx2::write_xlsx(dt, file = paste0(sport, '/dailyFiles/', todaysDate, '_', todaysTime, '_', 'espnBets.xlsx'))
}
new_tape <- rbindlist(list(set_all_columns_to_character(dt), set_all_columns_to_character(tape)), fill = TRUE)

# setorder(new_tape, -GAME_DATE, GAME_ID)

fwrite(new_tape, paste0(sport, '/history/', 'history.csv'))

timestamp()
cat('\n')

