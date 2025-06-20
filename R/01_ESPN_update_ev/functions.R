
# EXPECTED VALUE FUNCTIONS ------------------------------------------------\

# LIBRARIES ---------------------------------------------------------------

library(R6)
library(jsonlite)
library(data.table)
library(DT)
library(tidyverse)
library(tools)
library(httr)
library(plotly)
library(openxlsx2)


# FUNCTIONS ---------------------------------------------------------------

`%ni%` <- Negate(`%in%`) 

# GET CURRENT NFL WEEK ----------------------------------------------------

get_nfl_week <- function(current_date = Sys.Date()) {
  # Define the start of the 2024 NFL season (Thursday, September 5, 2024)
  nfl_start_date <- as.Date("2024-09-02") # changed to 02 so the week would flip on tuesday

  # Calculate the difference in days from the start of the season
  day_difference <- as.numeric(difftime(current_date, nfl_start_date, units = "days"))
  
  # Calculate the current week by dividing the days by 7 and adding 1
  current_week <- ceiling(day_difference / 7)
  
  # Ensure that the week number is at least 1
  current_week <- max(1, current_week)
  
  return(current_week)
}


# RETRIEVE EVENTS FOR THIS WEEK (NFL) -------------------------------------

nfl_retrieveEvents <- function(spt = 'NFL', year = 2024, seasontype = 2, week = get_nfl_week()){
  week_eventsJSON <- switch(EXPR = spt,
                              'NFL' = GET(paste0('https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/',
                                                 year,'/types/',seasontype,'/weeks/',week,'/events')
                              )
  )
  if (status_code(week_eventsJSON) == 200) {
    week_events <- fromJSON(content(week_eventsJSON, "text"))
  } else {
    stop("Failed to fetch eventsJSON: HTTP ", status_code(week_events))
  }
  
  return(week_events)
}


# RETRIEVE PROBABILITIES OF EVENTS ----------------------------------------

nfl_retrieveProbabilities <- function(event){
  
  gameID = gsub('[\\/?]', '',str_extract(event, '\\/[0-9]+\\?'))
  
  game_dateJSON = GET(event)
  if (status_code(game_dateJSON) == 200) {
    game_date = fromJSON(content(game_dateJSON, "text"))
  } else {
    stop("Failed to fetch game_dateJSON: HTTP ", status_code(game_dateJSON))
  }
  
  predictorUrl = paste0(sub('\\?lang.*', '',event), 
                        '/competitions/', gameID, '/predictor?lang=en&region=us')
  
  oddsUrl = paste0(sub('\\?lang.*', '',event), 
                   '/competitions/', gameID, '/odds?lang=en&region=us')
  
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
  
  game_info = list(d = game_date$date, p = predictor,  o = odds)
  
  return(game_info)
}


# GET STATS FOR FROM JSON PROBABILITIES -----------------------------------

nfl_get_stats <- function(prob_list = pb){
  
  game_id = gsub('[\\/\\/]', '',str_extract(prob_list[["p"]][["$ref"]], '\\/[0-9]+\\/'))
  
  game_dates = prob_list[["d"]]
  
  home_team_id = gsub('[\\/?]', '',
                      str_extract(prob_list[["p"]][["homeTeam"]][["team"]][["$ref"]], 
                                  '\\/[0-9]+\\?'))
  
  away_team_id = gsub('[\\/?]', '',
                      str_extract(prob_list[["p"]][["awayTeam"]][["team"]][["$ref"]], 
                                  '\\/[0-9]+\\?'))
  
  home_stats = data.table(prob_list[["p"]][["homeTeam"]][["statistics"]])
  away_stats = data.table(prob_list[["p"]][["awayTeam"]][["statistics"]])
  
  comb_stats <- rbindlist(list(Home = home_stats, Away = away_stats), idcol = "Home/Away")
  
  stats <- dcast(comb_stats[, .(`Home/Away`, displayName, value)], formula = `Home/Away` ~ displayName)

  stats[, `:=` (GAME_ID = game_id,
                GAME_DATE = ymd_hm(game_dates, tz = "UTC"),
                # GAME_DATE = game_dates,
                TEAM_ID = ifelse(`Home/Away` == "Home", yes = home_team_id, no = away_team_id))]
  return(stats)
}


# GET ODDS FOR FROM JSON PROBABILITIES ------------------------------------

nfl_get_odds <- function(prob_list = pb){
  
  game_id = gsub('[\\/\\/]', '',str_extract(prob_list[["p"]][["$ref"]], '\\/[0-9]+\\/'))
  
  game_dates = prob_list[["d"]]
  
  home_team_id = gsub('[\\/?]', '',
                      str_extract(prob_list[["p"]][["homeTeam"]][["team"]][["$ref"]], 
                                  '\\/[0-9]+\\?'))
  
  away_team_id = gsub('[\\/?]', '',
                      str_extract(prob_list[["p"]][["awayTeam"]][["team"]][["$ref"]], 
                                  '\\/[0-9]+\\?'))
  
  home_odds = data.table(prob_list[["o"]])
  away_odds = data.table(prob_list[["o"]])
  
  comb_odds <- rbindlist(list(Home = home_odds, Away = away_odds), idcol = "Home/Away")
  
  odds <- dcast(comb_odds[, .(`Home/Away`, displayName, value)], formula = `Home/Away` ~ displayName)

  odds[, `:=` (GAME_ID = game_id,
               GAME_DATE = game_dates,
               TEAM_ID = ifelse(`Home/Away` == "Home", yes = home_team_id, no = away_team_id))]
  return(odds)
}


# CONVERT SPORT ARGUMENT TO KEY FOR ODDS API URL --------------------------

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


# SORT HOME AND AWAY TEAMS ------------------------------------------------

sort_home_away2 <- function(dt = data){
  dt[Team == `Home Team`, `:=` (`Home/Away` = 'Home')]
  dt[Team == `Away Team`, `:=` (`Home/Away` = 'Away')]
  return(dt)
}


# UNNEST JSON DATA --------------------------------------------------------

unnest_jsonData <- function(jsonData = jsonBettingData){
  jsonData_un <- unnest_longer(jsonData, col = "bookmakers")
  jsonData_un <- as.data.table(jsonData_un)
  jsonData_un <- unnest_longer(jsonData_un, col = "bookmakers.markets", )
  jsonData_un <- as.data.table(jsonData_un)
  jsonData_un <- unnest_longer(jsonData_un, col = "bookmakers.markets.outcomes")
  jsonData_un <- as.data.table(jsonData_un)
  return(jsonData_un)
}


# FORMAT BETTING DATA -----------------------------------------------------

format_betting_data <- function(jbd = jsonBettingData_un){
  old_names <- c('id',
                 'sport_key',
                 'sport_title',
                 'commence_time',
                 'home_team',
                 'away_team',
                 'bookmakers.key',
                 'bookmakers.title',
                 'bookmakers.last_update',
                 'bookmakers.markets.key',
                 'bookmakers.markets.last_update',
                 'bookmakers.markets.outcomes.name',
                 'bookmakers.markets.outcomes.price',
                 'bookmakers.markets.outcomes.point')
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
  setnames(x = jbd, old = old_names, new = new_names)
  return(jbd)
}


# CALCULATE EXPECTED VALUE FROM ODDS LINE AND WIN PROBABILITY -------------

calcExpectedValue <- function(line, winProb){
  EV = (line*winProb)-100
  return(EV)
}


# GET WIN PROBABILITY FROM LINE AND EV ------------------------------------

get_prob <- function(EV, line){
  prob <- ((EV+100)/line)
  return(prob)
}


# GET LINE FROM EXPECTED VALUE AND WIN PROBABILITY ------------------------

get_line <- function(EV, winProb){
  line = ((EV+100)/winProb)
  return(line)
}


# CALCULATE THE KELLY CRITERION OF A BET ----------------------------------

calc_kelly_crit <- function(price = Price, winP = winProb){
  b = decimal_to_fraction(price)
  p = winP/100
  q = (1 - p)
  kc = p - (q/b)
  return(kc)
}


# CONVERT DECIMAL ODDS TO FRACTION ODDS -----------------------------------

decimal_to_fraction <- function(decimal_odds){
  fo = decimal_odds - 1
  return(fo)
}


# AMERICAN TO DECIMAL -----------------------------------------------------

american_to_decimal <- function(american_odds){
  if(american_odds > 0){
    do = (american_odds/100) + 1
    return(do)
  } else if(american_odds < 0){
    do = (100/(-1*american_odds)) + 1
    return(do)
  }
}


# CONVERT ALL COLUMNS IN A DATA.TABLE TO CHARACTER ------------------------

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

nfl_eventClass <- R6Class("eventClass",
                      public = list(
                        # Properties
                        sport = 'NFL', 
                        week = get_nfl_week(),
                        dates = Sys.Date(),
                        events = list(),
                        
                        # Constructor
                        initialize = function(sport = 'NFL', 
                                              week = get_nfl_week()){
                          self$sport <- sport
                          self$week <- week
                          self$dates <- Sys.Date()
                          self$events <- nfl_retrieveEvents(sport)
                          
                          cat("Event Information -------------------------------", '\n')
                          cat("Sport:            ", self$sport, '\n')
                          cat("Week:              ", self$week, '\n')
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
                          return(lapply(self$events$items, nfl_retrieveProbabilities))
                        },
                        
                        gameIDs = function() {
                          lapply(self$events$items, function(event) {
                            return(gsub('[\\/?]', '',
                                        str_extract(event, '\\/[0-9]+\\?')))
                          })
                        }
                        
                        # gameDates = function() {
                        #   lapply(self$events$items, )
                        # }
                        
                      )
)


# GAME CLASS --------------------------------------------------------------

nfl_gameClass <- R6Class("gameClass",
                     public = list(
                       # Properties
                       game_info = list(),
                       dates = NULL,
                       
                       # Constructor
                       initialize = function(events) {
                         self$game_info <- lapply(events$events$items$`$ref`, nfl_retrieveProbabilities)
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
                       
                       nfl_getStats = function() {
                         stats = rbindlist(lapply(self$game_info, nfl_get_stats), idcol = 'GAME', fill = TRUE)
                         return(stats)
                       }
                       
                       # getBooks = function() {
                       #   books = self[["game_info"]][["ORL @ DET"]][["o"]][["items"]]
                       #   return(lapply(books,function(book){return(book$provider$name)}))
                       # }
                       
                     )
)


# IDENTIFY BETS -----------------------------------------------------------

nfl_message_bets <- function(bets = posititve_games) {
  
  string = paste0('', '# --------------------------------', '\n',
                  bets$GAME, ':           ', 'Bet $', bets$KC*100, '\n',
                  'DATE:                    ', bets$`Start Time`, '\n',
                  'TEAM:                    ', bets$AB, '\n',
                  'BOOK:                    ', bets$Book, '\n',
                  'LINE:                       ', bets$Price, '\n',
                  'MINIMUM LINE:    ', round(bets$MIN_PRICE,2), '\n',
                  '# --------------------------------', '\n')
  return(string)
}

spread_nfl_message_bets <- function(bets = posititve_games) {
  
  string = paste0('', '# --------------------------------', '\n',
                  bets$GAME, ':           ', 'Bet $', bets$PTS_PERCENT_ADJ*100, '\n',
                  'DATE:                    ', bets$`Start Time`, '\n',
                  'TEAM:                    ', bets$AB, '\n',
                  'POINTS:               ', bets$Points, '\n',
                  'PRED. SPREAD:       ', bets$PREDICTED_SPREAD, '\n',
                  'BOOK:                    ', bets$Book, '\n',
                  'LINE:                       ', bets$Price, '\n',
                  '# --------------------------------', '\n')
  return(string)
}


# MONEYLINE CONVERSION TABLE ----------------------------------------------

moneyline_conversion_dt <- data.table(SPREAD = seq(from = 1, to = 20, by = 0.5),
                                      FAVORITE = c(-116,123,130,137,170,197,210,222,237,252,277,299,335,368,397,427,441,456,510,561,595,631,
                                                   657,681,730,781,904,1024,1086,1147,1223,1300,1418,1520,1664,1803,1985,2182,2390)*-1,
                                      UNDERDOG = c(-104,102,108,113,141,163,174,184,196,208,229,247,277,305,328,353,365,377,422,464,492,522,
                                                   543,564,604,646,748,847,898,949,1012,1076,1173,1257,1377,1492,1642,1805,1977))

value_of_points_dt <- data.table(SPREAD = seq(from = 1, to = 18, by =1),
                                 CHANCE_OF_HITTING = c(3,3,8,3,3,5,6,3,2,4,2,2,2,5,2,3,3,3)*0.01,
                                 VALUE_OF_HALF_PT = c(6,6,21,6,6,10,13,6,3,9,5,4,5,11,5,6,6,6)*0.01)

value_of_points_dt_negative <- value_of_points_dt[, .(SPREAD = -1 * SPREAD, CHANCE_OF_HITTING, VALUE_OF_HALF_PT)]

value_of_points_dt <- rbindlist(list(value_of_points_dt, value_of_points_dt_negative))


# RESULTS FUNCTIONS -------------------------------------------------------


# RETRIEVE RESULTS --------------------------------------------------------

retrieveResults <- function(eventURL){
  
  resultsJSON = GET(eventURL)
  if (status_code(resultsJSON) == 200) {
    
    results = fromJSON(content(resultsJSON, "text"))
  } else {
    stop("Failed to fetch resultsJSON: HTTP ", status_code(resultsJSON))
  }
  
  return(results)
}


# FORMAT RESULTS ----------------------------------------------------------

nfl_format_results <- function(results){
  game = results[["shortName"]]
  home_team = str_extract(game, '([A-z]+) @ ([A-z]+)', group = 2)
  away_team = str_extract(game, '([A-z]+) @ ([A-z]+)', group = 1)
  game_id = results[["id"]]
  teams = results[["competitions"]][["competitors"]][[1]][["id"]]
  home_away = results[["competitions"]][["competitors"]][[1]][["homeAway"]]
  winner = results[["competitions"]][["competitors"]][[1]][["winner"]]
  game_date = lubridate::ymd_hm(results[["date"]])
  start_time = ymd_hm(results[["date"]], tz = "UTC")
  # game_date = data.table::as.IDate(game_date, tz = 'UTC')
  game_date = with_tz(game_date, tzone = 'America/New_York')
  game_date = as.IDate(game_date)
  
  fresults = data.table(GAME = game,
                        GAME_DATE = game_date,
                        "Start Time" = with_tz(start_time, tzone = "America/New_York"),
                        HOME_TEAM = home_team,
                        AWAY_TEAM = away_team,
                        GAME_ID = game_id,
                        TEAM_ID = teams,
                        HOME_AWAY = home_away,
                        WINNER = winner)
  
  return(fresults)
}

  
# FORMAT SCORE ------------------------------------------------------------

nfl_format_score <- function(score){
  # game = score[["shortName"]]
  # home_team = str_extract(game, '([A-z]+) @ ([A-z]+)', group = 2)
  # away_team = str_extract(game, '([A-z]+) @ ([A-z]+)', group = 1)
  game_id = str_extract(score[["$ref"]], "(\\d{9})(?=.*\\1)")
  team_id = str_extract(score[["$ref"]], "(?<=competitors/)\\d{1,2}(?=/)")
  score = score[["value"]]
  # winner = score[["winner"]]
  # game_date = lubridate::ymd_hm( score[["date"]])
  # 
  # game_date = with_tz(game_date, tzone = 'America/New_York')
  # game_date = as.IDate(game_date)
  
  # fscore = data.table(GAME = game,
  #                       GAME_DATE = game_date,
  #                       HOME_TEAM = home_team,
  #                       AWAY_TEAM = away_team,
  #                       GAME_ID = game_id,
  #                       TEAM_ID = teams,
  #                       HOME_AWAY = home_away,
  #                       WINNER = winner)
  
  fscore <- data.table(GAME_ID = game_id, 
                       TEAM_ID = team_id, 
                       SCORE = score) #,
                       # WINNER = winner)
  
  return(fscore)
}


# RETRIEVE SCORE ----------------------------------------------------------

retrieveScore <- function(scoreURL){
  
  resultsJSON = GET(scoreURL)
  if (status_code(resultsJSON) == 200) {
    
    results = fromJSON(content(resultsJSON, "text"))
  } else {
    stop("Failed to fetch resultsJSON: HTTP ", status_code(resultsJSON))
  }
  
  return(results)
}


# RETURN RESULTS ----------------------------------------------------------

nfl_message_results <- function(bets = pending_bets) {
  
  string = paste0('', '# --------------------------------', '\n',
                  bets$GAME, ':           ', 'Bet $', bets$KC*100, '\n',
                  'DATE:                    ', as.Date(bets$`Start Time`), '\n',
                  'TEAM:                    ', bets$AB, '\n',
                  'BOOK:                    ', bets$Book, '\n',
                  'LINE:                       ', bets$Price, '\n',
                  'WINNER:                  ', bets[KC > 0, WINNER], '\n',
                  'PROFIT:                  ', bets[KC > 0, WINNER*Price*KC*100 - KC*100], '\n',
                  '# --------------------------------', '\n')
  return(string)
}

nfl_message_score <- function(bets = pending_bets_spread) {
  
  string = paste0('', '# --------------------------------', '\n',
                  bets$GAME, ':           ', 'Bet $', bets$PTS_PERCENT_ADJ*100, '\n',
                  'DATE:                    ', as.Date(bets$`Start Time`), '\n',
                  'TEAM:                    ', bets$AB, '\n',
                  'BOOK:                    ', bets$Book, '\n',
                  'LINE:                       ', bets$Price, '\n',
                  'SPREAD:                  ', bets$Points, '\n', 
                  'POINT DIFF:              ', bets$POINT_DIFF, '\n',           
                  'WINNER:                  ', bets[PTS_PERCENT_ADJ > 0, SPREAD_WINNER], '\n',
                  'PROFIT:                  ', bets[PTS_PERCENT_ADJ > 0, SPREAD_WINNER*Price*PTS_PERCENT_ADJ*100 - PTS_PERCENT_ADJ*100], '\n',
                  '# --------------------------------', '\n')
  return(string)
}


# SHINY APP FUNCTIONS -----------------------------------------------------


# FUNCTIONS FOR EV RESULTS SHINY DASHBOARD --------------------------------

get_line <- function(EV, winProb){
  line = ((EV+100)/winProb)
  return(line)
}

get_prob <- function(EV = 0, line){
  prob <- ((EV+100)/line)
  return(prob)
}

return_rate <- function(total_returned, total_wagered){
  return_rate = total_returned/total_wagered
  return(return_rate)
}

effective_win_rate <- function(total_returned, total_wagered, juice_adjusted = FALSE){
  return_rate = total_returned/total_wagered
  if(juice_adjusted){
    return_rate = return_rate/0.9545455
  }
  win_rate = return_rate*100/2
  return(win_rate)
}

plot_ev <- function(games_dt = dt_history, title = 'Odds vs Win Probability'){
  ylims = c(games_dt[,min(Price)]-0.25,
            games_dt[,max(Price)]+0.25)
  xlims = c(games_dt[,min(WIN_PROB)]-5,
            games_dt[,max(WIN_PROB)]+5)
  
  z <- data.table(x=c(0,105))
  p <- ggplot(data = z, mapping = aes(x=x)) +
    # stat_function(data = data.frame(x=c(0, 100)), mapping = aes(x),fun=function(x) get_line(EV = 0, winProb = x)) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x)) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x),
                  geom = 'ribbon',
                  mapping = aes(ymin=after_stat((get_line(EV = -5, winProb = x))),
                                ymax=after_stat((get_line(EV = 0, winProb = x)))),
                  fill = 'red',
                  alpha = 0.3) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x),
                  geom = 'ribbon',
                  mapping = aes(ymin=after_stat((get_line(EV = 0, winProb = x))),
                                ymax=after_stat((get_line(EV = 5, winProb = x)))),
                  fill = 'orange',
                  alpha = 0.2) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x),
                  geom = 'ribbon',
                  mapping = aes(ymin=after_stat((get_line(EV = 5, winProb = x))),
                                ymax=after_stat((get_line(EV = 10, winProb = x)))),
                  fill = 'yellow',
                  alpha = 0.2) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x),
                  geom = 'ribbon',
                  mapping = aes(ymin=after_stat((get_line(EV = 10, winProb = x))),
                                ymax=after_stat((get_line(EV = 10000, winProb = x)))),
                  fill = 'green',
                  alpha = 0.2) +
    stat_function(fun=function(x) get_line(EV = -5, winProb = x),
                  geom = 'area',
                  fill = 'red',
                  alpha = 0.4)
  
  p <- p + geom_point(data = games_dt,
                      mapping = aes(x = WIN_PROB,
                                    y = Price,
                                    color = WINNER
                                    # shape = Team
                      )) +
    labs(x = 'Win Probability', y = 'Line',
         title = title
    ) +
    coord_cartesian(xlim = xlims,
                    ylim = ylims) +
    scale_color_discrete(name = "WINNER") +
    scale_x_continuous(breaks = seq(0, 100, by = 10)) +
    scale_y_continuous(breaks = seq(floor(min(games_dt$Price)), ceiling(max(games_dt$Price)), by = 0.5)) +
    # scale_shape_discrete(name = "Team") +
    theme(plot.title = element_text(hjust = 0.5))
  
  return(ggplotly(p))
}



