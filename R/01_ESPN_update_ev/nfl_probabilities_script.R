
# NFL EXPECTED VALUE IDENTIFIER -------------------------------------------

# Generate a random delay between 0 and 300 seconds (5 minutes)
delay <- runif(1, min = 0, max = 300)
print(paste("Sleeping for", round(delay, 2), "seconds"))

# Delay the script by the random amount of time
Sys.sleep(delay)

rm(list = ls())
gc()

setwd("/Users/david/PersonalDevelopment/EVC")

args <- commandArgs(trailingOnly = TRUE)

# Check if at least one argument is provided
if (length(args) == 0) {
  cat("No arguments provided.\n")
  sport = 'NFL'
} else {
  cat("Here's the argument you passed: ", args[1], "\n")
  sport = args[1]
}

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
                 'NFL' = yaml::yaml.load_file("NFL/Dictionary/espnTeams.yml"))

source("scripts/functions.R")


# CREATE EVENT CLASS ------------------------------------------------------

week_events <- nfl_eventClass$new(sport)

if(week_events[["events"]][["count"]] == 0){ 
  stop('No events today')
}

# week_urls <- week_events$eventURLs()

week_probabilities <- nfl_gameClass$new(week_events)

week_stats <- week_probabilities$nfl_getStats()

week_stats[, `:=` (TEAM = ifelse(`Home/Away` == "Home",
                                 yes = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 2),
                                 no = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 1)),
                   HOME_TEAM = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 2),
                   AWAY_TEAM = str_extract(GAME, '([A-z]+) @ ([A-z]+)', group = 1),
                   GAME_URL = paste0("http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/", GAME_ID, "?lang=en&region=us"),
                   SCORE_URL = paste0("http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/", 
                                      GAME_ID, "/competitions/", GAME_ID, "/competitors/", TEAM_ID, "/score?lang=en&region=us")),
           by = .I]

# week_odds <- week_probabilities$nfl_getOdds()


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


# QUOTA INFORMATION -------------------------------------------------------

Quota = list()
Quota$used <- rawBettingData$headers$`x-requests-used`
Quota$remaining <- rawBettingData$headers$`x-requests-remaining`
cat("Quota Information -------------------------------", '\n')
cat("Queries used:           ", Quota$used, '\n') 
cat("Queries remaining:      ", Quota$remaining, '\n')
cat("Quota Information -------------------------------", '\n')


# PROCESS ODDS API DATA ---------------------------------------------------

jsonBettingData <- fromJSON(rawBettingDataContent)
jsonBettingData_un <- unnest_jsonData(jsonBettingData)
BettingData <- format_betting_data(jbd = jsonBettingData_un)

BettingData[, `:=` (MIN = min(Price),
                    MAX = max(Price),
                    MEAN = mean(Price),
                    MEDIAN = median(Price),
                    SD = sd(Price),
                    VAR = var(Price), 
                    Team = ifelse(Team == "Over", yes = `Home Team`, 
                                no = ifelse(Team == "Under", yes = `Away Team`, no = Team)
                    )), 
            by = .(id, Team, Market, Points)]

Dict <- fread(paste0(sport,"/Dictionary/",sport,"_teams.csv"),
              sep = ":",
              header = FALSE,
              col.names = c('AB', 'Team'))

BettingData <- merge.data.table(x = BettingData, y = Dict, by = "Team")

BettingData[, `:=` (`Start Time` = ymd_hms(`Start Time`, tz = "UTC"),
                    `Last Update` = ymd_hms(`Last Update`, tz = "UTC"),
                    bookmakers.last_update = ymd_hms(bookmakers.last_update, tz = "UTC"))]

BettingData[,`:=` (`Start Time` = with_tz(`Start Time`, tzone = "America/New_York"),
                   `Last Update` = with_tz(`Last Update`, tzone = "America/New_York"),
                   bookmakers.last_update = with_tz(bookmakers.last_update, tzone = "America/New_York"))]


# HEAD TO HEAD ------------------------------------------------------------

# Merge ESPN probability data with odds API line data
h2h_betting_data <- merge.data.table(x = BettingData[Market == "h2h",
                                                     .SD[which.max(Price)], 
                                                     by = .(id, Team)], 
                                     y = week_stats, by.x = c("Start Time", "AB"), 
                                     by.y = c("GAME_DATE", "TEAM"))

# Calculate expected value and kelly criterion statistics
h2h_betting_data[, `:=` (MIN_PRICE = round(get_line(EV = 0.001, winProb = `WIN PROB`), 2),
                         EV = round(calcExpectedValue(line = Price,winProb = `WIN PROB`),2),
                         KC = round((calc_kelly_crit(price = Price, winP = `WIN PROB`)), 2))]

setcolorder(h2h_betting_data, c("Start Time", "GAME", "AB", "WIN PROB", "Price",
                                "EV", "KC", "Book", "MIN", "MAX", "MEDIAN", "MEAN",
                                "SD", "VAR", "Team Pred. Point Difference", "Matchup Quality", 
                                "Team Chance Tie", "Opponent Seasson Strength Rating"))

my_string = paste0('', '# --------------------------------', '\n',
                   '#------------- ',"Bets" , ' -------------#', '\n',
                   timestamp(prefix = '#-- ', suffix = ' --#'), '\n',
                   'Sport:                           ', sport, '\n',
                   'Games analyzed:        ', h2h_betting_data[,.N]*0.5, '\n',
                   'Bets recommended:  ', h2h_betting_data[KC > 0, .N], '\n', 
                   "Queries used:             ", Quota$used, '\n',
                   "Queries remaining:    ", Quota$remaining, '\n',
                   '# --------------------------------', '\n')
for(n in 1:nrow(h2h_betting_data[KC > 0])){
  my_string = paste0(my_string, '', n, ' of ', nrow(h2h_betting_data[KC > 0]), '\n')
  my_string = paste0(my_string, nfl_message_bets(bets = h2h_betting_data[KC > 0][n]))
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

# Save Data
openxlsx2::write_xlsx(h2h_betting_data, file = paste0(sport, '/dailyFiles/', todaysDate, '_', todaysTime, '_', 'nfl_espnBets.xlsx'))

# Read old records
tape <- fread(paste0(sport, '/history/', 'h2h_data_record.csv'))

new_tape <- rbindlist(list(tape, h2h_betting_data), fill = TRUE)

fwrite(new_tape, paste0(sport, '/history/', 'h2h_data_record.csv'))

timestamp()
cat('\n')


# SPREADS -----------------------------------------------------------------

spread_betting_data <- merge.data.table(x = BettingData[Market == "spreads",
                                                        .SD[which.max(Price)], 
                                                        by = .(id, Team, Points)], 
                                        y = week_stats, by.x = c("Start Time", "AB"), 
                                        by.y = c("GAME_DATE", "TEAM"))

spread_betting_data[, `:=` (PREDICTED_SPREAD = `Team Pred. Point Difference` * -1)]
spread_betting_data[, `:=` (DELTA_PT = `Team Pred. Point Difference` + Points)]
spread_betting_data[, `:=` (DELTA_PT_2 = Points - PREDICTED_SPREAD)]

calc_value_nums <- function(point_spread = pts, predicted_spread = prds) {
  start <- ceiling(min(point_spread, predicted_spread)) # Get the lower bound
  start_gap <- round(start - min(point_spread, predicted_spread), 2)
  end <- floor(max(point_spread, predicted_spread)) # Get the upper bound
  end_gap <- round(max(point_spread, predicted_spread) - end, 2)
  return(list(start_gap = start_gap, points = seq(start, end), end_gap = end_gap)) # Create a sequence between these bounds
}

spread_betting_data[DELTA_PT > 0.5, 
                    `:=` (VALUE_PTS = lapply(1:.N, 
                                             function(i) calc_value_nums(point_spread = Points[i],
                                                                         predicted_spread = PREDICTED_SPREAD[i])))]

spread_betting_data[DELTA_PT > 0.5,
                    `:=` (PTS_PERCENT = as.numeric(lapply(1:.N,
                                                function(i) 
                                                  if(spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[1]] == 0 & spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[3]] == 0){
                                                    return(sum(value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING) - 
                                                             value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING[1] *.5 -
                                                             value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING[length(value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING)] *.5)
                                                  } else if(spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[1]] == 0){
                                                    sum(value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING) - 
                                                      value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING[1] *.5 - 
                                                      value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING[length(value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING)] * (0.5 * (1 - spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[3]]))
                                                  } else if(spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[3]] == 0){
                                                    sum(value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING) - 
                                                      value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING[1] * (0.5 * (1 - spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[1]])) -
                                                      value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING[length(value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING)] *.5
                                                  } else {
                                                    sum(value_of_points_dt[SPREAD %in% spread_betting_data[DELTA_PT > 0.5][i]$VALUE_PTS[[1]][[2]]]$CHANCE_OF_HITTING)
                                                  }
                    )))]


spread_betting_data[, PTS_PERCENT_ADJ := (Price - 1) * PTS_PERCENT]

setcolorder(spread_betting_data, c("Start Time", "GAME", "AB", "WIN PROB", "Price", "Points",
                                   "PREDICTED_SPREAD", "DELTA_PT", "VALUE_PTS", "PTS_PERCENT",
                                   "PTS_PERCENT_ADJ", "Team Pred. Point Difference", "DELTA_PT_2",
                                   # "EV", "KC", "Book", "MIN", "MAX", "MEDIAN", "MEAN",
                                   # "SD", "VAR","Matchup Quality", 
                                   "Team Chance Tie", "Opponent Seasson Strength Rating"))


spread_string = paste0('', '# --------------------------------', '\n',
                   '#---------- ',"Spread Bets" , ' ----------#', '\n',
                   timestamp(prefix = '#-- ', suffix = ' --#'), '\n',
                   'Sport:                           ', sport, '\n',
                   'Games analyzed:        ', spread_betting_data[,.N]*0.5, '\n',
                   'Bets recommended:  ', spread_betting_data[PTS_PERCENT_ADJ > 0, .N], '\n', 
                   "Queries used:             ", Quota$used, '\n',
                   "Queries remaining:    ", Quota$remaining, '\n',
                   '# --------------------------------', '\n')
for(n in 1:nrow(spread_betting_data[PTS_PERCENT_ADJ > 0])){
  spread_string = paste0(spread_string, '', n, ' of ', nrow(spread_betting_data[PTS_PERCENT_ADJ > 0]), '\n')
  spread_string = paste0(spread_string, spread_nfl_message_bets(bets = spread_betting_data[PTS_PERCENT_ADJ > 0][n]))
}

cat("Bet Information ---------------------------------", '\n')
cat(spread_string)
cat("Bet Information ---------------------------------", '\n')

# Construct the AppleScript command
appleScriptCommand <- sprintf('tell application "Messages"\n
                              set targetBuddy to "%s"\n
                              set targetService to id of 1st account whose service type = iMessage\n
                              set textMessage to "%s"\n
                              set theBuddy to participant targetBuddy of account id targetService\n
                              send textMessage to theBuddy\n
                              end tell', keys$CONTACT, spread_string)

cat("Sending message ---------------------------------", '\n')

# Execute the AppleScript command
system(paste("osascript -e", shQuote(appleScriptCommand)))
cat("Message sent ------------------------------------", '\n')

set_all_columns_to_character(spread_betting_data)

# Save Data
openxlsx2::write_xlsx(set_all_columns_to_character(spread_betting_data),
                      file = paste0(sport, '/dailyFiles/', todaysDate, '_', todaysTime, '_', 'nfl_spread_espnBets.xlsx'))

# Read old records
spread_tape <- fread(paste0(sport, '/history/', 'spread_data_record.csv'))

spread_new_tape <- rbindlist(list(spread_tape, spread_betting_data), fill = TRUE)

fwrite(set_all_columns_to_character(spread_new_tape), paste0(sport, '/history/', 'spread_data_record.csv'))

timestamp()
cat('\n')





# spread_betting_data[, `:=` (VALUE_PT = ifelse(DELTA_PT > 0,
#                                               yes = c(Points, -1*`Team Pred. Point Difference`), 
#                                               no = NA))]
# 
# spread_betting_data[3, seq(from = min(c(-1*`Team Pred. Point Difference`,Points)),
#                            to = max(c(-1*`Team Pred. Point Difference`,Points)),
#                            by = 0.5)]

# # Moneyline conversion table from Life at risk
# moneyline_conversion_dt[, `:=` (FAVORITE_DEC = american_to_decimal(FAVORITE),
#                                 UNDERDOG_DEC = american_to_decimal(UNDERDOG)),
#                         by = .I]
# 
# # Spline interpolation
# interp_favorite <- spline(x = moneyline_conversion_dt$SPREAD,
#                           y = moneyline_conversion_dt$FAVORITE_DEC, n = 100)
# 
# interp_favorite <- as.data.table(interp_favorite)[, `:=` (x = round(x*-1, 2),
#                                                           y = round(y, 2))]
# 
# interp_underdog <- spline(x = moneyline_conversion_dt$SPREAD,
#                           y = moneyline_conversion_dt$UNDERDOG_DEC, n = 100)
# 
# interp_underdog <- as.data.table(interp_underdog)[, `:=` (x = round(x, 2),
#                                                           y = round(y, 2))]
# 
# interp <- rbindlist(list(FAV = interp_favorite, UND = interp_underdog), idcol = "FAV")
# 
# setkey(spread_betting_data, Points)
# setkey(interp, x)
# 
# spread_betting_data <- interp[spread_betting_data, on = .(x = Points), roll = "nearest"]
# 
# # Create a plotly plot with spline interpolation
# p_spline <- plot_ly(x = ~interp_spline$x, y = ~interp_spline$y, type = 'scatter', mode = 'lines',
#                     name = 'Spline Interpolated Line') %>%
#   add_trace(x = ~moneyline_conversion_dt$SPREAD, y = ~moneyline_conversion_dt$FAVORITE_DEC, type = 'scatter', mode = 'markers',
#             name = 'Original Points')
# 
# # Show the plot
# p_spline
#  
# plot_ly(data = moneyline_conversion_dt, x = ~SPREAD, y = ~FAVORITE_DEC, 
#         type = "scatter", mode = "lines", name = "Favorite") |>
#   add_trace(y = ~UNDERDOG_DEC, type = "scatter", mode = "lines", name = "Underdog") |>
#   layout()
# 
# 
# setcolorder(spread_betting_data, c("Start Time", "GAME", "AB", "WIN PROB", "Price", "Points",
#                                    "Team Pred. Point Difference", "DELTA_PT", "PREDICTED_SPREAD",
#                                    # "EV", "KC", "Book", "MIN", "MAX", "MEDIAN", "MEAN",
#                                    # "SD", "VAR","Matchup Quality", 
#                                    "Team Chance Tie", "Opponent Seasson Strength Rating"))


# TOTALS ------------------------------------------------------------------

# total_betting_data <- merge.data.table(x = BettingData[Market == "totals",
#                                        .SD[which.max(Price)], 
#                                        by = .(id, Team, Points)],
#                                        y = week_stats, by.x = c("Start Time", "AB"),
#                                        by.y = c("GAME_DATE", "TEAM"))
# 
# setcolorder(total_betting_data, c("Start Time", "GAME", "AB", "WIN PROB", "Price", "Points",
#                                   "EV", "KC", "Book", "MIN", "MAX", "MEDIAN", "MEAN",
#                                   "SD", "VAR", "Team Pred. Point Difference", "Matchup Quality", 
#                                   "Team Chance Tie", "Opponent Seasson Strength Rating"))

