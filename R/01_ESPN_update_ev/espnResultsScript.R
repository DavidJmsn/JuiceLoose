
# EXPECTED VALUE CALCULATOR & BETTOR - VERSION 2  -------------------------

# RESULTS COMPILER --------------------------------------------------------

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

config <- switch(EXPR = sport, 
                 'NBA' = yaml::yaml.load_file("NBA/Dictionary/espnTeams.yml"),
                 'NFL' = yaml::yaml.load_file("NFL/Dictionary/espnTeams.yml"),
                 'NHL' = yaml::yaml.load_file("NHL/Dictionary/espnTeams.yml"),
                 'ERE' = yaml::yaml.load_file("ERE/Dictionary/espnTeams.yml"),
                 'MLB' = yaml::yaml.load_file("MLB/Dictionary/espnTeams.yml"),
                 'EPL' = yaml::yaml.load_file("EPL/Dictionary/espnTeams.yml"))

keys <- yaml::yaml.load_file("/Users/david/odds_api.yml")

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

retrieveResults <- function(eventURL){
  
  resultsJSON = GET(eventURL)
  if (status_code(resultsJSON) == 200) {
    
    results = fromJSON(content(resultsJSON, "text"))
  } else {
    stop("Failed to fetch resultsJSON: HTTP ", status_code(resultsJSON))
  }
  
  return(results)
}

format_results <- function(results){
  game = results[["shortName"]]
  home_team = str_extract(game, '([A-z]+) @ ([A-z]+)', group = 2)
  away_team = str_extract(game, '([A-z]+) @ ([A-z]+)', group = 1)
  game_id = results[["id"]]
  teams = results[["competitions"]][["competitors"]][[1]][["id"]]
  home_away = results[["competitions"]][["competitors"]][[1]][["homeAway"]]
  winner = results[["competitions"]][["competitors"]][[1]][["winner"]]
  game_date = lubridate::ymd_hm( results[["date"]])
  # game_date = data.table::as.IDate(game_date, tz = 'UTC')
  game_date = with_tz(game_date, tzone = 'America/New_York')
  game_date = as.IDate(game_date)
  
  fresults = data.table(GAME = game,
                        GAME_DATE = game_date,
                        HOME_TEAM = home_team,
                        AWAY_TEAM = away_team,
                        GAME_ID = game_id,
                        TEAM_ID = teams,
                        HOME_AWAY = home_away,
                        WINNER = winner)
  
  return(fresults)
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

message(timestamp())
cat('\n')

# OPEN PENDING BETS -------------------------------------------------------

pending <- fread(paste0(sport, '/history/', 'history.csv'))

# GET RESULTS -------------------------------------------------------------

results_list <- lapply(unique(pending[is.na(WINNER) & !is.na(GAME_ID),GAME_URL]), retrieveResults)

pending_ids <- pending[is.na(WINNER) & !is.na(GAME_ID),GAME_ID]

fresults_list <- lapply(results_list, format_results)

fresults <- rbindlist(fresults_list, fill = TRUE)

column_names = c('GAME','GAME_DATE','HOME_TEAM','AWAY_TEAM','GAME_ID','TEAM_ID','HOME_AWAY','WINNER')  # Example column names
empty_dt <- data.table(matrix(ncol = length(column_names), nrow = 0))
colnames(empty_dt) <- column_names
set_all_columns_to_character(empty_dt)
empty_dt[,`:=` (GAME_DATE = data.table::as.IDate(GAME_DATE))]

fresults[, `:=` (TEAM_ID = as.integer(TEAM_ID), GAME_ID = as.integer(GAME_ID), GAME_DATE = GAME_DATE)]

fresults <- rbindlist(list(empty_dt, fresults), fill = TRUE)

# UPDATE DATA -------------------------------------------------------------

results_dt <- fresults[pending, on = .(TEAM_ID = TEAM_ID, GAME_ID = GAME_ID, GAME_DATE)]

# i.cols <- na.omit(str_extract(names(results_dt), 'i\\..*'))
# cols_i <- str_extract(i.cols, '(i)\\.([A-z]+)', group = 2)
# paste0(cols_i, '=', 'fcoalesce(', cols_i, ',', i.cols, ')', collapse = ',')
# paste0(i.cols, '=', 'NULL', collapse = ',')

results_dt[, `:=` (GAME=fcoalesce(GAME,i.GAME),
                   HOME_TEAM=fcoalesce(HOME_TEAM,i.HOME_TEAM),
                   AWAY_TEAM=fcoalesce(AWAY_TEAM,i.AWAY_TEAM),
                   HOME_AWAY=fcoalesce(HOME_AWAY,i.HOME_AWAY),
                   WINNER=fcoalesce(WINNER,i.WINNER),
                   # GAME_DATE=fcoalesce(GAME_DATE,i.GAME_DATE),
                   i.GAME=NULL,
                   i.HOME_TEAM=NULL,
                   i.AWAY_TEAM=NULL,
                   i.HOME_AWAY=NULL,
                   i.WINNER=NULL
                   # i.GAME_DATE=NULL
                   )]

pending_results <- results_dt[GAME_ID %in% pending_ids & !is.na(WINNER)]

setorder(pending_results, `Start Time`, GAME_ID, `Last Update`, HOME_AWAY)

num_bets <- pending_results[KC > 0, .N]
total_wagered <- pending_results[KC > 0, sum(KC*100)]
num_winners <- pending_results[KC > 0 & WINNER == TRUE, .N]
num_losers <- pending_results[KC > 0 & WINNER == FALSE, .N]
total_return <- pending_results[KC > 0 & WINNER == TRUE, sum((KC*Price*100))]
wagered_on_losers <- pending_results[KC > 0 & WINNER == FALSE, sum(KC*100)]
profit <- total_return - total_wagered
bets <- pending_results[KC > 0]
winners <- pending_results[KC > 0 & WINNER == TRUE]
losers <- pending_results[KC > 0 & WINNER == FALSE]

# DEFINE R6 BANK ACCOUNT --------------------------------------------------

BankAccount <- R6::R6Class(
  "BankAccount",
  private = list(
    balance = 0,
    transactions = list(),
    
    addTransaction = function(amount, type) {
      transaction <- list(amount = amount, type = type, timestamp = Sys.time())
      private$transactions <- c(private$transactions, list(transaction))
    }
  ),
  public = list(
    initialize = function(initial_balance = 0) {
      private$balance <- initial_balance
      private$transactions <- list()
      private$addTransaction(initial_balance, "Initial Deposit")
    },
    deposit = function(amount) {
      if(amount <= 0) {
        stop("Deposit amount must be positive.")
      }
      private$balance <- private$balance + amount
      private$addTransaction(amount, "Deposit")
    },
    
    withdraw = function(amount) {
      if(amount <= 0) {
        stop("Withdrawal amount must be positive.")
      }
      if(amount > private$balance) {
        stop("Insufficient funds.")
      }
      private$balance <- private$balance - amount
      private$addTransaction(-amount, "Withdrawal")
    },
    
    getBalance = function() {
      private$balance
    },
    
    getTransactions = function() {
      private$transactions
    }
  )
)

# -------------------------------------------------------------------------

balance <- as.numeric(readLines(paste0(sport, '/history/', 'bank_account.txt')))
bank_account <- BankAccount$new(balance)

tryCatch(switch(EXPR = as.character(profit > 0), 
                'TRUE' = bank_account$deposit(profit),
                'FALSE' = bank_account$withdraw(0 - profit)),
         error = function(e) e)

fwrite(bank_account$getTransactions(), paste0(sport, '/history/', 'bank_transactions.txt'), append = TRUE)
write_lines(bank_account$getBalance(), paste0(sport, '/history/', 'bank_account.txt'))

updated_bets <- paste0('', '# --------------------------------', '\n',
                       '#----------- ',"Results" , ' -----------#', '\n',
                       timestamp(prefix = '#-- ', suffix = ' --#'), '\n',
                       'Sport:                          ', sport, '\n',
                       'Bets recommended:  ', num_bets, '\n', 
                       "Winners:                ", num_winners, '/', num_bets, '\n',
                       "Profit:                 ", profit, '\n',
                       'Balance:               ', bank_account$getBalance(), '\n',
                       '# --------------------------------', '\n')


message_results <- function(bets = pending_bets) {
  
  string = paste0('', '# --------------------------------', '\n',
                  bets$GAME, ':           ', 'Bet $', bets$KC*100, '\n',
                  'DATE:                    ', bets$GAME_DATE, '\n',
                  'TEAM:                    ', bets$TEAM, '\n',
                  'BOOK:                    ', bets$Book, '\n',
                  'LINE:                       ', bets$Price, '\n',
                  'WINNER:                  ', bets[KC > 0, WINNER], '\n',
                  'PROFIT:                  ', bets[KC > 0, WINNER*Price*KC*100 - KC*100], '\n',
                  '# --------------------------------', '\n')
  return(string)
}

for(n in 1:nrow(bets)){
  updated_bets = paste0(updated_bets, '', n, ' of ', nrow(bets), '\n')
  updated_bets = paste0(updated_bets, message_results(bets = bets[n]))
}

cat('\n',updated_bets)

appleScriptCommand <- sprintf('tell application "Messages"\n
                              set targetBuddy to "%s"\n
                              set targetService to id of 1st account whose service type = iMessage\n
                              set textMessage to "%s"\n
                              set theBuddy to participant targetBuddy of account id targetService\n
                              send textMessage to theBuddy\n
                              end tell', keys$CONTACT, updated_bets)

cat("Sending message ---------------------------------", '\n')

# Execute the AppleScript command
system(paste("osascript -e", shQuote(appleScriptCommand)))
cat("Message sent ------------------------------------", '\n')

fwrite(results_dt[!is.na(GAME_ID)], paste0(sport, '/history/', 'history.csv'))


# # Reset transactions
# daily_profits <- results_dt[KC > 0, sum(WINNER*Price*KC*100 - KC*100), by = day(with_tz(`Start Time`, tzone = "America/New_York"))]
# 
# for(i in daily_profits[!is.na(V1), .N]:1){
#   balance <- as.numeric(readLines(paste0(sport, '/history/', 'bank_account.txt')))
#   bank_account <- BankAccount$new(balance)
#   switch(EXPR = as.character(daily_profits[!is.na(V1)][i, V1] > 0),
#          'TRUE' = bank_account$deposit(daily_profits[!is.na(V1)][i, V1]),
#          'FALSE' = bank_account$withdraw(0 - daily_profits[!is.na(V1)][i, V1]))
#   fwrite(bank_account$getTransactions(), paste0(sport, '/history/', 'bank_transactions.txt'), append = TRUE)
#   write_lines(bank_account$getBalance(), paste0(sport, '/history/', 'bank_account.txt'))
# }

