
# EXPECTED VALUE CALCULATOR & BETTOR - VERSION 2  -------------------------

# Generate a random delay between 0 and 300 seconds (5 minutes)
delay <- runif(1, min = 0, max = 300)
print(paste("Sleeping for", round(delay, 2), "seconds"))

# Delay the script by the random amount of time
Sys.sleep(delay)
print("over")

# RESULTS COMPILER --------------------------------------------------------

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

config <- switch(EXPR = sport, 
                 'NFL' = yaml::yaml.load_file("NFL/Dictionary/espnTeams.yml"))

keys <- yaml::yaml.load_file("/Users/david/odds_api.yml")

source("scripts/functions.R")

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

message(timestamp())
cat('\n')

# OPEN PENDING BETS -------------------------------------------------------

pending_h2h <- fread(paste0(sport, '/history/', 'h2h_data_record.csv'))
pending_spread <- fread(paste0(sport, '/history/', 'spread_data_record.csv'))

# GET RESULTS -------------------------------------------------------------

h2h_results_list <- lapply(unique(pending_h2h[is.na(WINNER) & !is.na(GAME_ID),GAME_URL]), retrieveResults)

h2h_score_list <- lapply(unique(pending_h2h[is.na(WINNER) & !is.na(GAME_ID),SCORE_URL]), retrieveScore)

h2h_pending_ids <- pending_h2h[is.na(WINNER) & !is.na(GAME_ID),GAME_ID]

h2h_fresults_list <- lapply(h2h_results_list, nfl_format_results)

h2h_fscore_list <- lapply(h2h_score_list, nfl_format_score)

h2h_fresults <- rbindlist(h2h_fresults_list, fill = TRUE)

h2h_fscore <- rbindlist(h2h_fscore_list, fill = TRUE)

results_column_names = c('GAME','GAME_DATE','HOME_TEAM','AWAY_TEAM','GAME_ID','TEAM_ID','HOME_AWAY','WINNER')  # Example column names
results_empty_dt <- data.table(matrix(ncol = length(results_column_names), nrow = 0))
colnames(results_empty_dt) <- results_column_names
set_all_columns_to_character(results_empty_dt)
results_empty_dt[,`:=` (GAME_DATE = data.table::as.IDate(GAME_DATE))]

h2h_fresults[, `:=` (TEAM_ID = as.integer(TEAM_ID), GAME_ID = as.integer(GAME_ID), GAME_DATE = GAME_DATE)]

fresults <- rbindlist(list(results_empty_dt, h2h_fresults), fill = TRUE)

score_column_names = c('GAME_ID','TEAM_ID','SCORE')  # Example column names
score_empty_dt <- data.table(matrix(ncol = length(score_column_names), nrow = 0))
colnames(score_empty_dt) <- score_column_names
set_all_columns_to_character(score_empty_dt)

h2h_fscore[, `:=` (TEAM_ID = as.integer(TEAM_ID),
                   GAME_ID = as.integer(GAME_ID),
                   SCORE = as.integer(SCORE))]

h2h_fscore[, `:=` (POINT_DIFF = SCORE - shift(SCORE, type = "lead")), by = GAME_ID]
h2h_fscore[, `:=` (POINT_DIFF = ifelse(is.na(POINT_DIFF),
                                       -shift(POINT_DIFF, type = "lag"), 
                                       POINT_DIFF)), by = GAME_ID]

fscore <- rbindlist(list(score_empty_dt, h2h_fscore), fill = TRUE)


# UPDATE DATA -------------------------------------------------------------

# h2h_results_dt <- fresults[pending_h2h, on = .(TEAM_ID = TEAM_ID, GAME_ID = GAME_ID)]
h2h_results_dt <- merge.data.table(x = fresults[, .(GAME_ID, TEAM_ID, WINNER)],
                                   y = pending_h2h, , by = c("GAME_ID", "TEAM_ID"), all.y = TRUE)

# h2h_score_dt <- fscore[pending_spread, on = .(TEAM_ID = TEAM_ID, GAME_ID = GAME_ID)]
spread_score_dt <- merge.data.table(x = fscore[, .(GAME_ID, TEAM_ID, SCORE, POINT_DIFF)], 
                                    y = pending_spread, , by = c("GAME_ID", "TEAM_ID"), all.y = TRUE)

# # i.cols <- na.omit(str_extract(names(results_dt), 'i\\..*'))
# # cols_i <- str_extract(i.cols, '(i)\\.([A-z]+)', group = 2)
# # paste0(cols_i, '=', 'fcoalesce(', cols_i, ',', i.cols, ')', collapse = ',')
# # paste0(i.cols, '=', 'NULL', collapse = ',')
# 
# results_dt[, `:=` (GAME=fcoalesce(GAME,i.GAME),
#                    HOME_TEAM=fcoalesce(HOME_TEAM,i.HOME_TEAM),
#                    AWAY_TEAM=fcoalesce(AWAY_TEAM,i.AWAY_TEAM),
#                    HOME_AWAY=fcoalesce(HOME_AWAY,i.HOME_AWAY),
#                    WINNER=fcoalesce(WINNER,i.WINNER),
#                    # GAME_DATE=fcoalesce(GAME_DATE,i.GAME_DATE),
#                    i.GAME=NULL,
#                    i.HOME_TEAM=NULL,
#                    i.AWAY_TEAM=NULL,
#                    i.HOME_AWAY=NULL,
#                    i.WINNER=NULL
#                    # i.GAME_DATE=NULL
# )]

h2h_results_dt[, `:=` (WINNER=fcoalesce(WINNER.x,WINNER.y),WINNER.x=NULL, WINNER.y=NULL)]
spread_score_dt[, `:=` (SCORE=fcoalesce(as.numeric(SCORE.x),as.numeric(SCORE.y)),SCORE.x=NULL, SCORE.y=NULL,
                        POINT_DIFF=fcoalesce(as.numeric(POINT_DIFF.x),as.numeric(POINT_DIFF.y)),POINT_DIFF.x=NULL, POINT_DIFF.y=NULL)]

spread_score_dt[, `:=` (SPREAD_WINNER = (POINT_DIFF + Points) > 0)]

h2h_pending_results <- h2h_results_dt[GAME_ID %in% h2h_pending_ids & !is.na(WINNER)]

spread_pending_score <- spread_score_dt[GAME_ID %in% h2h_pending_results$GAME_ID]

setorder(h2h_pending_results, `Start Time`, GAME, `Last Update`, `Home/Away`)

h2h_bets <- h2h_pending_results[KC > 0, .SD[which.max(KC)], by = .(GAME_ID, Team)]
h2h_num_bets <- h2h_bets[KC > 0, .N]
h2h_total_wagered <- h2h_bets[KC > 0, sum(KC*100)]
h2h_num_winners <- h2h_bets[KC > 0 & WINNER == TRUE, .N]
h2h_num_losers <- h2h_bets[KC > 0 & WINNER == FALSE, .N]
h2h_total_return <- h2h_bets[KC > 0 & WINNER == TRUE, sum((KC*Price*100))]
h2h_wagered_on_losers <- h2h_bets[KC > 0 & WINNER == FALSE, sum(KC*100)]
h2h_profit <- h2h_total_return - h2h_total_wagered
# h2h_bets <- h2h_pending_results[KC > 0]
h2h_winners <- h2h_bets[KC > 0 & WINNER == TRUE]
h2h_losers <- h2h_bets[KC > 0 & WINNER == FALSE]

setorder(spread_pending_score, `Start Time`, GAME, `Last Update`, `Home/Away`)

spread_bets <- spread_pending_score[PTS_PERCENT_ADJ > 0, .SD[which.max(PTS_PERCENT_ADJ)], by = .(GAME_ID, Team, Points)]
spread_num_bets <- spread_bets[PTS_PERCENT_ADJ > 0, .N]
spread_total_wagered <- spread_bets[PTS_PERCENT_ADJ > 0, sum(PTS_PERCENT_ADJ*100)]
spread_num_winners <- spread_bets[PTS_PERCENT_ADJ > 0 & SPREAD_WINNER == TRUE, .N]
spread_num_losers <- spread_bets[PTS_PERCENT_ADJ > 0 & SPREAD_WINNER == FALSE, .N]
spread_total_return <- spread_bets[PTS_PERCENT_ADJ > 0 & SPREAD_WINNER == TRUE, sum((PTS_PERCENT_ADJ*Price*100))]
spread_wagered_on_losers <- spread_bets[PTS_PERCENT_ADJ > 0 & SPREAD_WINNER == FALSE, sum(PTS_PERCENT_ADJ*100)]
spread_profit <- spread_total_return - spread_total_wagered
# spread_bets <- spread_pending_score[PTS_PERCENT_ADJ > 0]
spread_winners <- spread_bets[PTS_PERCENT_ADJ > 0 & SPREAD_WINNER == TRUE]
spread_losers <- spread_bets[PTS_PERCENT_ADJ > 0 & SPREAD_WINNER == FALSE]


# DEFINE R6 BANK ACCOUNT --------------------------------------------------

# BankAccount <- R6::R6Class(
#   "BankAccount",
#   private = list(
#     balance = 0,
#     transactions = list(),
#     
#     addTransaction = function(amount, type) {
#       transaction <- list(amount = amount, type = type, timestamp = Sys.time())
#       private$transactions <- c(private$transactions, list(transaction))
#     }
#   ),
#   public = list(
#     initialize = function(initial_balance = 0) {
#       private$balance <- initial_balance
#       private$transactions <- list()
#       private$addTransaction(initial_balance, "Initial Deposit")
#     },
#     deposit = function(amount) {
#       if(amount <= 0) {
#         stop("Deposit amount must be positive.")
#       }
#       private$balance <- private$balance + amount
#       private$addTransaction(amount, "Deposit")
#     },
#     
#     withdraw = function(amount) {
#       if(amount <= 0) {
#         stop("Withdrawal amount must be positive.")
#       }
#       if(amount > private$balance) {
#         stop("Insufficient funds.")
#       }
#       private$balance <- private$balance - amount
#       private$addTransaction(-amount, "Withdrawal")
#     },
#     
#     getBalance = function() {
#       private$balance
#     },
#     
#     getTransactions = function() {
#       private$transactions
#     }
#   )
# )

# -------------------------------------------------------------------------

# balance <- as.numeric(readLines(paste0(sport, '/history/', 'bank_account.txt')))
# bank_account <- BankAccount$new(balance)
# 
# tryCatch(switch(EXPR = as.character(profit > 0), 
#                 'TRUE' = bank_account$deposit(profit),
#                 'FALSE' = bank_account$withdraw(0 - profit)),
#          error = function(e) e)
# 
# fwrite(bank_account$getTransactions(), paste0(sport, '/history/', 'bank_transactions.txt'), append = TRUE)
# write_lines(bank_account$getBalance(), paste0(sport, '/history/', 'bank_account.txt'))

h2h_updated_bets <- paste0('', '# --------------------------------', '\n',
                       '#----------- ',"Results" , ' -----------#', '\n',
                       timestamp(prefix = '#-- ', suffix = ' --#'), '\n',
                       'Sport:                          ', sport, '\n',
                       'Bets recommended:  ', h2h_num_bets, '\n', 
                       "Winners:                ", h2h_num_winners, '/', h2h_num_bets, '\n',
                       "Profit:                 ", h2h_profit, '\n',
                       # 'Balance:               ', bank_account$getBalance(), '\n',
                       '# --------------------------------', '\n')

spread_updated_bets <- paste0('', '# --------------------------------', '\n',
                       '#----------- ',"Results" , ' -----------#', '\n',
                       timestamp(prefix = '#-- ', suffix = ' --#'), '\n',
                       'Sport:                          ', sport, '\n',
                       'Bets recommended:  ', spread_num_bets, '\n', 
                       "Winners:                ", spread_num_winners, '/', spread_num_bets, '\n',
                       "Profit:                 ", spread_profit, '\n',
                       # 'Balance:               ', bank_account$getBalance(), '\n',
                       '# --------------------------------', '\n')

# 
# nfl_message_results <- function(bets = pending_bets) {
#   
#   string = paste0('', '# --------------------------------', '\n',
#                   bets$GAME, ':           ', 'Bet $', bets$KC*100, '\n',
#                   'DATE:                    ', bets$GAME_DATE, '\n',
#                   'TEAM:                    ', bets$TEAM, '\n',
#                   'BOOK:                    ', bets$Book, '\n',
#                   'LINE:                       ', bets$Price, '\n',
#                   'WINNER:                  ', bets[KC > 0, WINNER], '\n',
#                   'PROFIT:                  ', bets[KC > 0, WINNER*Price*KC*100 - KC*100], '\n',
#                   '# --------------------------------', '\n')
#   return(string)
# }

for(n in 1:nrow(h2h_bets)){
  h2h_updated_bets = paste0(h2h_updated_bets, '', n, ' of ', nrow(h2h_bets), '\n')
  h2h_updated_bets = paste0(h2h_updated_bets, nfl_message_results(bets = h2h_bets[n]))
}

for(n in 1:nrow(spread_bets)){
  spread_updated_bets = paste0(spread_updated_bets, '', n, ' of ', nrow(spread_bets), '\n')
  spread_updated_bets = paste0(spread_updated_bets, nfl_message_score(bets = spread_bets[n]))
}

cat('\n',h2h_updated_bets)
cat('\n',spread_updated_bets)

h2h_appleScriptCommand <- sprintf('tell application "Messages"\n
                              set targetBuddy to "%s"\n
                              set targetService to id of 1st account whose service type = iMessage\n
                              set textMessage to "%s"\n
                              set theBuddy to participant targetBuddy of account id targetService\n
                              send textMessage to theBuddy\n
                              end tell', keys$CONTACT, h2h_updated_bets)

cat("Sending message ---------------------------------", '\n')

# Execute the AppleScript command
system(paste("osascript -e", shQuote(h2h_appleScriptCommand)))
cat("Message sent ------------------------------------", '\n')

spread_appleScriptCommand <- sprintf('tell application "Messages"\n
                              set targetBuddy to "%s"\n
                              set targetService to id of 1st account whose service type = iMessage\n
                              set textMessage to "%s"\n
                              set theBuddy to participant targetBuddy of account id targetService\n
                              send textMessage to theBuddy\n
                              end tell', keys$CONTACT, spread_updated_bets)

cat("Sending message ---------------------------------", '\n')

# Execute the AppleScript command
system(paste("osascript -e", shQuote(spread_appleScriptCommand)))
cat("Message sent ------------------------------------", '\n')

fwrite(h2h_results_dt[!is.na(GAME_ID)], paste0(sport, '/history/', 'h2h_data_record.csv'))

fwrite(spread_score_dt[!is.na(GAME_ID)], paste0(sport, '/history/', 'spread_data_record.csv'))


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

