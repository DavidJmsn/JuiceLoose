# NHL EV RETRIEVAL MASTER SCRIPT ------------------------------------------
# Purpose: Orchestrate efficient collection of NHL data and EV calculation
# Author: Professional implementation with data.table optimization
# Last Updated: 2025-12-19

suppressPackageStartupMessages({
  library(data.table)
})

# Get script directory
if (interactive()) {
  script_dir <- getwd()
} else {
  script_dir <- dirname(sys.frame(1)$ofile)
}

# Source all modules
source(file.path(script_dir, "utils", "common.R"))
source(file.path(script_dir, "utils", "api_client.R"))
source(file.path(script_dir, "utils", "selenium_manager.R"))
source(file.path(script_dir, "collectors", "schedule.R"))
source(file.path(script_dir, "collectors", "probabilities.R"))
source(file.path(script_dir, "collectors", "odds.R"))
source(file.path(script_dir, "collectors", "goalies.R"))

# MAIN FUNCTIONS -----------------------------------------------------------

#' Master function to retrieve NHL expected values
#' @param dates Date or vector of dates to process (NULL = today & tomorrow)
#' @param save_file Whether to save output to CSV (default: TRUE)
#' @param include_broadcasts Include TV broadcast info (default: FALSE)
#' @param include_standings Include team standings (default: FALSE)
#' @return data.table with expected values (invisibly)
retrieve_nhl_expected_values <- function(dates = NULL, 
                                         save_file = TRUE,
                                         include_standings = FALSE) {
  
  # Initialize
  init_logging("ev_retrieval")
  check_delay_arg()
  ensure_directories()
  
  # Parse dates
  dates <- parse_date_args(dates)
  
  log_message(sprintf("=== NHL EV Retrieval for %d dates ===", length(dates)), "INFO")
  
  # Step 1: Collect schedule data
  log_message("Step 1/4: Collecting schedule data", "INFO")
  schedule_dt <- with_retry(
    collect_nhl_schedule,
    dates = dates,
    include_standings = include_standings,
    context = "Schedule collection"
  )
  
  if (is.null(schedule_dt) || nrow(schedule_dt) == 0) {
    log_message("No games found for specified dates", "ERROR")
    return(invisible(data.table()))
  }
  
  # Step 2: Collect win probabilities
  log_message("Step 2/4: Collecting win probabilities", "INFO")
  probabilities_dt <- with_retry(
    collect_win_probabilities,
    dates = dates,
    context = "Win probability collection"
  )
  
  if (is.null(probabilities_dt) || nrow(probabilities_dt) == 0) {
    log_message("No win probabilities found - cannot calculate EV", "ERROR")
    return(invisible(data.table()))
  }
  
  # Step 3: Collect odds
  log_message("Step 3/4: Collecting betting odds", "INFO")
  odds_dt <- with_retry(
    collect_nhl_odds,
    dates = dates,
    markets = "h2h",
    context = "Odds collection"
  )
  
  if (is.null(odds_dt) || nrow(odds_dt) == 0) {
    log_message("No odds found - cannot calculate EV", "ERROR")
    return(invisible(data.table()))
  }
  
  # Step 4: Collect starting goalies (non-vital)
  log_message("Step 4/4: Collecting starting goalies", "INFO")
  goalies_dt <- with_retry(
    collect_starting_goalies,
    dates = dates,
    context = "Goalie collection"
  )
  
  if (is.null(goalies_dt) || nrow(goalies_dt) == 0) {
    log_message("No goalie data retrieved - continuing without", "WARN")
    # Create minimal goalie data
    goalies_dt <- data.table(
      team = character(),
      goalie = character(),
      date = as.Date(character())
    )
  }
  
# MERGE DATA -------------------------------------------------------------
  log_message("Merging all data sources", "INFO")
  
  # Prepare schedule base
  schedule_base <- schedule_dt[, .(
    game_id, game_date_est, 
    game_time_est = as.POSIXct(paste(game_date_est, start_time_est), format = "%Y-%m-%d %H:%M"),
    local_timezone,
    home, away, home_team, away_team,
    venue, game_state,
    home_score, away_score,
    broadcast
  )]
  
  # Create team-level rows with FIXED score mapping
  team_schedule <- rbind(
    # Home team rows
    schedule_base[, .(
      game_id, game_date_est, game_time_est,
      team = home, opponent = away,
      home_or_away = "home",
      home, away, home_team, away_team,
      venue, game_state,
      # FIXED: Proper score mapping for home teams
      team_score = ifelse(game_state %in% c("FINAL", "OFF") & !is.na(home_score), 
                          home_score, NA_integer_),
      opponent_score = ifelse(game_state %in% c("FINAL", "OFF") & !is.na(away_score), 
                              away_score, NA_integer_),
      broadcast
    )],
    # Away team rows  
    schedule_base[, .(
      game_id, game_date_est, game_time_est,
      team = away, opponent = home,
      home_or_away = "away",
      home, away, home_team, away_team,
      venue, game_state,
      # FIXED: Proper score mapping for away teams
      team_score = ifelse(game_state %in% c("FINAL", "OFF") & !is.na(away_score), 
                          away_score, NA_integer_),
      opponent_score = ifelse(game_state %in% c("FINAL", "OFF") & !is.na(home_score), 
                              home_score, NA_integer_),
      broadcast
    )]
  )
  
  # Merge probabilities
  prob_merge <- probabilities_dt[, .(
    date, game_time = as.POSIXct(paste(date, game_time), format = "%Y-%m-%d %H:%M"), 
    team, win_probability
  )]
  
  merged_dt <- merge(
    team_schedule,
    prob_merge,
    by.x = c("game_date_est", "game_time_est", "team"),
    by.y = c("date", "game_time", "team"),
    all.x = TRUE
  )
  
  # Merge odds
  odds_merge <- odds_dt[market == "h2h", .(
    game_date, 
    game_time = as.POSIXct(paste(game_date, game_time), format = "%Y-%m-%d %H:%M"),
    home_team, away_team, 
    name, price, book,
    price_mean, price_min, price_sd, n_books,
    retrieved_time
  )]
  
  setkey(merged_dt, game_date_est, game_time_est, home, away, team)
  setkey(odds_merge, game_date, game_time, home_team, away_team, name)
  merged_dt <- odds_merge[merged_dt,
    on = .(
      game_date = game_date_est,
      home_team = home, 
      away_team = away,
      name = team,
      game_time = game_time_est
    ),
    roll = "nearest"
  ]
  
  # Rename columns properly
  # FIXED: Rename columns using actual column names from odds_merge
  setnames(merged_dt, old = c("game_date", "game_time", "home_team", "away_team", "name"),
           new = c("game_date_est", "game_time_est", "home", "away", "team"))
  
  # Merge goalies
  if (nrow(goalies_dt) > 0) {
    goalie_merge <- goalies_dt[, .(
      date, game_time = as.POSIXct(paste(date, game_time), format = "%Y-%m-%d %H:%M"), 
      team, goalie, goalie_status = status
    )]
    
    merged_dt <- merge(
      merged_dt,
      goalie_merge,
      by.x = c("game_date_est", "game_time_est", "team"),
      by.y = c("date", "game_time", "team"),
      all.x = TRUE
    )
  } else {
    merged_dt[, `:=`(goalie = NA_character_, goalie_status = NA_character_)]
  }
  
  # CALCULATE EV AND KELLY -------------------------------------------------
  log_message("Calculating expected values and Kelly Criterion", "INFO")
  
  merged_dt[, `:=`(
    expected_value = calculate_ev(price, win_probability),
    kelly_criterion = calculate_kelly(price, win_probability),
    implied_win_prob = ifelse(price > 0, 1/price, NA_real_)
  )]
  
  # Add metadata - FIXED: Remove redundant win_percent column
  merged_dt[, retrieval_time := format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ", tz = "America/New_York")]
  
  # Select final columns - FIXED: Remove win_percent from final selection
  final_cols <- c(
    "game_id", "team", "home", "away", "game_date_est", "game_time_est",
    "venue", "broadcast", "game_state", "home_or_away",
    "goalie", "goalie_status",
    "book", "price", "win_probability",  # REMOVED: win_percent
    "expected_value", "kelly_criterion", "implied_win_prob",
    "price_mean", "price_sd", "n_books",
    "team_score", "opponent_score",
    "retrieval_time"
  )
  
  # Keep only existing columns
  existing_cols <- intersect(final_cols, names(merged_dt))
  final_dt <- merged_dt[, ..existing_cols]
  final_dt[, game_time_est := format(game_time_est, "%Y-%m-%d %H:%M:%S")]
  
  # Sort
  setorder(final_dt, game_date_est, game_time_est, home, team)
  
  # SAVE OUTPUT ------------------------------------------------------------
  if (save_file) {
    output_file <- file.path(
      nhl_config$data_dir, "expected_value",
      sprintf("%s_h2h_expected_values.csv", format(Sys.time(), "%Y%m%d_%H%M%S"))
    )
    
    fwrite(final_dt, output_file)
    log_message(sprintf("Saved %d rows to %s", nrow(final_dt), output_file), "SUCCESS")
    
    # Generate summary
    generate_ev_summary(final_dt)
  }
  
  log_message("NHL EV retrieval completed successfully", "SUCCESS")
  
  return(invisible(final_dt))
}

#' Generate EV summary report
#' @param dt data.table with EV calculations
generate_ev_summary <- function(dt) {
  # Filter for positive EV
  positive_ev <- dt[expected_value > 0]
  
  if (nrow(positive_ev) == 0) {
    log_message("No positive EV bets found", "INFO")
    return()
  }
  
  # Summary statistics
  summary_stats <- positive_ev[, .(
    n_bets = .N,
    avg_ev = mean(expected_value),
    max_ev = max(expected_value),
    avg_kelly = mean(kelly_criterion, na.rm = TRUE),
    n_games = uniqueN(game_id)
  )]
  
  log_message("=== POSITIVE EV SUMMARY ===", "INFO")
  log_message(sprintf("Found %d positive EV bets across %d games", 
                      summary_stats$n_bets, summary_stats$n_games), "INFO")
  log_message(sprintf("Average EV: %.2f%% | Max EV: %.2f%%", 
                      summary_stats$avg_ev * 100, summary_stats$max_ev * 100), "INFO")
  
  # Top 5 EV bets
  top_bets <- positive_ev[order(-expected_value)][1:min(5, .N), .(
    team, price, win_probability, expected_value, kelly_criterion, book
  )]
  
  if (nrow(top_bets) > 0) {
    log_message("Top EV Bets:", "INFO")
    for (i in 1:nrow(top_bets)) {
      bet <- top_bets[i]
      log_message(sprintf("  %s: %.2f @ %.1f%% = %.1f%% EV (Kelly: %.1f%%)",
                          bet$team, bet$price, bet$win_probability * 100,
                          bet$expected_value * 100, bet$kelly_criterion * 100), "INFO")
    }
  }
}

#' Retrieve NHL scores only
#' @param dates Date or vector of dates to process
#' @param save_file Whether to save output to CSV
#' @return data.table with scores (invisibly)
retrieve_nhl_scores <- function(dates = NULL, save_file = TRUE) {
  # Initialize
  init_logging("scores")
  check_delay_arg()
  ensure_directories()
  
  # Parse dates
  dates <- parse_date_args(dates)
  
  log_message(sprintf("=== NHL Scores Retrieval for %d dates ===", length(dates)), "INFO")
  
  # Collect schedule data (includes scores)
  schedule_dt <- with_retry(
    collect_nhl_schedule,
    dates = dates,
    # include_broadcasts = FALSE,
    include_standings = FALSE,
    context = "Schedule collection"
  )
  
  if (is.null(schedule_dt) || nrow(schedule_dt) == 0) {
    log_message("No games found for specified dates", "WARN")
    return(invisible(data.table()))
  }
  
  # Filter to completed games
  completed_dt <- schedule_dt[game_state %in% c("FINAL", "OFF") & !is.na(home_score)]
  completed_dt[, start_time_est := format(as.POSIXct(paste(game_date_est, start_time_est), format = "%Y-%m-%d %H:%M"), "%Y-%m-%d %H:%M:%S")]
  
  if (nrow(completed_dt) == 0) {
    log_message("No completed games found", "INFO")
    return(invisible(data.table()))
  }
  
  log_message(sprintf("Found %d completed games", nrow(completed_dt)), "SUCCESS")
  
  # Save if requested
  if (save_file) {
    output_file <- file.path(
      nhl_config$data_dir, "scores",
      sprintf("%s_scores.csv", format(Sys.time(), "%Y%m%d_%H%M%S"))
    )
    
    fwrite(completed_dt, output_file)
    log_message(sprintf("Saved scores to %s", output_file), "SUCCESS")
  }
  
  return(invisible(completed_dt))
}

# RUN IF CALLED DIRECTLY ---------------------------------------------------

if (!interactive()) {
  # Get function name from command line or default to EV
  args <- commandArgs(trailingOnly = TRUE)
  
  if ("scores" %in% args) {
    retrieve_nhl_scores()
  } else {
    retrieve_nhl_expected_values()
  }
}
