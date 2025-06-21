# ESPN EV RETRIEVAL MASTER SCRIPT -----------------------------------------
# Purpose: Orchestrate efficient collection of ESPN data and EV calculation
# Author: Professional implementation with data.table optimization
# Last Updated: 2025-06-21

suppressPackageStartupMessages({
  library(data.table)
  library(here)
})

# Source all modules using here()
source(here("R", "espn_ev_retrieval", "utils", "common.R"))
source(here("R", "espn_ev_retrieval", "utils", "api_client.R"))
source(here("R", "espn_ev_retrieval", "collectors", "schedule.R"))
source(here("R", "espn_ev_retrieval", "collectors", "odds.R"))

# MAIN FUNCTIONS -----------------------------------------------------------

#' Master function to retrieve ESPN expected values
#' @param dates Date or vector of dates to process (NULL = today & tomorrow)
#' @param sport Character - "NBA", "NFL", or "MLB"
#' @param save_file Whether to save output to CSV (default: TRUE)
#' @return data.table with expected values (invisibly)
retrieve_espn_expected_values <- function(dates = NULL, 
                                          sport = "NBA",
                                          save_file = TRUE) {
  
  # Initialize
  init_logging(sprintf("%s_ev_retrieval", tolower(sport)))
  check_delay_arg()
  ensure_directories()
  
  # Validate sport
  sport <- toupper(sport)
  if (!sport %in% c("NBA", "NFL", "MLB")) {
    log_message(sprintf("Invalid sport: %s. Must be NBA, NFL, or MLB", sport), "ERROR")
    return(invisible(data.table()))
  }
  
  # Parse dates
  dates <- parse_date_args(dates)
  
  log_message(sprintf("=== %s EV Retrieval for %d dates ===", sport, length(dates)), "INFO")
  
  # Step 1: Collect schedule data (includes win probabilities)
  log_message("Step 1/3: Collecting schedule and win probabilities", "INFO")
  schedule_dt <- with_retry(
    collect_espn_schedule,
    dates = dates,
    sport = sport,
    context = "Schedule collection"
  )
  
  if (is.null(schedule_dt) || nrow(schedule_dt) == 0) {
    log_message("No games found for specified dates", "ERROR")
    return(invisible(data.table()))
  }
  
  # Step 2: Collect odds
  log_message("Step 2/3: Collecting betting odds", "INFO")
  odds_dt <- with_retry(
    collect_espn_odds,
    dates = dates,
    sport = sport,
    markets = "h2h",
    context = "Odds collection"
  )
  
  if (is.null(odds_dt) || nrow(odds_dt) == 0) {
    log_message("No odds found - cannot calculate EV", "ERROR")
    return(invisible(data.table()))
  }
  
  # Step 3: Merge and calculate EV
  log_message("Step 3/3: Merging data and calculating EV", "INFO")
  
  # Prepare schedule base
  schedule_base <- schedule_dt[, .(
    game_id, game_date_est = game_date, 
    game_time_est = as.POSIXct(paste(game_date, game_time), format = "%Y-%m-%d %H:%M"),
    home = home_team, away = away_team,
    venue, game_state, broadcast,
    home_score, away_score,
    home_starter, away_starter
  )]
  
  # Create team-level rows
  team_schedule <- rbind(
    # Home team rows
    schedule_base[, .(
      game_id, game_date_est, game_time_est,
      team = home, opponent = away,
      home_or_away = "home",
      home, away, venue, game_state, broadcast,
      team_score = ifelse(game_state %in% c("FINAL", "STATUS_FINAL") & !is.na(home_score), 
                          home_score, NA_integer_),
      opponent_score = ifelse(game_state %in% c("FINAL", "STATUS_FINAL") & !is.na(away_score), 
                              away_score, NA_integer_),
      player = home_starter
    )],
    # Away team rows  
    schedule_base[, .(
      game_id, game_date_est, game_time_est,
      team = away, opponent = home,
      home_or_away = "away",
      home, away, venue, game_state, broadcast,
      team_score = ifelse(game_state %in% c("FINAL", "STATUS_FINAL") & !is.na(away_score), 
                          away_score, NA_integer_),
      opponent_score = ifelse(game_state %in% c("FINAL", "STATUS_FINAL") & !is.na(home_score), 
                              home_score, NA_integer_),
      player = away_starter
    )]
  )
  
  # Extract win probabilities from schedule
  prob_dt <- schedule_dt[, .(
    game_date = game_date,
    game_time = as.POSIXct(paste(game_date, game_time), format = "%Y-%m-%d %H:%M"),
    home_team, away_team, home_win_prob, away_win_prob
  )]
  
  # Create team-level probabilities
  prob_team <- rbind(
    prob_dt[!is.na(home_win_prob), .(
      game_date, game_time,
      team = home_team,
      win_probability = home_win_prob
    )],
    prob_dt[!is.na(away_win_prob), .(
      game_date, game_time,
      team = away_team,
      win_probability = away_win_prob
    )]
  )
  
  # Merge schedule with probabilities
  merged_dt <- merge(
    team_schedule,
    prob_team,
    by.x = c("game_date_est", "game_time_est", "team"),
    by.y = c("game_date", "game_time", "team"),
    all.x = TRUE
  )
  
  # Merge with odds
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
  
  final_dt <- odds_merge[merged_dt,
    on = .(
      game_date = game_date_est,
      home_team = home,
      away_team = away,
      name = team,
      game_time = game_time_est
    ),
    roll = -2 * 3600  # 2 hour window for odds
  ]
  
  # Rename columns
  setnames(final_dt, 
           old = c("game_date", "game_time", "home_team", "away_team", "name"),
           new = c("game_date_est", "game_time_est", "home", "away", "team"))
  
  # Calculate EV and Kelly
  final_dt[, `:=`(
    expected_value = calculate_ev(price, win_probability),
    kelly_criterion = calculate_kelly(price, win_probability),
    implied_win_prob = ifelse(!is.na(price), 1/price, NA_real_),
    retrieval_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "America/New_York")
  )]
  
  # Add sport-specific player column name
  player_col <- switch(sport,
    NFL = "QB",
    MLB = "pitcher",
    NBA = "star_player"
  )
  
  setnames(final_dt, "player", player_col)
  
  # Add player status (if available from schedule)
  final_dt[, paste0(player_col, "_status") := "confirmed"]
  
  # Remove rows with no odds
  final_dt <- final_dt[!is.na(price)]
  
  if (nrow(final_dt) == 0) {
    log_message("No valid EV data after merging", "ERROR")
    return(invisible(data.table()))
  }
  
  # Order columns to match NHL schema
  col_order <- c(
    "game_id", "team", "home", "away", "game_date_est", "game_time_est",
    "venue", "broadcast", "game_state", "home_or_away",
    player_col, paste0(player_col, "_status"),
    "book", "price", "win_probability", "expected_value", "kelly_criterion",
    "implied_win_prob", "price_mean", "price_sd", "n_books",
    "team_score", "opponent_score", "retrieval_time"
  )
  
  setcolorder(final_dt, intersect(col_order, names(final_dt)))
  
  # Log summary
  log_message(sprintf("Retrieved EV data for %d bets across %d games", 
                      nrow(final_dt),
                      uniqueN(final_dt[, paste(game_date_est, home, away)])), "SUCCESS")
  
  # Log top EV bets
  log_top_ev_bets(final_dt)
  
  # Save if requested
  if (save_file) {
    output_file <- file.path(
      espn_config$data_dir, "expected_value",
      sprintf("%s_%s_expected_values.csv", 
              format(Sys.time(), format = "%Y%m%d_%H%M%S"),
              tolower(sport))
    )
    
    fwrite(final_dt, output_file)
    log_message(sprintf("Saved %d rows to %s", nrow(final_dt), output_file), "SUCCESS")
  }
  
  return(invisible(final_dt))
}

#' Log top EV bets
#' @param dt data.table with EV data
log_top_ev_bets <- function(dt) {
  # Get top 10 EV bets
  top_bets <- dt[expected_value > 0][order(-expected_value)][1:min(10, .N), .(
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

#' Retrieve ESPN scores only
#' @param dates Date or vector of dates to process
#' @param sport Character - "NBA", "NFL", or "MLB"
#' @param save_file Whether to save output to CSV
#' @return data.table with scores (invisibly)
retrieve_espn_scores <- function(dates = NULL, sport = "NBA", save_file = TRUE) {
  # Initialize
  init_logging(sprintf("%s_scores", tolower(sport)))
  check_delay_arg()
  ensure_directories()

  # Custom default dates: yesterday only
  if (is.null(dates)) {
    args <- commandArgs(trailingOnly = TRUE)
    args <- setdiff(args, c("scores", "delay"))

    if (length(args) > 0) {
      dates <- args
    } else {
      dates <- Sys.Date() - 1
      log_message("No dates specified, using yesterday's date", "INFO")
    }
  }

  dates <- parse_date_args(dates)
  
  log_message(sprintf("=== %s Scores Retrieval for %d dates ===", sport, length(dates)), "INFO")
  
  # Collect schedule data (includes scores)
  schedule_dt <- with_retry(
    collect_espn_schedule,
    dates = dates,
    sport = sport,
    context = "Schedule collection"
  )
  
  if (is.null(schedule_dt) || nrow(schedule_dt) == 0) {
    log_message("No games found for specified dates", "WARN")
    return(invisible(data.table()))
  }
  
  # Filter to completed games
  completed_dt <- schedule_dt[game_state %in% c("FINAL", "STATUS_FINAL") & !is.na(home_score)]
  
  if (nrow(completed_dt) == 0) {
    log_message("No completed games found", "INFO")
    return(invisible(data.table()))
  }
  
  log_message(sprintf("Found %d completed games", nrow(completed_dt)), "SUCCESS")
  
  # Save if requested
  if (save_file) {
    output_file <- file.path(
      espn_config$data_dir, "scores",
      sprintf("%s_%s_scores.csv", 
              format(Sys.time(), format = "%Y%m%d_%H%M%S"),
              tolower(sport))
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
  
  # Extract sport argument
  sport <- "NBA"  # Default
  for (arg in args) {
    if (arg %in% c("NBA", "NFL", "MLB")) {
      sport <- arg
      break
    }
  }
  
  if ("scores" %in% args) {
    retrieve_espn_scores(sport = sport)
  } else {
    retrieve_espn_expected_values(sport = sport)
  }
}
