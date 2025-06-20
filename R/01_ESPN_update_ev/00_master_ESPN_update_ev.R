# RETRIEVE ESPN SPORTS ODDS + WIN PROB + CALCULATE EXPECTED VALUES ------------
# Purpose: Master script to retrieve NBA/NFL/MLB schedule, odds, win probabilities,
#          and calculate expected values
# Author: David Jamieson (adapted from NHL version)
# E-mail: david.jmsn@icloud.com
# Last Updated: 2025-06-04

# SETUP ------------------------------------------------------------------------

# Clean environment
rm(list = ls())

# Suppress startup messages
suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(stringr)
  library(lubridate)
  library(readr)
  library(tidyr)
})

# CONFIGURATION ----------------------------------------------------------------

master_config <- list(
  # Script paths
  espn_script         = file.path(Sys.getenv("HOME"), "R/01_ESPN_update_ev/01_ESPN_schedule_retrieval.R"),
  odds_script         = file.path(Sys.getenv("HOME"), "R/01_ESPN_update_ev/02_ESPN_odds_retrieval.R"),
  
  # Output configuration
  output_base_dir     = file.path(Sys.getenv("HOME"), "data"),
  log_base_dir        = file.path(Sys.getenv("HOME"), "logs"),
  
  # Script behavior
  save_intermediate   = TRUE,  # Save intermediate reports
  email_reports       = FALSE, # Email reports when complete
  email_address       = "user@email.com",
  
  # Retry configuration
  max_retries         = 3,
  retry_delay         = 5,     # seconds
  
  # Supported sports
  supported_sports    = c("NBA", "NFL", "MLB")
)

# LOGGING FUNCTIONS ------------------------------------------------------------

#' Initialize log file for this run
#' @param sport Sport name for log file
#' @return Path to log file
init_log <- function(sport) {
  log_dir <- file.path(master_config$log_base_dir, sport)
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  
  log_file <- file.path(log_dir, 
                        sprintf("%s_master_%s.log", 
                                tolower(sport),
                                format(Sys.time(), format = "%Y%m%d_%H%M%S")))
  
  # Write header
  cat(sprintf("%s Master Retrieval Script\n", sport), file = log_file)
  cat("Started:", format(Sys.time(), format = "%Y-%m-%d %H:%M:%S"), "\n", file = log_file, append = TRUE)
  cat(rep("=", 60), "\n\n", file = log_file, append = TRUE)
  
  return(log_file)
}

# Global log file path (will be set when sport is determined)
LOG_FILE <- NULL

#' Log message to file and console
#' @param message Message to log
#' @param level Log level (INFO, WARN, ERROR, SUCCESS)
log_message <- function(message, level = "INFO") {
  timestamp <- format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
  log_entry <- sprintf("[%s] %s: %s", timestamp, level, message)
  
  # Console output with color coding
  if (level == "ERROR") {
    message(log_entry)
  } else if (level == "SUCCESS") {
    cat("\033[32m", log_entry, "\033[0m\n", sep = "")
  } else if (level == "WARN") {
    cat("\033[33m", log_entry, "\033[0m\n", sep = "")
  } else {
    cat(log_entry, "\n")
  }
  
  # File output
  if (!is.null(LOG_FILE)) {
    cat(log_entry, "\n", file = LOG_FILE, append = TRUE)
  }
}

# UTILITY FUNCTIONS ------------------------------------------------------------

#' Create sport-specific output directory
#' @param sport Sport name
#' @return Path to output directory
create_sport_output_dir <- function(sport) {
  output_dir <- file.path(master_config$output_base_dir, sport, "expected_value")
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  return(output_dir)
}

#' Parse command line arguments for dates
#' @return Vector of dates or NULL
parse_command_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  
  if (length(args) == 0) {
    return(NULL)
  }
  
  # Try to parse as dates
  dates <- tryCatch({
    as.Date(args)
  }, error = function(e) {
    log_message(sprintf("Invalid date arguments: %s", paste(args, collapse = ", ")), "ERROR")
    stop("Please provide dates in YYYY-MM-DD format")
  })
  
  # Validate dates
  dates <- dates[!is.na(dates)]
  
  if (length(dates) == 0) {
    return(NULL)
  }
  
  log_message(sprintf("Parsed %d date(s) from command line: %s", 
                      length(dates), 
                      paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")))
  
  return(dates)
}

#' Get dates for processing
#' @param manual_dates Manually specified dates (overrides command line)
#' @return Vector of dates
get_processing_dates <- function(manual_dates = NULL) {
  # Priority: manual_dates > command_args > default (today & tomorrow)
  
  if (!is.null(manual_dates)) {
    dates <- as.Date(manual_dates)
    log_message(sprintf("Using manually specified dates: %s", 
                        paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")))
  } else {
    # Check command line arguments
    dates <- parse_command_args()
    
    if (is.null(dates)) {
      # Default to today and tomorrow
      dates <- c(Sys.Date(), Sys.Date() + 1)
      log_message("No dates specified, using default: today and tomorrow")
    }
  }
  
  return(sort(unique(dates)))
}

# EXPECTED VALUE FUNCTIONS -----------------------------------------------------

#' Calculate expected value from odds line and win probability
#' @param line Decimal odds line
#' @param winProb Win probability (0-1)
#' @return Expected value
calcExpectedValue <- function(line, winProb) {
  (line * winProb) - 1
}

#' Get win probability from line and expected value
#' @param EV Expected value
#' @param line Decimal odds line
#' @return Win probability
get_prob <- function(EV, line) {
  (EV + 1) / line
}

#' Get line from expected value and win probability
#' @param EV Expected value
#' @param winProb Win probability
#' @return Decimal odds line
get_line <- function(EV, winProb) {
  (EV + 1) / winProb
}

#' Convert decimal odds to fractional odds
#' @param decimal_odds Decimal odds
#' @return Fractional odds
decimal_to_fraction <- function(decimal_odds) {
  decimal_odds - 1
}

#' Calculate Kelly Criterion for bet sizing
#' @param price Decimal odds
#' @param winP Win probability
#' @return Kelly Criterion value
calc_kelly_crit <- function(price, winP) {
  b <- decimal_to_fraction(price)
  p <- winP
  q <- 1 - p
  p - (q / b)
}

# DATA RETRIEVAL FUNCTIONS -----------------------------------------------------

#' Source a script with retry logic in isolated environment
#' @param script_path Path to script
#' @param script_name Name for logging
#' @param vital Whether script failure should stop execution
#' @return TRUE if successful, FALSE otherwise
source_with_retry <- function(script_path, script_name, vital = TRUE) {
  # Create a new environment for sourcing to prevent variable conflicts
  script_env <- new.env(parent = globalenv())
  
  for (attempt in 1:master_config$max_retries) {
    log_message(sprintf("Attempting to source %s (attempt %d/%d)", 
                        script_name, attempt, master_config$max_retries))
    
    result <- tryCatch({
      source(script_path, local = script_env)
      
      # After successful sourcing, attach the functions we need to global env
      if (script_name == "ESPN schedule") {
        retrieve_espn_schedule <<- script_env$retrieve_espn_schedule
      } else if (script_name == "odds") {
        retrieve_odds <<- script_env$retrieve_odds
      }
      
      TRUE
    }, error = function(e) {
      log_message(sprintf("Failed to source %s: %s", script_name, e$message), "ERROR")
      FALSE
    })
    
    if (result) {
      log_message(sprintf("Successfully sourced %s", script_name), "SUCCESS")
      return(TRUE)
    }
    
    if (attempt < master_config$max_retries) {
      log_message(sprintf("Retrying in %d seconds...", master_config$retry_delay), "WARN")
      Sys.sleep(master_config$retry_delay)
    }
  }
  
  if (vital) {
    stop(sprintf("Failed to source vital script: %s", script_name))
  } else {
    log_message(sprintf("Non-vital script %s failed, continuing...", script_name), "WARN")
    return(FALSE)
  }
}

#' Generate and save intermediate report
#' @param data Data frame to report
#' @param report_type Type of report
#' @param dates Dates being processed
#' @param sport Sport name
save_intermediate_report <- function(data, report_type, dates, sport) {
  if (!master_config$save_intermediate || is.null(data) || nrow(data) == 0) {
    return()
  }
  
  log_dir <- file.path(master_config$log_base_dir, sport)
  report_file <- file.path(log_dir, 
                           sprintf("%s_%s_%s.txt", 
                                   tolower(sport),
                                   report_type,
                                   format(Sys.Date(), format = "%Y%m%d")))
  
  sink(report_file)
  cat(sport, toupper(report_type), "Report -", format(Sys.Date(), format = "%B %d, %Y"), "\n")
  cat(rep("=", 60), "\n")
  cat("Dates processed:", paste(format(dates, format = "%Y-%m-%d"), collapse = ", "), "\n\n")
  
  # Print summary statistics
  cat("Summary:\n")
  cat("Total rows:", nrow(data), "\n")
  
  if ("date" %in% names(data)) {
    cat("\nRows by date:\n")
    print(table(data$date))
  }
  
  cat("\nSample data (first 10 rows):\n")
  print(head(data, 10))
  
  sink()
  
  log_message(sprintf("Saved %s report to %s", report_type, report_file))
}

# MAIN EXECUTION ---------------------------------------------------------------

#' Main function to orchestrate all data retrieval and calculations
#' @param manual_dates Optional manual date specification
#' @param sport Sport to process (NBA, NFL, or MLB)
#' @return Combined data frame with expected values
retrieve_espn_expected_values <- function(manual_dates = NULL, sport = "NBA") {
  
  # Validate sport
  sport <- toupper(sport)
  if (!sport %in% master_config$supported_sports) {
    stop(sprintf("Invalid sport: %s. Must be one of: %s", 
                 sport, paste(master_config$supported_sports, collapse = ", ")))
  }
  
  # Initialize log file for this sport
  LOG_FILE <<- init_log(sport)
  
  # Create output directory
  output_dir <- create_sport_output_dir(sport)
  
  # Get dates to process
  dates <- get_processing_dates(manual_dates)
  
  log_message(sprintf("Starting %s data retrieval for %d date(s): %s",
                      sport,
                      length(dates),
                      paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")))
  
  # Step 1: Retrieve ESPN schedule with win probabilities (VITAL)
  log_message("Step 1/2: Retrieving ESPN schedule and win probabilities", "INFO")
  
  if (!source_with_retry(master_config$espn_script, "ESPN schedule", vital = TRUE)) {
    stop("Cannot proceed without ESPN schedule data")
  }
  
  games <- tryCatch({
    retrieve_espn_schedule(dates = dates, sport = sport)
  }, error = function(e) {
    log_message(sprintf("Failed to retrieve ESPN schedule: %s", e$message), "ERROR")
    NULL
  })
  
  if (is.null(games) || nrow(games) == 0) {
    log_message("No games found for specified dates", "WARN")
    return(invisible(NULL))
  }
  
  log_message(sprintf("Retrieved %d games", nrow(games)), "SUCCESS")
  save_intermediate_report(games, "schedule", dates, sport)
  
  # Check if we have win probabilities
  games_with_probs <- sum(!is.na(games$home_win_prob) | !is.na(games$away_win_prob))
  if (games_with_probs == 0) {
    log_message("No win probabilities found in ESPN data - cannot calculate expected values", "ERROR")
    stop("No win probabilities available")
  }
  
  log_message(sprintf("Found win probabilities for %d out of %d games", 
                      games_with_probs, nrow(games)), "SUCCESS")
  
  # Step 2: Retrieve odds (VITAL)
  log_message("Step 2/2: Retrieving betting odds", "INFO")
  
  if (!source_with_retry(master_config$odds_script, "odds", vital = TRUE)) {
    stop("Cannot proceed without odds data")
  }
  
  betting_data <- tryCatch({
    retrieve_odds(dates = dates, sport = sport, markets = c("h2h", "spreads", "totals"))
  }, error = function(e) {
    log_message(sprintf("Failed to retrieve odds: %s", e$message), "ERROR")
    NULL
  })
  
  if (is.null(betting_data) || nrow(betting_data) == 0) {
    stop("No odds found - cannot calculate expected values")
  }
  
  log_message(sprintf("Retrieved %d odds entries", nrow(betting_data)), "SUCCESS")
  save_intermediate_report(betting_data, "odds", dates, sport)
  
  # DATA PROCESSING ------------------------------------------------------------
  
  log_message("Processing and merging data...", "INFO")
  
  # Clean team names function - handle different naming conventions
  clean_team_name <- function(name) {
    # Remove common abbreviations and standardize
    name <- toupper(name)
    name <- gsub("\\.", "", name)
    name <- trimws(name)
    
    # Sport-specific name mappings if needed
    # Add any team name standardizations here
    
    return(name)
  }
  
  # Process ESPN data to create team-level records for win probabilities
  win_prob_data <- bind_rows(
    # Home team records
    games %>%
      filter(!is.na(home_win_prob)) %>%
      mutate(
        team = ifelse(sport == "MLB", gsub("ATHLETICS ATHLETICS", "OAKLAND ATHLETICS", clean_team_name(home_team_full)), clean_team_name(home_team_full)),
        opponent = ifelse(sport == "MLB", gsub("ATHLETICS ATHLETICS", "OAKLAND ATHLETICS", clean_team_name(away_team_full)),clean_team_name(away_team_full)),
        win_probability = home_win_prob / 100,  # Convert percentage to decimal
        is_home = TRUE
      ) %>%
      select(game_id, date = game_date, team, opponent, win_probability, is_home),
    
    # Away team records
    games %>%
      filter(!is.na(away_win_prob)) %>%
      mutate(
        team = ifelse(sport == "MLB", gsub("ATHLETICS ATHLETICS", "OAKLAND ATHLETICS", clean_team_name(away_team_full)),clean_team_name(away_team_full)),
        opponent = ifelse(sport == "MLB", gsub("ATHLETICS ATHLETICS", "OAKLAND ATHLETICS", clean_team_name(home_team_full)),clean_team_name(home_team_full)),
        win_probability = away_win_prob / 100,  # Convert percentage to decimal
        is_home = FALSE
      ) %>%
      select(game_id, date = game_date, team, opponent, win_probability, is_home)
  )
  
  # Clean games data for merging
  games_clean <- games %>%
    mutate(
      home_team = clean_team_name(home_team),
      away_team = clean_team_name(away_team),
      home_team_full = ifelse(sport == "MLB", gsub("ATHLETICS ATHLETICS", "OAKLAND ATHLETICS", clean_team_name(home_team_full)),clean_team_name(home_team_full)),       
      away_team_full = ifelse(sport == "MLB", gsub("ATHLETICS ATHLETICS", "OAKLAND ATHLETICS", clean_team_name(away_team_full)),clean_team_name(away_team_full)),
      game_datetime = as.numeric(as.POSIXct(start_time_utc, tz = "UTC"))
    )
  
  # Process betting data - focus on h2h (moneyline) for expected value
  h2h_odds <- betting_data %>%
    filter(market == "h2h") %>%
    mutate(
      home_team_full = clean_team_name(home_team),
      away_team_full = clean_team_name(away_team),
      name = clean_team_name(name),
      game_date = as.Date(commence_time),
      game_datetime = as.numeric(as.POSIXct(commence_time, tz = "UTC"))
    ) %>%
    select(-home_team, -away_team) %>%
    group_by(home_team_full, away_team_full, name) %>%
    slice_max(price, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  # Create matching key for games
  games_key <- games_clean %>%
    select(game_id, home_team, away_team, home_team_full, away_team_full, game_date, game_datetime, start_time_local, venue) %>%
    distinct()
  
  if(sport == "MLB"){
    h2h_odds <- as.data.table(h2h_odds)
    games_key <- as.data.table(games_key)
    setkey(h2h_odds,  home_team_full, away_team_full, game_date, game_datetime)
    setkey(games_key, home_team_full, away_team_full, game_date, game_datetime)
    joined <- data.frame(
      games_key[h2h_odds,
                on = .(
                  home_team_full,
                  away_team_full,
                  game_date,
                  game_datetime
                ),
                roll = "nearest"]
    )
    final_data <- joined %>%
      left_join(
        win_prob_data,
        by = c("name" = "team", "game_date" = "date", "game_id"),
        suffix = c("_odds", "_wp")
      ) %>%
      filter(!is.na(win_probability))  # Only keep rows with win probabilities
  } else {
    # Perform a rolling join similar to the NHL master script
    # Restrict matches to start times within two hours of the scheduled game
    h2h_odds_dt <- as.data.table(h2h_odds)
    games_key_dt <- as.data.table(games_key)

    setkey(h2h_odds_dt,  home_team_full, away_team_full, game_date, game_datetime)
    setkey(games_key_dt, home_team_full, away_team_full, game_date, game_datetime)

    joined <- data.frame(
      games_key_dt[h2h_odds_dt,
                   on = .(
                     home_team_full,
                     away_team_full,
                     game_date,
                     game_datetime
                   ),
                   roll = "nearest"]
    )

    joined <- joined %>%
      filter(abs(game_datetime - i.game_datetime) <= 2 * 3600)

    final_data <- joined %>%
      left_join(
        win_prob_data,
        by = c("name" = "team", "game_date" = "date", "game_id"),
        suffix = c("_odds", "_wp")
      ) %>%
      filter(!is.na(win_probability))  # Only keep rows with win probabilities
  }
  
  # CALCULATE EXPECTED VALUES --------------------------------------------------
  
  log_message("Calculating expected values and Kelly Criterion...", "INFO")
  
  final_data <- final_data %>%
    mutate(
      # Calculate betting metrics
      expected_value = calcExpectedValue(price, win_probability),
      kelly_criterion = calc_kelly_crit(price, win_probability),
      implied_line = get_line(0, win_probability),
      implied_win_percent = get_prob(0, price),
      
      # Add metadata
      sport = sport,
      current_time = Sys.time(),
      retrieval_date = Sys.Date()
    ) %>%
    # Reorder columns for readability
    select(
      # Game info
      sport, team = name, home_team, away_team, is_home, game_date, 
      start_time = start_time_local, venue,
      
      # Betting info
      book, price, win_probability, expected_value, kelly_criterion,
      implied_line, implied_win_percent,
      
      # Metadata
      retrieval_date, current_time,
      
      # Keep all other columns
      everything()
    ) %>%
    arrange(game_date, start_time, home_team, team)
  
  # Set timezone
  attr(final_data$current_time, "tzone") <- "America/New_York"
  
  # SAVE OUTPUT ----------------------------------------------------------------
  
  timestamp <- format(Sys.time(), format = "%Y%m%d_%H%M%S")
  output_file <- file.path(output_dir, 
                           sprintf("%s_%s_h2h_expected_values.csv", 
                                   timestamp, tolower(sport)))
  
  final_data$file_name <- basename(output_file)
  
  write_csv(final_data, output_file)
  log_message(sprintf("Saved expected values to %s", output_file), "SUCCESS")
  
  # Generate summary report
  generate_final_report(final_data, output_file, sport)
  
  # Email if configured
  if (master_config$email_reports) {
    email_report(output_file, sport)
  }
  
  log_message(sprintf("%s data retrieval completed successfully", sport), "SUCCESS")
  
  return(invisible(final_data))
}

#' Generate final summary report
#' @param data Final data frame
#' @param output_file Path to output file
#' @param sport Sport name
generate_final_report <- function(data, output_file, sport) {
  log_dir <- file.path(master_config$log_base_dir, sport)
  report_file <- file.path(log_dir,
                           sprintf("%s_ev_summary_%s.txt",
                                   tolower(sport),
                                   format(Sys.Date(), format = "%Y%m%d")))
  
  sink(report_file)
  cat(sprintf("%s EXPECTED VALUE SUMMARY REPORT\n", sport))
  cat("Generated:", format(Sys.time(), format = "%Y-%m-%d %H:%M:%S"), "\n")
  cat(rep("=", 60), "\n\n")
  
  # Summary statistics
  cat("SUMMARY STATISTICS\n")
  cat("-----------------\n")
  cat("Total games:", length(unique(paste(data$home_team, data$away_team))), "\n")
  cat("Total betting opportunities:", nrow(data), "\n")
  cat("Date range:", min(data$game_date), "to", max(data$game_date), "\n\n")
  
  # Positive EV bets
  positive_ev <- data %>% filter(expected_value > 0)
  if (nrow(positive_ev) > 0) {
    cat("POSITIVE EXPECTED VALUE BETS\n")
    cat("---------------------------\n")
    cat("Count:", nrow(positive_ev), "\n")
    cat("Average EV:", sprintf("%.2f%%", mean(positive_ev$expected_value) * 100), "\n")
    cat("Max EV:", sprintf("%.2f%%", max(positive_ev$expected_value) * 100), "\n\n")
    
    cat("Top 10 EV Bets:\n")
    top_ev <- positive_ev %>%
      arrange(desc(expected_value)) %>%
      head(10) %>%
      select(team, price, win_probability, expected_value, kelly_criterion, book)
    
    print(as.data.frame(top_ev), row.names = FALSE)
  } else {
    cat("No positive expected value bets found.\n")
  }
  
  cat("\n\nOutput saved to:", output_file, "\n")
  
  sink()
  
  log_message(sprintf("Generated summary report: %s", report_file))
}

#' Email report (placeholder function)
#' @param output_file Path to output file
#' @param sport Sport name
email_report <- function(output_file, sport) {
  # This is a placeholder - implement based on your email setup
  log_message("Email functionality not implemented", "WARN")
  
  # Example implementation:
  # subject <- sprintf("%s Expected Values Report", sport)
  # cmd <- sprintf("mail -s '%s' %s < %s",
  #                subject, master_config$email_address, output_file)
  # system(cmd)
}

# CONVENIENCE WRAPPER FUNCTIONS ------------------------------------------------

#' Retrieve NBA expected values
#' @param dates Dates to process (NULL for today and tomorrow)
#' @return Data frame with expected values (invisibly)
retrieve_nba_expected_values <- function(dates = NULL) {
  retrieve_espn_expected_values(manual_dates = dates, sport = "NBA")
}

#' Retrieve NFL expected values
#' @param dates Dates to process (NULL for today and tomorrow)
#' @return Data frame with expected values (invisibly)
retrieve_nfl_expected_values <- function(dates = NULL) {
  retrieve_espn_expected_values(manual_dates = dates, sport = "NFL")
}

#' Retrieve MLB expected values
#' @param dates Dates to process (NULL for today and tomorrow)
#' @return Data frame with expected values (invisibly)
retrieve_mlb_expected_values <- function(dates = NULL) {
  retrieve_espn_expected_values(manual_dates = dates, sport = "MLB")
}

# RUN SCRIPT -------------------------------------------------------------------

# Only run if not being sourced
if (!interactive() && identical(environment(), globalenv())) {
  # Check for sport argument
  args <- commandArgs(trailingOnly = TRUE)
  
  # First non-date argument should be sport
  sport <- "NBA"  # Default
  dates <- NULL
  
  if (length(args) > 0) {
    # Try to parse each argument as a date
    for (arg in args) {
      if (is.na(as.Date(arg, optional = TRUE))) {
        # Not a date, assume it's the sport
        if (toupper(arg) %in% master_config$supported_sports) {
          sport <- toupper(arg)
        }
      } else {
        # It's a date
        if (is.null(dates)) {
          dates <- arg
        } else {
          dates <- c(dates, arg)
        }
      }
    }
  }
  
  tryCatch({
    # Run the main function
    retrieve_espn_expected_values(manual_dates = dates, sport = sport)
  }, error = function(e) {
    if (!is.null(LOG_FILE)) {
      log_message(sprintf("Fatal error: %s", e$message), "ERROR")
    } else {
      message(sprintf("Fatal error: %s", e$message))
    }
    quit(status = 1)
  })
}