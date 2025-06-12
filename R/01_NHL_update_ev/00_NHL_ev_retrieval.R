# RETRIEVE ODDS + WIN PROB + STARTING GOALIES ---------------------------------
# Purpose: Master script to retrieve NHL schedule, odds, win probabilities, 
#          and starting goalies, then calculate expected values
# Author: David Jamieson (optimized version)
# E-mail: david.jmsn@icloud.com
# Last Updated: 2025-05-31

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
  schedule_script     = file.path(Sys.getenv("HOME"), "R/01_NHL_update_ev/01_NHL_retrieve_schedule.R"),
  win_prob_script     = file.path(Sys.getenv("HOME"), "R/01_NHL_update_ev/02_NHL_win_probability_retrieval.R"),
  odds_script         = file.path(Sys.getenv("HOME"), "R/01_NHL_update_ev/03_NHL_odds_retrieval.R"),
  goalies_script      = file.path(Sys.getenv("HOME"), "R/01_NHL_update_ev/04_NHL_starting_goalie_retrieval.R"),
  
  # Output configuration
  output_dir          = file.path(Sys.getenv("HOME"), "data/NHL/expected_value"),
  log_dir             = file.path(Sys.getenv("HOME"), "logs"),
  
  # Script behavior
  save_intermediate   = TRUE,  # Save intermediate reports
  email_reports       = FALSE, # Email reports when complete
  email_address       = "user@email.com",
  
  # Retry configuration
  max_retries         = 3,
  retry_delay         = 5      # seconds
)

# Create directories if they don't exist
dir.create(master_config$output_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(master_config$log_dir, showWarnings = FALSE, recursive = TRUE)

# LOGGING FUNCTIONS ------------------------------------------------------------

#' Initialize log file for this run
#' @return Path to log file
init_log <- function() {
  log_file <- file.path(master_config$log_dir, 
                        sprintf("nhl_master_%s.log", 
                                format(Sys.time(), format = "%Y%m%d_%H%M%S")))
  
  # Write header
  cat("NHL Master Retrieval Script\n", file = log_file)
  cat("Started:", format(Sys.time(), format = "%Y-%m-%d %H:%M:%S"), "\n", file = log_file, append = TRUE)
  cat(rep("=", 60), "\n\n", file = log_file, append = TRUE)
  
  return(log_file)
}

# Global log file path
LOG_FILE <- init_log()

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
  cat(log_entry, "\n", file = LOG_FILE, append = TRUE)
}

# UTILITY FUNCTIONS ------------------------------------------------------------

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
      # This makes the retrieve_* functions available while protecting our variables
      if (script_name == "schedule") {
        retrieve_nhl_schedule <<- script_env$retrieve_nhl_schedule
      } else if (script_name == "win probabilities") {
        retrieve_win_probabilities <<- script_env$retrieve_win_probabilities
      } else if (script_name == "odds") {
        retrieve_nhl_odds <<- script_env$retrieve_nhl_odds
      } else if (script_name == "starting goalies") {
        retrieve_starting_goalies <<- script_env$retrieve_starting_goalies
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
save_intermediate_report <- function(data, report_type, dates) {
  if (!master_config$save_intermediate || is.null(data) || nrow(data) == 0) {
    return()
  }
  
  report_file <- file.path(master_config$log_dir, 
                           sprintf("nhl_%s_%s.txt", 
                                   report_type,
                                   format(Sys.Date(), format = "%Y%m%d")))
  
  sink(report_file)
  cat("NHL", toupper(report_type), "Report -", format(Sys.Date(), format = "%B %d, %Y"), "\n")
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

#' Create dummy goalie data when retrieval fails
#' @param games Schedule data frame
#' @return Data frame with NA goalie data
create_dummy_goalie_data <- function(games) {
  if (is.null(games) || nrow(games) == 0) {
    return(data.frame())
  }
  
  # Create rows for both home and away teams
  dummy_data <- bind_rows(
    # Home teams
    games %>%
      select(home, away, game_date, start_time_local) %>%
      mutate(
        game = paste0(away, " at ", home),
        away = away,
        home = home,
        start_time = start_time_local,
        team = home,
        goalie = NA_character_,
        status = "Unknown",
        status_updated = NA_character_,
        current_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ"),
        date = as.character(game_date),
        retrieval_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ")
      ),
    # Away teams  
    games %>%
      select(home, away, game_date, start_time_local) %>%
      mutate(
        game = paste0(away, " at ", home),
        away = away,
        home = home,
        start_time = start_time_local,
        team = away,
        goalie = NA_character_,
        status = "Unknown",
        status_updated = NA_character_,
        current_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ"),
        date = as.character(game_date),
        retrieval_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ")
      )
  ) %>%
    select(
      game,away,home,start_time,team,goalie,status,status_updated,current_time,date,retrieval_time
    )
  
  log_message("Created dummy goalie data due to retrieval failure", "WARN")
  return(dummy_data)
}

# MAIN EXECUTION ---------------------------------------------------------------

#' Main function to orchestrate all data retrieval and calculations
#' @param manual_dates Optional manual date specification
#' @return Combined data frame with expected values
retrieve_nhl_expected_values <- function(manual_dates = NULL) {
  
  # Get dates to process
  dates <- get_processing_dates(manual_dates)
  
  log_message(sprintf("Starting NHL data retrieval for %d date(s): %s",
                      length(dates),
                      paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")))
  
  # Step 1: Retrieve schedule (VITAL)
  log_message("Step 1/4: Retrieving NHL schedule", "INFO")
  
  if (!source_with_retry(master_config$schedule_script, "schedule", vital = TRUE)) {
    stop("Cannot proceed without schedule data")
  }
  
  games <- tryCatch({
    retrieve_nhl_schedule(dates = dates)
  }, error = function(e) {
    log_message(sprintf("Failed to retrieve schedule: %s", e$message), "ERROR")
    NULL
  })
  
  if (is.null(games) || nrow(games) == 0) {
    log_message("No games found for specified dates", "WARN")
    return(invisible(NULL))
  }
  
  log_message(sprintf("Retrieved %d games", nrow(games)), "SUCCESS")
  save_intermediate_report(games, "schedule", dates)
  
  # Step 2: Retrieve win probabilities (VITAL)
  log_message("Step 2/4: Retrieving win probabilities", "INFO")
  
  if (!source_with_retry(master_config$win_prob_script, "win probabilities", vital = TRUE)) {
    stop("Cannot proceed without win probability data")
  }
  
  win_probability <- tryCatch({
    retrieve_win_probabilities(dates = dates)
  }, error = function(e) {
    log_message(sprintf("Failed to retrieve win probabilities: %s", e$message), "ERROR")
    NULL
  })
  
  if (is.null(win_probability) || nrow(win_probability) == 0) {
    stop("No win probabilities found - cannot calculate expected values")
  }
  
  log_message(sprintf("Retrieved %d win probability entries", nrow(win_probability)), "SUCCESS")
  save_intermediate_report(win_probability, "win_probabilities", dates)
  
  # Step 3: Retrieve odds (VITAL)
  log_message("Step 3/4: Retrieving betting odds", "INFO")
  
  if (!source_with_retry(master_config$odds_script, "odds", vital = TRUE)) {
    stop("Cannot proceed without odds data")
  }
  
  betting_data <- tryCatch({
    retrieve_nhl_odds(dates = dates)
  }, error = function(e) {
    log_message(sprintf("Failed to retrieve odds: %s", e$message), "ERROR")
    NULL
  })
  
  if (is.null(betting_data) || nrow(betting_data) == 0) {
    stop("No odds found - cannot calculate expected values")
  }
  
  log_message(sprintf("Retrieved %d odds entries", nrow(betting_data)), "SUCCESS")
  save_intermediate_report(betting_data, "odds", dates)
  
  # Step 4: Retrieve starting goalies (NON-VITAL)
  log_message("Step 4/4: Retrieving starting goalies", "INFO")
  
  starting_goalies <- NULL
  if (source_with_retry(master_config$goalies_script, "starting goalies", vital = FALSE)) {
    starting_goalies <- tryCatch({
      retrieve_starting_goalies(dates = dates)
    }, error = function(e) {
      log_message(sprintf("Failed to retrieve goalies: %s", e$message), "ERROR")
      NULL
    })
  }
  
  # Create dummy data if goalies retrieval failed
  if (is.null(starting_goalies) || nrow(starting_goalies) == 0) {
    starting_goalies <- create_dummy_goalie_data(games)
  } else {
    log_message(sprintf("Retrieved %d goalie entries", nrow(starting_goalies)), "SUCCESS")
    save_intermediate_report(starting_goalies, "goalies", dates)
  }
  
  # DATA PROCESSING ------------------------------------------------------------
  
  log_message("Processing and merging data...", "INFO")
  
  # Clean team names function
  clean_team_name <- function(name) {
    toupper(gsub("\\.", "", name))
  }
  
  # Process betting data - get best odds per market
  betting_processed <- betting_data %>%
    mutate(
      home_team = clean_team_name(home_team),
      away_team = clean_team_name(away_team),
      name = clean_team_name(name)
    ) %>%
    group_by(market, home_team, away_team, name, point) %>%
    mutate(
      sd_price = sd(price, na.rm = TRUE),
      var_price = var(price, na.rm = TRUE)
    ) %>%
    slice_max(price, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  # Split by market
  h2h_odds <- betting_processed %>%
    filter(market == "h2h")
  
  # Clean schedule and win probability data
  games_clean <- games %>%
    mutate(
      home = clean_team_name(home),
      away = clean_team_name(away)
    )
  
  win_prob_clean <- win_probability %>%
    mutate(
      home = clean_team_name(home),
      away = clean_team_name(away),
      team = clean_team_name(team)
    )
  
  # Merge schedule with win probabilities
  schedule_win_prob <- games_clean %>%
    left_join(
      win_prob_clean,
      by = c("home", "away"),
      suffix = c("_schedule", "_winprob")
    )
  
  # Merge with odds
  win_prob_odds <- schedule_win_prob %>%
    left_join(
      h2h_odds,
      by = c("team" = "name", "away" = "away_team", "home" = "home_team"),
      suffix = c("_wp", "_odds")
    )
  
  # Clean goalie data and merge
  goalies_clean <- starting_goalies %>%
    mutate(
      team = clean_team_name(team),
      away = clean_team_name(away),
      home = clean_team_name(home)
    ) %>%
    select(team, away, home, goalie, status, status_updated)
  
  final_data <- win_prob_odds %>%
    left_join(
      goalies_clean,
      by = c("team", "away", "home"),
      suffix = c("_ev", "_sg")
    )
  
  # CALCULATE EXPECTED VALUES --------------------------------------------------
  
  log_message("Calculating expected values and Kelly Criterion...", "INFO")
  
  final_data <- final_data %>%
    mutate(
      # Convert win percentage to decimal
      win_percent = as.numeric(sub("%", "", win_probability)) / 100,
      
      # Calculate betting metrics
      expected_value = calcExpectedValue(price, win_percent),
      kelly_criterion = calc_kelly_crit(price, win_percent),
      implied_line = get_line(0, win_percent),
      implied_win_percent = get_prob(0, price),
      
      # Metadata
      current_time = Sys.time(),
      retrieval_date = Sys.Date()
    ) %>%
    # Reorder columns for readability
    select(
      # Game info
      team, home, away, game_date = date, game_time,
      
      # Goalie info
      goalie, goalie_status = status,
      
      # Betting info
      book, price, win_percent, expected_value, kelly_criterion,
      implied_line, implied_win_percent,
      
      # Metadata
      retrieval_date, current_time,
      
      # Keep all other columns
      everything()
    ) %>%
    arrange(game_date, game_time, home, team)
  
  # Set timezone
  attr(final_data$current_time, "tzone") <- "America/New_York"
  
  # SAVE OUTPUT ----------------------------------------------------------------
  
  timestamp <- format(Sys.time(), format = "%Y%m%d_%H%M%S")
  output_file <- file.path(master_config$output_dir, 
                           sprintf("%s_h2h_expected_values.csv", timestamp))
  
  final_data$file_name <- basename(output_file)
  
  write_csv(final_data, output_file)
  log_message(sprintf("Saved expected values to %s", output_file), "SUCCESS")
  
  # Generate summary report
  generate_final_report(final_data, output_file)
  
  # Email if configured
  if (master_config$email_reports) {
    email_report(output_file)
  }
  
  log_message("NHL data retrieval completed successfully", "SUCCESS")
  
  return(invisible(final_data))
}

#' Generate final summary report
#' @param data Final data frame
#' @param output_file Path to output file
generate_final_report <- function(data, output_file) {
  report_file <- file.path(master_config$log_dir,
                           sprintf("nhl_ev_summary_%s.txt",
                                   format(Sys.Date(), format = "%Y%m%d")))
  
  sink(report_file)
  cat("NHL EXPECTED VALUE SUMMARY REPORT\n")
  cat("Generated:", format(Sys.time(), format = "%Y-%m-%d %H:%M:%S"), "\n")
  cat(rep("=", 60), "\n\n")
  
  # Summary statistics
  cat("SUMMARY STATISTICS\n")
  cat("-----------------\n")
  cat("Total games:", length(unique(paste(data$home, data$away))), "\n")
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
      select(team, price, win_percent, expected_value, kelly_criterion, book)
    
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
email_report <- function(output_file) {
  # This is a placeholder - implement based on your email setup
  log_message("Email functionality not implemented", "WARN")
  
  # Example implementation:
  # cmd <- sprintf("mail -s 'NHL Expected Values Report' %s < %s",
  #                master_config$email_address, output_file)
  # system(cmd)
}

# RUN SCRIPT -------------------------------------------------------------------

# Only run if not being sourced
if (!interactive() && identical(environment(), globalenv())) {
  tryCatch({
    # Run the main function
    retrieve_nhl_expected_values()
  }, error = function(e) {
    log_message(sprintf("Fatal error: %s", e$message), "ERROR")
    quit(status = 1)
  })
}