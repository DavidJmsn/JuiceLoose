# RETRIEVE SCORES FOR NHL GAMES -------------------------------------------
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
  
  # Output configuration
  output_dir          = file.path(Sys.getenv("HOME"), "data/NHL/scores"),
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
                        sprintf("nhl_scores_%s.log", 
                                format(Sys.time(), format = "%Y%m%d_%H%M%S")))
  
  # Write header
  cat("NHL Master Scores Script\n", file = log_file)
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

# MAIN EXECUTION ---------------------------------------------------------------

#' Main function to orchestrate NHL score retrieval
#' @param manual_dates Optional manual date specification
#' @return Data frame with scores
retrieve_nhl_scores <- function(manual_dates = NULL) {
  
  # Get dates to process
  dates <- get_processing_dates(manual_dates)
  
  log_message(sprintf("Starting NHL scores retrieval for %d date(s): %s",
                      length(dates),
                      paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")))
  
  # Step 1: Retrieve schedule (VITAL)
  log_message("Step 1/1: Retrieving NHL scores", "INFO")
  
  if (!source_with_retry(master_config$schedule_script, "schedule", vital = TRUE)) {
    stop("Cannot proceed without schedule data")
  }
  
  games <- tryCatch({
    retrieve_nhl_schedule(dates = dates)
  }, error = function(e) {
    log_message(sprintf("Failed to retrieve scores: %s", e$message), "ERROR")
    NULL
  })
  
  if (is.null(games) || nrow(filter(games, !is.na(home_score))) == 0) {
    log_message("No games found for specified dates", "WARN")
    return(invisible(NULL))
  }
  
  log_message(sprintf("Retrieved %d games", nrow(filter(games, !is.na(home_score)))), "SUCCESS")
  save_intermediate_report(games, "schedule", dates)
  
  # SAVE OUTPUT ----------------------------------------------------------------
  
  timestamp <- format(Sys.time(), format = "%Y%m%d_%H%M%S")
  output_file <- file.path(master_config$output_dir, 
                           sprintf("%s_scores.csv", timestamp))
  
  output_df <- games |>
    filter(!is.na(home_score)) |>
    mutate(start_time_utc = format(start_time_utc, format = "%Y-%m-%dT%H:%M:%SZ")) |>
    select(
      game_id,season,game_type,game_state,start_time_utc,venue,home_team,home,
      away_team,away,game_date,start_time_local,tv_broadcasts,game_type_desc,
      home_score,away_score
    )
  
  write_csv(output_df, output_file)
  log_message(sprintf("Saved scores to %s", output_file), "SUCCESS")

  log_message("NHL score retrieval completed successfully", "SUCCESS")
  
  return(invisible(games))
}

# RUN SCRIPT -------------------------------------------------------------------

# Only run if not being sourced
if (!interactive() && identical(environment(), globalenv())) {
  tryCatch({
    # Run the main function
    retrieve_nhl_scores()
  }, error = function(e) {
    log_message(sprintf("Fatal error: %s", e$message), "ERROR")
    quit(status = 1)
  })
}