# SHARED UTILITIES FOR NHL EV RETRIEVAL -----------------------------------
# Purpose: Common functions and configuration used across all NHL scripts
# Author: Professional implementation with data.table optimization
# Last Updated: 2025-12-19

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(here)
})

# CONFIGURATION ------------------------------------------------------------

nhl_config <- list(
  # Base paths
  data_dir = here("data", "NHL"),
  log_file = NULL,  # Set at runtime
  
  # API endpoints
  nhl_api_base = "https://api-web.nhle.com",
  odds_api_base = "https://api.the-odds-api.com",
  odds_api_key = Sys.getenv("ODDS_API_KEY"),
  
  # Scraping URLs
  moneypuck_url = "https://moneypuck.com/index.html",
  dailyfaceoff_url = "https://www.dailyfaceoff.com/starting-goalies",
  
  # Docker/Selenium settings
  selenium_port = 4444L,
  docker_image = "selenium/standalone-firefox:3.141.59",
  docker_memory = "2g",
  docker_path = Sys.getenv("DOCKER_PATH", "/opt/homebrew/bin/docker"),
  
  # Processing settings
  retry_max = 3,
  retry_delay = 2,
  timeout = 30,
  
  # Behavior flags
  save_intermediate = FALSE,
  debug_mode = FALSE
)

# LOGGING FUNCTIONS --------------------------------------------------------

#' Initialize logging for the session
#' @param script_name Name of the calling script
#' @return Path to log file
init_logging <- function(script_name = "nhl_ev") {
  log_dir <- file.path(nhl_config$data_dir, "logs")
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  
  log_file <- file.path(
    log_dir,
    sprintf("%s_%s.log", script_name, format(Sys.time(), "%Y%m%d_%H%M%S"))
  )
  
  # Set in config
  nhl_config$log_file <<- log_file
  
  # Write header
  cat(sprintf("NHL %s Script Log\n", toupper(script_name)), file = log_file)
  cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n", 
      file = log_file, append = TRUE)
  cat(strrep("=", 60), "\n\n", file = log_file, append = TRUE)
  
  return(log_file)
}

#' Log message with timestamp and level
#' @param msg Message to log
#' @param level Log level (INFO, WARN, ERROR, SUCCESS, DEBUG)
log_message <- function(msg, level = "INFO") {
  # Skip DEBUG messages unless debug mode is on
  if (level == "DEBUG" && !nhl_config$debug_mode) return(invisible())
  
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_entry <- sprintf("[%s] %s: %s", timestamp, level, msg)
  
  # Console output with color coding
  if (interactive() || level %in% c("ERROR", "WARN")) {
    if (level == "ERROR") {
      message(log_entry)
    } else if (level == "SUCCESS") {
      cat("\033[32m", log_entry, "\033[0m\n", sep = "")
    } else if (level == "WARN") {
      cat("\033[33m", log_entry, "\033[0m\n", sep = "")
    } else {
      cat(log_entry, "\n")
    }
  }
  
  # File output
  if (!is.null(nhl_config$log_file)) {
    cat(log_entry, "\n", file = nhl_config$log_file, append = TRUE)
  }
}

# DATE UTILITIES -----------------------------------------------------------

#' Parse and validate date arguments
#' @param dates Character vector, Date vector, or NULL
#' @return Vector of Date objects
parse_date_args <- function(dates = NULL) {
  # Check command line args if no dates provided
  if (is.null(dates)) {
    args <- commandArgs(trailingOnly = TRUE)
    
    if (length(args) > 0 && args[1] != "delay") {
      dates <- args[!args %in% "delay"]
    } else {
      # Default to today and tomorrow
      dates <- c(Sys.Date(), Sys.Date() + 1)
      log_message("No dates specified, using today and tomorrow")
    }
  }
  
  # Convert to Date objects
  if (is.character(dates)) {
    dates <- tryCatch(
      as.Date(dates),
      error = function(e) {
        log_message(sprintf("Invalid date format: %s", paste(dates, collapse = ", ")), "ERROR")
        stop("Dates must be in YYYY-MM-DD format")
      }
    )
  }
  
  # Remove duplicates and sort
  dates <- sort(unique(as.Date(dates)))
  
  log_message(sprintf("Processing %d date(s): %s", 
                      length(dates),
                      paste(format(dates, "%Y-%m-%d"), collapse = ", ")))
  
  return(dates)
}

#' Check for delay argument
#' @return NULL (processes delay if found)
check_delay_arg <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if ("delay" %in% args) {
    log_message("Delaying execution by 5 minutes...", "INFO")
    Sys.sleep(300)
    log_message("Resuming execution", "INFO")
  }
  return(invisible())
}

# TEAM NAME STANDARDIZATION ------------------------------------------------

#' Standardize team names for consistent merging
#' @param names Character vector of team names
#' @return Standardized team names
standardize_team_name <- function(names) {
  if (is.null(names) || length(names) == 0) return(character(0))
  
  # Convert to uppercase and remove periods
  standardized <- toupper(gsub("\\.", "", names))
  
  # Remove "Utah" prefix if present
  standardized <- gsub("^UTAH\\s+", "", standardized)
  
  # Handle special characters
  standardized <- iconv(standardized, from = "UTF-8", to = "ASCII//TRANSLIT")
  
  # Remove any remaining non-alphanumeric except spaces
  standardized <- gsub("[^A-Z0-9 ]", "", standardized)
  
  # Trim whitespace
  standardized <- trimws(standardized)
  
  return(standardized)
}

# DATA VALIDATION ----------------------------------------------------------

#' Validate required columns exist in data.table
#' @param dt data.table to check
#' @param required_cols Character vector of required column names
#' @param context Context for error message
validate_columns <- function(dt, required_cols, context = "data") {
  if (!is.data.table(dt)) {
    log_message(sprintf("%s is not a data.table", context), "ERROR")
    return(FALSE)
  }
  
  missing <- setdiff(required_cols, names(dt))
  if (length(missing) > 0) {
    log_message(sprintf("Missing columns in %s: %s", 
                        context, paste(missing, collapse = ", ")), "ERROR")
    return(FALSE)
  }
  
  return(TRUE)
}

#' Validate data quality
#' @param dt data.table to check
#' @param context Context for logging
validate_data_quality <- function(dt, context = "data") {
  if (nrow(dt) == 0) {
    log_message(sprintf("No data in %s", context), "WARN")
    return(FALSE)
  }
  
  # Check for excessive NAs
  na_pct <- sapply(dt, function(x) sum(is.na(x)) / length(x) * 100)
  high_na <- names(na_pct[na_pct > 50])
  
  if (length(high_na) > 0) {
    log_message(sprintf("High NA percentage in %s columns: %s", 
                        context, paste(high_na, collapse = ", ")), "WARN")
  }
  
  return(TRUE)
}

# CALCULATION FUNCTIONS ----------------------------------------------------

#' Calculate expected value from line and win probability
#' @param line Decimal odds
#' @param win_prob Win probability (0-1)
#' @return Expected value
calculate_ev <- function(line, win_prob) {
  ifelse(is.na(line) | is.na(win_prob) | line <= 0 | win_prob <= 0 | win_prob > 1,
         NA_real_,
         line * win_prob - 1)
}

#' Calculate Kelly Criterion
#' @param line Decimal odds  
#' @param win_prob Win probability (0-1)
#' @return Kelly Criterion value
calculate_kelly <- function(line, win_prob) {
  ifelse(is.na(line) | is.na(win_prob) | line <= 1 | win_prob <= 0 | win_prob >= 1,
         NA_real_,
         win_prob - ((1 - win_prob) / (line - 1)))
}

# ERROR HANDLING -----------------------------------------------------------

#' Execute function with retry logic
#' @param fn Function to execute
#' @param ... Arguments to pass to function
#' @param max_retries Maximum retry attempts
#' @param delay Delay between retries in seconds
#' @param context Context for logging
#' @return Function result or NULL on failure
with_retry <- function(fn, ..., max_retries = nhl_config$retry_max, 
                       delay = nhl_config$retry_delay, context = "operation") {
  for (attempt in 1:max_retries) {
    result <- tryCatch({
      fn(...)
    }, error = function(e) {
      if (attempt < max_retries) {
        log_message(sprintf("%s failed (attempt %d/%d): %s. Retrying...", 
                            context, attempt, max_retries, e$message), "WARN")
        Sys.sleep(delay * attempt)  # Exponential backoff
        NULL
      } else {
        log_message(sprintf("%s failed after %d attempts: %s", 
                            context, max_retries, e$message), "ERROR")
        NULL
      }
    })
    
    if (!is.null(result)) return(result)
  }
  
  return(NULL)
}

# DIRECTORY MANAGEMENT -----------------------------------------------------

#' Ensure output directories exist
#' @param subdirs Character vector of subdirectories to create
ensure_directories <- function(subdirs = c("expected_value", "scores", "logs")) {
  for (subdir in subdirs) {
    dir_path <- file.path(nhl_config$data_dir, subdir)
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, showWarnings = FALSE, recursive = TRUE)
      log_message(sprintf("Created directory: %s", dir_path), "DEBUG")
    }
  }
}

# EXPORT CONFIGURATION FOR OTHER MODULES -----------------------------------

.nhl_env <- new.env()
.nhl_env$config <- nhl_config
.nhl_env$standardize_team_name <- standardize_team_name
.nhl_env$log_message <- log_message
