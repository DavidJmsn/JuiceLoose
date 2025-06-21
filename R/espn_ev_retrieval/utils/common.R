# SHARED UTILITIES FOR ESPN EV RETRIEVAL ----------------------------------
# Purpose: Common functions and configuration used across all ESPN scripts
# Author: Professional implementation with data.table optimization
# Last Updated: 2025-06-21

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(here)
})

# CONFIGURATION ------------------------------------------------------------

espn_config <- list(
  # Base paths
  data_dir = here("data", "ESPN"),
  log_file = NULL,  # Set at runtime
  
  # API endpoints
  espn_api_base = "https://sports.core.api.espn.com/v2/sports",
  odds_api_base = "https://api.the-odds-api.com",
  odds_api_key = Sys.getenv("ODDS_API_KEY"),
  
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
init_logging <- function(script_name = "espn_ev") {
  log_dir <- file.path(espn_config$data_dir, "logs")
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  
  log_file <- file.path(
    log_dir,
    sprintf("%s_%s.log", script_name, format(Sys.time(), format = "%Y%m%d_%H%M%S"))
  )
  
  # Set in config
  espn_config$log_file <<- log_file
  
  # Write header
  cat(sprintf("ESPN %s Script Log\n", toupper(script_name)), file = log_file)
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
  if (level == "DEBUG" && !espn_config$debug_mode) return(invisible())
  
  timestamp <- format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
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
  if (!is.null(espn_config$log_file)) {
    cat(log_entry, "\n", file = espn_config$log_file, append = TRUE)
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
    
    # Filter out non-date arguments
    args <- setdiff(args, c("scores", "delay", "NBA", "NFL", "MLB"))
    
    if (length(args) > 0) {
      dates <- args
    } else {
      # Default: today and tomorrow
      dates <- c(Sys.Date(), Sys.Date() + 1)
      log_message("No dates specified, using today and tomorrow", "INFO")
    }
  }
  
  # Convert to Date objects
  if (is.character(dates)) {
    # Try multiple date formats
    parsed_dates <- list()
    
    for (date_str in dates) {
      # Handle relative dates
      if (date_str == "today") {
        parsed_dates[[length(parsed_dates) + 1]] <- Sys.Date()
      } else if (date_str == "tomorrow") {
        parsed_dates[[length(parsed_dates) + 1]] <- Sys.Date() + 1
      } else if (date_str == "yesterday") {
        parsed_dates[[length(parsed_dates) + 1]] <- Sys.Date() - 1
      } else {
        # Try parsing
        parsed <- tryCatch({
          as.Date(date_str)
        }, error = function(e) {
          # Try alternative format
          as.Date(date_str, format = "%m/%d/%Y")
        })
        
        if (!is.na(parsed)) {
          parsed_dates[[length(parsed_dates) + 1]] <- parsed
        } else {
          log_message(sprintf("Could not parse date: %s", date_str), "WARN")
        }
      }
    }
    
    dates <- do.call(c, parsed_dates)
  }
  
  # Ensure Date class
  dates <- as.Date(dates)
  
  # Remove duplicates and sort
  dates <- sort(unique(dates))
  
  # Validate date range
  if (length(dates) > 30) {
    log_message("Limiting to 30 days to avoid API limits", "WARN")
    dates <- dates[1:30]
  }
  
  return(dates)
}

#' Check for delay argument in command line
#' @return invisible NULL
check_delay_arg <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  
  if ("delay" %in% args) {
    delay_seconds <- sample(1:10, 1)
    log_message(sprintf("Delaying start by %d seconds", delay_seconds), "INFO")
    Sys.sleep(delay_seconds)
  }
  
  invisible()
}

# TEAM NAME STANDARDIZATION ------------------------------------------------

#' Standardize team names to uppercase without city
#' @param team_name Character team name
#' @return Standardized team name
standardize_team_name <- function(team_name) {
  if (is.na(team_name) || team_name == "") return(NA_character_)
  
  # Convert to uppercase
  team_upper <- toupper(team_name)
  
  # Remove common city prefixes (comprehensive list)
  cities <- c(
    "NEW YORK", "LOS ANGELES", "LA", "SAN FRANCISCO", "SF", "SAN ANTONIO", 
    "SAN DIEGO", "ST LOUIS", "ST. LOUIS", "TAMPA BAY", "KANSAS CITY", 
    "OKLAHOMA CITY", "SALT LAKE CITY", "NEW ORLEANS", "LAS VEGAS", "VEGAS",
    "GOLDEN STATE", "NEW JERSEY", "NEW ENGLAND", "PORTLAND", "DENVER",
    "DALLAS", "HOUSTON", "PHOENIX", "PHILADELPHIA", "PHILLY", "MIAMI",
    "ATLANTA", "BOSTON", "BROOKLYN", "CHARLOTTE", "CHICAGO", "CLEVELAND",
    "DETROIT", "INDIANA", "MILWAUKEE", "MINNESOTA", "ORLANDO", "TORONTO",
    "WASHINGTON", "MEMPHIS", "SACRAMENTO", "UTAH", "GREEN BAY", "BUFFALO",
    "CINCINNATI", "BALTIMORE", "PITTSBURGH", "TENNESSEE", "JACKSONVILLE",
    "INDIANAPOLIS", "CAROLINA", "ARIZONA", "SEATTLE", "COLORADO", "TEXAS"
  )
  
  # Create regex pattern
  city_pattern <- paste0("^(", paste(cities, collapse = "|"), ")\\s+")
  
  # Remove city prefix
  team_clean <- gsub(city_pattern, "", team_upper)
  
  # Special cases
  team_clean <- switch(team_clean,
    "GOLDEN STATE WARRIORS" = "WARRIORS",
    "TRAIL BLAZERS" = "BLAZERS",
    "LA LAKERS" = "LAKERS",
    "LA CLIPPERS" = "CLIPPERS",
    "NY KNICKS" = "KNICKS",
    "NY YANKEES" = "YANKEES",
    "NY METS" = "METS",
    team_clean
  )
  
  return(team_clean)
}

# EV CALCULATION FUNCTIONS -------------------------------------------------

#' Calculate expected value
#' @param price Decimal odds
#' @param win_prob Win probability (0-1)
#' @return Expected value (-1 to Inf)
calculate_ev <- function(price, win_prob) {
  ifelse(is.na(price) | is.na(win_prob) | win_prob == 0,
         NA_real_,
         (price * win_prob) - 1)
}

#' Calculate Kelly Criterion
#' @param price Decimal odds  
#' @param win_prob Win probability (0-1)
#' @return Kelly fraction (0-1, capped at 0.25)
calculate_kelly <- function(price, win_prob) {
  kelly <- ifelse(
    is.na(price) | is.na(win_prob) | price <= 1 | win_prob == 0,
    NA_real_,
    (win_prob * price - 1) / (price - 1)
  )
  
  # Cap at 25% and floor at 0
  kelly <- pmin(pmax(kelly, 0), 0.25)
  
  return(kelly)
}

# RETRY LOGIC --------------------------------------------------------------

#' Execute function with retry logic
#' @param func Function to execute
#' @param ... Arguments to pass to func
#' @param max_retries Maximum retry attempts
#' @param delay Base delay between retries in seconds
#' @param context Description for logging
#' @return Function result or NULL on failure
with_retry <- function(func, ..., max_retries = espn_config$retry_max, 
                      delay = espn_config$retry_delay, context = "Operation") {
  
  for (attempt in 1:(max_retries + 1)) {
    result <- tryCatch({
      func(...)
    }, error = function(e) {
      if (attempt <= max_retries) {
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
    dir_path <- file.path(espn_config$data_dir, subdir)
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, showWarnings = FALSE, recursive = TRUE)
      log_message(sprintf("Created directory: %s", dir_path), "DEBUG")
    }
  }
}

# EXPORT CONFIGURATION FOR OTHER MODULES -----------------------------------

.espn_env <- new.env()
.espn_env$config <- espn_config
.espn_env$standardize_team_name <- standardize_team_name
.espn_env$log_message <- log_message
