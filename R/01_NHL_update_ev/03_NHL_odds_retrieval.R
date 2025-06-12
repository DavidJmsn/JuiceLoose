# RETRIEVE NHL ODDS -------------------------------------------------------
# Purpose: Retrieve NHL betting odds with robust error handling and optimization
# Author: David Jamieson (improved version)
# E-mail: david.jmsn@icloud.com
# Last Updated: 2025-05-31

# SETUP -------------------------------------------------------------------

# Suppress startup messages
suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(lubridate)
  library(data.table)
  library(tidyr)
  library(stringr)
})

# CONFIGURATION -----------------------------------------------------------

config <- list(
  base_url = "https://api.the-odds-api.com",
  sport = "icehockey_nhl",
  regions = "us",
  odds_format = "decimal",
  markets = c("h2h", "spreads", "totals"),
  date_format = "iso",
  api_key = Sys.getenv("ODDS_API_KEY"),
  max_retries = 3,
  retry_delay = 2,  # seconds
  timeout = 30,     # seconds
  output_dir = file.path(Sys.getenv("HOME"), "data/NHL/odds"),
  log_file = "nhl_odds_retrieval.log"
)

# Create output directory if it doesn't exist
dir.create(config$output_dir, showWarnings = FALSE, recursive = TRUE)

# LOGGING FUNCTIONS -------------------------------------------------------

log_message <- function(message, level = "INFO") {
  timestamp <- format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
  log_entry <- sprintf("[%s] %s: %s", timestamp, level, message)
  
  # Print to console
  if (level == "ERROR") {
    message(log_entry)
  } else {
    cat(log_entry, "\n")
  }
  
  # Write to log file
  log_path <- file.path(config$output_dir, config$log_file)
  cat(log_entry, "\n", file = log_path, append = TRUE)
}

# API FUNCTIONS WITH RETRY LOGIC ------------------------------------------

#' Make API request with retry logic
#' @param endpoint API endpoint
#' @param max_retries Maximum number of retry attempts
#' @return Parsed JSON response or NULL on failure
api_request <- function(endpoint, max_retries = config$max_retries) {
  full_url <- paste0(config$base_url, endpoint)
  
  for (attempt in 1:max_retries) {
    tryCatch({
      response <- GET(
        full_url,
        timeout(config$timeout),
        add_headers("Accept" = "application/json")
      )
      
      if (status_code(response) == 200) {
        content_text <- content(response, "text", encoding = "UTF-8")
        return(fromJSON(content_text, simplifyDataFrame = TRUE))
      } else if (status_code(response) %in% c(429, 500, 502, 503, 504)) {
        # Retryable errors
        if (attempt < max_retries) {
          log_message(sprintf("API request failed (attempt %d/%d): Status %d. Retrying...", 
                              attempt, max_retries, status_code(response)), "WARN")
          Sys.sleep(config$retry_delay * attempt)  # Exponential backoff
        }
      } else {
        # Non-retryable error
        log_message(sprintf("API request failed: Status %d for URL: %s", 
                            status_code(response), full_url), "ERROR")
        return(NULL)
      }
    }, error = function(e) {
      log_message(sprintf("API request error (attempt %d/%d): %s", 
                          attempt, max_retries, e$message), "WARN")
      if (attempt < max_retries) {
        Sys.sleep(config$retry_delay * attempt)
      }
    })
  }
  
  log_message("API request failed after all retries", "ERROR")
  return(NULL)
}

# DATA TRANSFORMATION FUNCTIONS -------------------------------------------

#' Unnest nested JSON data structure
#' @param json_data Raw JSON data from API
#' @return Unnested data.table
unnest_json_data <- function(json_data) {
  tryCatch({
    # Convert to tibble for unnesting
    df <- as_tibble(json_data)
    
    # Unnest bookmakers
    df1 <- unnest_longer(df, col = "bookmakers")
    dt1 <- as.data.table(df1)
    
    # Unnest markets
    dt2 <- unnest_longer(dt1, col = "bookmakers.markets")
    dt2 <- as.data.table(dt2)
    
    # Unnest outcomes
    dt3 <- unnest_longer(dt2, col = "bookmakers.markets.outcomes")
    dt3 <- as.data.table(dt3)
    
    return(dt3)
  }, error = function(e) {
    log_message(sprintf("Error unnesting JSON data: %s", e$message), "ERROR")
    return(NULL)
  })
}

#' Format betting data with clean column names
#' @param betting_data Unnested betting data
#' @return Formatted data.table with standardized column names
format_betting_data <- function(betting_data) {
  if (is.null(betting_data) || nrow(betting_data) == 0) {
    return(NULL)
  }
  
  tryCatch({
    # Rename nested columns
    setnames(
      betting_data,
      old = c("bookmakers.key", "bookmakers.markets.key",
              "bookmakers.last_update", "bookmakers.markets.last_update"),
      new = c("book", "market", "book_last_update", "market_last_update"),
      skip_absent = TRUE
    )
    
    # Clean column names systematically
    new_names <- sapply(names(betting_data), function(nm) {
      # Remove nested prefixes
      if (grepl("\\.", nm)) {
        nm <- sub(".*\\.", "", nm)
      }
      # Convert to lowercase
      nm <- tolower(nm)
      # Replace spaces with underscores
      nm <- gsub("\\s+", "_", nm)
      # Remove non-alphanumeric characters
      nm <- gsub("[^a-z0-9_]", "", nm)
      # Remove leading/trailing underscores
      nm <- gsub("^_+|_+$", "", nm)
      return(nm)
    })
    
    setnames(betting_data, old = names(betting_data), new = new_names)
    
    return(betting_data)
  }, error = function(e) {
    log_message(sprintf("Error formatting betting data: %s", e$message), "ERROR")
    return(NULL)
  })
}

#' Parse and enhance odds data
#' @param raw_data Raw API response data
#' @return Enhanced data.table with parsed timestamps
parse_odds_data <- function(raw_data) {
  if (is.null(raw_data) || length(raw_data) == 0) {
    return(NULL)
  }
  
  # Unnest the JSON structure
  unnested_data <- unnest_json_data(raw_data)
  if (is.null(unnested_data)) {
    return(NULL)
  }
  
  # Format column names
  odds_dt <- format_betting_data(unnested_data)
  if (is.null(odds_dt)) {
    return(NULL)
  }
  
  # Parse timestamps with error handling
  tryCatch({
    odds_dt[, `:=`(
      commence_time = format(ymd_hms(commence_time, tz = "UTC", quiet = TRUE), format = "%Y-%m-%dT%H:%M:%SZ"),
      book_last_update = format(ymd_hms(book_last_update, tz = "UTC", quiet = TRUE), format = "%Y-%m-%dT%H:%M:%SZ"),
      market_last_update = format(ymd_hms(market_last_update, tz = "UTC", quiet = TRUE), format = "%Y-%m-%dT%H:%M:%SZ"),
      retrieved_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ")
    )]
  }, error = function(e) {
    log_message(sprintf("Error parsing timestamps: %s", e$message), "WARN")
  })
  
  # Add game date for easier filtering
  odds_dt[, game_date := as.Date(commence_time)]
  
  return(odds_dt)
}

#' Build API endpoint URL for date range
#' @param start_date Start date of range
#' @param end_date End date of range
#' @return Formatted endpoint URL
build_endpoint <- function(start_date, end_date) {
  # Convert dates to ISO format timestamps
  start_dt <- as.POSIXct(start_date, tz = "UTC")
  end_dt <- as.POSIXct(end_date, tz = "UTC") + days(2) - 1
  
  start_iso <- format(start_dt, format = "%Y-%m-%dT%H:%M:%SZ")
  end_iso <- format(end_dt, format = "%Y-%m-%dT%H:%M:%SZ")
  
  # Build endpoint URL
  endpoint <- sprintf(
    "/v4/sports/%s/odds/?regions=%s&oddsFormat=%s&markets=%s&dateFormat=%s&commenceTimeFrom=%s&commenceTimeTo=%s&apiKey=%s",
    config$sport,
    config$regions,
    config$odds_format,
    paste(config$markets, collapse = ","),
    config$date_format,
    start_iso,
    end_iso,
    config$api_key
  )
  
  return(endpoint)
}

# MAIN EXECUTION ----------------------------------------------------------

#' Retrieve NHL odds for specified dates
#' @param dates Date or vector of dates to retrieve odds for.
#'              Can be Date objects or character strings in "YYYY-MM-DD" format.
#'              If NULL (default), retrieves today and tomorrow's odds.
#' @return Data frame with all odds for the specified dates (invisibly)
#' @examples
#' # Get today and tomorrow's odds
#' retrieve_nhl_odds()
#' 
#' # Get odds for a specific date
#' retrieve_nhl_odds("2025-06-01")
#' 
#' # Get odds for multiple dates
#' retrieve_nhl_odds(c("2025-06-01", "2025-06-02", "2025-06-03"))
#' 
#' # Using Date objects
#' retrieve_nhl_odds(Sys.Date() + 0:7)  # Next week's odds
retrieve_nhl_odds <- function(dates = NULL) {
  # Default to today and tomorrow if no dates provided
  if (is.null(dates)) {
    dates <- c(Sys.Date(), Sys.Date() + 1)
    log_message("No dates specified, retrieving odds for today and tomorrow")
  } else {
    # Convert character dates to Date objects
    if (is.character(dates)) {
      dates <- as.Date(dates)
    }
    # Ensure dates is a vector
    dates <- as.Date(dates)
  }
  
  # Remove duplicate dates and sort
  dates <- sort(unique(dates))
  
  # Determine date range
  min_date <- min(dates)
  max_date <- max(dates)
  
  log_message(sprintf("Starting NHL odds retrieval for date range: %s to %s", 
                      format(min_date, format = "%Y-%m-%d"),
                      format(max_date, format = "%Y-%m-%d")))
  
  # Validate API key
  if (is.null(config$api_key) || config$api_key == "") {
    log_message("API key not found. Please set ODDS_API_KEY environment variable", "ERROR")
    return(invisible(NULL))
  }
  
  # Build endpoint for date range
  endpoint <- build_endpoint(start_date = min_date, end_date = max_date)
  
  # Fetch odds data
  log_message("Fetching odds data from API...")
  raw_data <- api_request(endpoint)
  
  if (is.null(raw_data) || length(raw_data) == 0) {
    log_message("No odds data returned for the specified date range", "WARN")
    return(invisible(NULL))
  }
  
  log_message(sprintf("Retrieved %d events from API", length(raw_data)))
  
  # Parse and process the data
  odds_dt <- parse_odds_data(raw_data)
  
  if (is.null(odds_dt) || nrow(odds_dt) == 0) {
    log_message("No valid odds data after processing", "WARN")
    return(invisible(NULL))
  }
  
  log_message(sprintf("Processed %d total odds records across %d games", 
                      nrow(odds_dt), 
                      uniqueN(odds_dt$id)))
  
  # Log market summary
  market_summary <- odds_dt[, .N, by = market]
  log_message(sprintf("Market breakdown: %s", 
                      paste(sprintf("%s=%d", market_summary$market, market_summary$N), 
                            collapse = ", ")))
  
  # Log bookmaker summary
  book_count <- uniqueN(odds_dt$book)
  log_message(sprintf("Found odds from %d different bookmakers", book_count))
  
  # Create filename with date range
  if (length(dates) == 1) {
    date_suffix <- format(dates[1], format = "%Y-%m-%d")
  } else {
    date_suffix <- sprintf("%s_to_%s", 
                           format(min(dates), format = "%Y-%m-%d"),
                           format(max(dates), format = "%Y-%m-%d"))
  }
  
  filename <- sprintf("%s_nhl_odds_%s.csv",
                      format(Sys.time(), format = "%Y%m%d_%H%M%S"),
                      date_suffix)
  output_path <- file.path(config$output_dir, filename)
  
  # Write to CSV
  fwrite(odds_dt, file = output_path)
  log_message(sprintf("Odds saved to: %s", output_path))
  
  # Return the data frame invisibly
  invisible(odds_dt)
}

# # Run the script
# if (!interactive()) {
#   # When run as a script, use default behavior (today and tomorrow)
#   retrieve_nhl_odds()
# }