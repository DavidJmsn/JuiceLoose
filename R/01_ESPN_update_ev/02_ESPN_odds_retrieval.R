# RETRIEVE SPORTS ODDS -------------------------------------------------------
# Purpose: Retrieve betting odds for multiple sports with robust error handling and optimization
# Author: David Jamieson (improved version)
# E-mail: david.jmsn@icloud.com
# Last Updated: 2025-06-02

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

# Sport mappings for API
SPORT_MAPPINGS <- list(
  "NHL" = "icehockey_nhl",
  "NFL" = "americanfootball_nfl", 
  "NBA" = "basketball_nba",
  "MLB" = "baseball_mlb"
)

# Available markets
AVAILABLE_MARKETS <- c("h2h", "spreads", "totals")

config <- list(
  base_url = "https://api.the-odds-api.com",
  regions = "us",
  odds_format = "decimal",
  date_format = "iso",
  api_key = Sys.getenv("ODDS_API_KEY"),
  max_retries = 3,
  retry_delay = 2,  # seconds
  timeout = 30,     # seconds
  base_data_dir = file.path(Sys.getenv("HOME"), "data"),
  log_file = "odds_retrieval.log"
)

# LOGGING FUNCTIONS -------------------------------------------------------

log_message <- function(message, level = "INFO", output_dir = NULL) {
  timestamp <- format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
  log_entry <- sprintf("[%s] %s: %s", timestamp, level, message)
  
  # Print to console
  if (level == "ERROR") {
    message(log_entry)
  } else {
    cat(log_entry, "\n")
  }
  
  # Write to log file (if output_dir provided)
  if (!is.null(output_dir)) {
    log_path <- file.path(output_dir, config$log_file)
    cat(log_entry, "\n", file = log_path, append = TRUE)
  }
}

# SPORT AND MARKET VALIDATION ---------------------------------------------

#' Create sport-specific output directory
#' @param sport_name Sport name (e.g., "NHL")
#' @return Full path to sport-specific odds directory
create_sport_directory <- function(sport_name) {
  sport_dir <- file.path(config$base_data_dir, sport_name, "odds")
  dir.create(sport_dir, showWarnings = FALSE, recursive = TRUE)
  return(sport_dir)
}

#' Validate and convert sport parameter
#' @param sport Sport name (NHL, NFL, NBA, MLB) - case insensitive
#' @return List with sport name, API code, and output directory
validate_sport <- function(sport) {
  if (is.null(sport) || is.na(sport) || sport == "") {
    stop("Sport parameter is required. Must be one of: ", paste(names(SPORT_MAPPINGS), collapse = ", "))
  }
  
  # Convert to uppercase for case-insensitive matching
  sport_upper <- toupper(sport)
  
  if (!sport_upper %in% names(SPORT_MAPPINGS)) {
    stop("Invalid sport '", sport, "'. Must be one of: ", paste(names(SPORT_MAPPINGS), collapse = ", "))
  }
  
  api_sport_code <- SPORT_MAPPINGS[[sport_upper]]
  output_dir <- create_sport_directory(sport_upper)
  
  log_message(sprintf("Using sport: %s (API code: %s)", sport_upper, api_sport_code), output_dir = output_dir)
  log_message(sprintf("Output directory: %s", output_dir), output_dir = output_dir)
  
  return(list(name = sport_upper, code = api_sport_code, output_dir = output_dir))
}

#' Validate and convert markets parameter
#' @param markets Character vector of market types or single string
#' @return Character vector of validated markets
validate_markets <- function(markets) {
  if (is.null(markets) || length(markets) == 0) {
    stop("Markets parameter is required. Must be one or more of: ", paste(AVAILABLE_MARKETS, collapse = ", "))
  }
  
  # Convert to lowercase for case-insensitive matching
  markets_lower <- tolower(markets)
  
  # Check for invalid markets
  invalid_markets <- markets_lower[!markets_lower %in% AVAILABLE_MARKETS]
  if (length(invalid_markets) > 0) {
    stop("Invalid market(s): ", paste(invalid_markets, collapse = ", "), 
         ". Must be one or more of: ", paste(AVAILABLE_MARKETS, collapse = ", "))
  }
  
  # Remove duplicates and sort
  markets_clean <- sort(unique(markets_lower))
  
  return(markets_clean)
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
#' @param output_dir Output directory for logging
#' @return Unnested data.table
unnest_json_data <- function(json_data, output_dir = NULL) {
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
    log_message(sprintf("Error unnesting JSON data: %s", e$message), "ERROR", output_dir = output_dir)
    return(NULL)
  })
}

#' Format betting data with clean column names
#' @param betting_data Unnested betting data
#' @param output_dir Output directory for logging
#' @return Formatted data.table with standardized column names
format_betting_data <- function(betting_data, output_dir = NULL) {
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
    log_message(sprintf("Error formatting betting data: %s", e$message), "ERROR", output_dir = output_dir)
    return(NULL)
  })
}

#' Parse and enhance odds data
#' @param raw_data Raw API response data
#' @param sport_name Sport name for metadata
#' @param output_dir Output directory for logging
#' @return Enhanced data.table with parsed timestamps
parse_odds_data <- function(raw_data, sport_name, output_dir = NULL) {
  if (is.null(raw_data) || length(raw_data) == 0) {
    return(NULL)
  }
  
  # Unnest the JSON structure
  unnested_data <- unnest_json_data(raw_data, output_dir)
  if (is.null(unnested_data)) {
    return(NULL)
  }
  
  # Format column names
  odds_dt <- format_betting_data(unnested_data, output_dir)
  if (is.null(odds_dt)) {
    return(NULL)
  }
  
  # Parse timestamps with error handling
  tryCatch({
    odds_dt[, `:=`(
      commence_time = format(ymd_hms(commence_time, tz = "UTC", quiet = TRUE), format = "%Y-%m-%dT%H:%M:%SZ"),
      book_last_update = format(ymd_hms(book_last_update, tz = "UTC", quiet = TRUE), format = "%Y-%m-%dT%H:%M:%SZ"),
      market_last_update = format(ymd_hms(market_last_update, tz = "UTC", quiet = TRUE), format = "%Y-%m-%dT%H:%M:%SZ"),
      retrieved_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ"),
      sport = sport_name
    )]
  }, error = function(e) {
    log_message(sprintf("Error parsing timestamps: %s", e$message), "WARN", output_dir = output_dir)
  })
  
  # Add game date for easier filtering
  odds_dt[, game_date := as.Date(commence_time)]
  
  return(odds_dt)
}

#' Build API endpoint URL for date range
#' @param start_date Start date of range
#' @param end_date End date of range
#' @param sport_code API sport code
#' @param markets Character vector of markets to include
#' @return Formatted endpoint URL
build_endpoint <- function(start_date, end_date, sport_code, markets) {
  # Convert dates to ISO format timestamps
  start_dt <- as.POSIXct(start_date, tz = "UTC")
  end_dt <- as.POSIXct(end_date, tz = "UTC") + days(2) - 1
  
  start_iso <- format(start_dt, format = "%Y-%m-%dT%H:%M:%SZ")
  end_iso <- format(end_dt, format = "%Y-%m-%dT%H:%M:%SZ")
  
  # Build endpoint URL
  endpoint <- sprintf(
    "/v4/sports/%s/odds/?regions=%s&oddsFormat=%s&markets=%s&dateFormat=%s&commenceTimeFrom=%s&commenceTimeTo=%s&apiKey=%s",
    sport_code,
    config$regions,
    config$odds_format,
    paste(markets, collapse = ","),
    config$date_format,
    start_iso,
    end_iso,
    config$api_key
  )
  
  return(endpoint)
}

# MAIN EXECUTION ----------------------------------------------------------

#' Retrieve sports betting odds for specified dates and sport
#' @param dates Date or vector of dates to retrieve odds for.
#'              Can be Date objects or character strings in "YYYY-MM-DD" format.
#'              If NULL (default), retrieves today and tomorrow's odds.
#' @param sport Sport to retrieve odds for. Must be one of: "NHL", "NFL", "NBA", "MLB".
#'              Case insensitive. Default is "NHL".
#' @param markets Character vector of markets to retrieve. Must be one or more of: "h2h", "spreads", "totals".
#'                Case insensitive. Default is c("h2h", "spreads", "totals").
#' @return Data frame with all odds for the specified dates and sport (invisibly)
#' @examples
#' # Get today and tomorrow's NHL odds (all markets)
#' retrieve_odds(sport = "NHL")
#' 
#' # Get NBA head-to-head odds only for a specific date
#' retrieve_odds("2025-06-01", sport = "NBA", markets = "h2h")
#' 
#' # Get NFL spreads and totals for multiple dates
#' retrieve_odds(c("2025-09-01", "2025-09-02"), sport = "NFL", markets = c("spreads", "totals"))
#' 
#' # Get MLB odds using Date objects
#' retrieve_odds(Sys.Date() + 0:7, sport = "MLB", markets = "h2h")  # Next week's MLB moneylines
retrieve_odds <- function(dates = NULL, sport = "NHL", markets = c("h2h", "spreads", "totals")) {
  # Validate sport parameter
  sport_info <- validate_sport(sport)
  sport_name <- sport_info$name
  sport_code <- sport_info$code
  output_dir <- sport_info$output_dir
  
  # Validate markets parameter
  validated_markets <- validate_markets(markets)
  log_message(sprintf("Using markets: %s", paste(validated_markets, collapse = ", ")), output_dir = output_dir)
  
  # Default to today and tomorrow if no dates provided
  if (is.null(dates)) {
    dates <- c(Sys.Date(), Sys.Date() + 1)
    log_message(sprintf("No dates specified, retrieving %s odds for today and tomorrow", sport_name), output_dir = output_dir)
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
  
  log_message(sprintf("Starting %s odds retrieval for date range: %s to %s", 
                      sport_name,
                      format(min_date, format = "%Y-%m-%d"),
                      format(max_date, format = "%Y-%m-%d")), output_dir = output_dir)
  
  # Validate API key
  if (is.null(config$api_key) || config$api_key == "") {
    log_message("API key not found. Please set ODDS_API_KEY environment variable", "ERROR", output_dir = output_dir)
    return(invisible(NULL))
  }
  
  # Build endpoint for date range
  endpoint <- build_endpoint(start_date = min_date, end_date = max_date, sport_code = sport_code, markets = validated_markets)
  
  # Fetch odds data
  log_message(sprintf("Fetching %s odds data from API...", sport_name), output_dir = output_dir)
  raw_data <- api_request(endpoint)
  
  if (is.null(raw_data) || length(raw_data) == 0) {
    log_message(sprintf("No %s odds data returned for the specified date range", sport_name), "WARN", output_dir = output_dir)
    return(invisible(NULL))
  }
  
  log_message(sprintf("Retrieved %d %s events from API", length(raw_data), sport_name), output_dir = output_dir)
  
  # Parse and process the data
  odds_dt <- parse_odds_data(raw_data, sport_name, output_dir)
  
  if (is.null(odds_dt) || nrow(odds_dt) == 0) {
    log_message(sprintf("No valid %s odds data after processing", sport_name), "WARN", output_dir = output_dir)
    return(invisible(NULL))
  }
  
  log_message(sprintf("Processed %d total %s odds records across %d games", 
                      nrow(odds_dt), 
                      sport_name,
                      uniqueN(odds_dt$id)), output_dir = output_dir)
  
  # Log market summary
  market_summary <- odds_dt[, .N, by = market]
  log_message(sprintf("Market breakdown: %s", 
                      paste(sprintf("%s=%d", market_summary$market, market_summary$N), 
                            collapse = ", ")), output_dir = output_dir)
  
  # Log bookmaker summary
  book_count <- uniqueN(odds_dt$book)
  log_message(sprintf("Found odds from %d different bookmakers", book_count), output_dir = output_dir)
  
  # Create filename with date range, sport, and markets
  if (length(dates) == 1) {
    date_suffix <- format(dates[1], format = "%Y-%m-%d")
  } else {
    date_suffix <- sprintf("%s_to_%s", 
                           format(min(dates), format = "%Y-%m-%d"),
                           format(max(dates), format = "%Y-%m-%d"))
  }
  
  # Include markets in filename if not all markets
  if (length(validated_markets) < length(AVAILABLE_MARKETS)) {
    market_suffix <- paste(validated_markets, collapse = "_")
    filename <- sprintf("%s_%s_odds_%s_%s.csv",
                        format(Sys.time(), format = "%Y%m%d_%H%M%S"),
                        tolower(sport_name),
                        date_suffix,
                        market_suffix)
  } else {
    filename <- sprintf("%s_%s_odds_%s.csv",
                        format(Sys.time(), format = "%Y%m%d_%H%M%S"),
                        tolower(sport_name),
                        date_suffix)
  }
  
  output_path <- file.path(output_dir, filename)
  
  # Write to CSV
  fwrite(odds_dt, file = output_path)
  log_message(sprintf("%s odds saved to: %s", sport_name, output_path), output_dir = output_dir)
  
  # Return the data frame invisibly
  invisible(odds_dt)
}

# CONVENIENCE FUNCTIONS ---------------------------------------------------

#' Get available sports
#' @return Character vector of supported sport names
get_supported_sports <- function() {
  return(names(SPORT_MAPPINGS))
}

#' Get available markets
#' @return Character vector of supported market types
get_available_markets <- function() {
  return(AVAILABLE_MARKETS)
}

#' Retrieve NHL odds (backwards compatibility)
#' @param dates Date or vector of dates to retrieve odds for
#' @param markets Markets to retrieve (optional, defaults to all)
#' @return Data frame with NHL odds (invisibly)
retrieve_nhl_odds <- function(dates = NULL, markets = c("h2h", "spreads", "totals")) {
  .Deprecated("retrieve_odds", 
              msg = "retrieve_nhl_odds() is deprecated. Use retrieve_odds(dates, sport = 'NHL', markets = markets) instead.")
  retrieve_odds(dates = dates, sport = "NHL", markets = markets)
}

# # Run the script
# if (!interactive()) {
#   # When run as a script, use default behavior (NHL for today and tomorrow, all markets)
#   retrieve_odds(sport = "NHL")
# }