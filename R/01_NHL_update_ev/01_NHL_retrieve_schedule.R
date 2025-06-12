# RETRIEVE NHL SCHEDULE ----------------------------------------------------
# Purpose: Retrieve NHL game schedules with robust error handling and optimization
# Author: David Jamieson (improved version)
# E-mail: david.jmsn@icloud.com
# Last Updated: 2025-05-30

# SETUP -------------------------------------------------------------------

# Suppress startup messages
suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(lubridate)
})

# CONFIGURATION -----------------------------------------------------------

config <- list(
  base_url = "https://api-web.nhle.com",
  max_retries = 3,
  retry_delay = 2,  # seconds
  timeout = 30,     # seconds
  output_dir = file.path(Sys.getenv("HOME"), "data/NHL/schedule"),
  log_file = "nhl_schedule_retrieval.log"
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
        return(fromJSON(content(response, "text", encoding = "UTF-8")))
      } else if (status_code(response) %in% c(429, 500, 502, 503, 504)) {
        # Retryable errors
        if (attempt < max_retries) {
          log_message(sprintf("API request failed (attempt %d/%d): Status %d. Retrying...", 
                              attempt, max_retries, status_code(response)), "WARN")
          Sys.sleep(config$retry_delay * attempt)  # Exponential backoff
        }
      } else {
        # Non-retryable error
        log_message(sprintf("API request failed: Status %d", status_code(response)), "ERROR")
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

# SCHEDULE FUNCTIONS ------------------------------------------------------

#' Get NHL schedule for a specific date
#' @param date Date in YYYY-MM-DD format (default: today)
#' @return Schedule data or NULL on failure
get_schedule_by_date <- function(date = Sys.Date()) {
  # Ensure date is in correct format
  if (is.character(date)) {
    date <- as.Date(date)
  }
  
  date_str <- format(date, format = "%Y-%m-%d")
  log_message(sprintf("Fetching schedule for %s", date_str))
  
  endpoint <- sprintf("/v1/schedule/%s", date_str)
  api_request(endpoint)
}

#' Parse games from schedule data
#' @param schedule_data Schedule data from API
#' @param date Date to parse games for
#' @return Data frame of games or NULL if no games
parse_daily_games <- function(schedule_data, date = Sys.Date()) {
  if (is.null(schedule_data)) return(NULL)
  
  # Ensure date is Date object
  if (is.character(date)) {
    date <- as.Date(date)
  }
  
  date_str <- format(date, format = "%Y-%m-%d")
  
  # Find games for the specified date
  date_index <- which(schedule_data$gameWeek$date == date_str)
  
  if (length(date_index) == 0 || is.null(schedule_data$gameWeek$games[[date_index]])) {
    log_message(sprintf("No games scheduled for %s", date_str))
    return(NULL)
  }
  
  games <- schedule_data$gameWeek$games[[date_index]]
  
  # Vectorized data extraction
  games_df <- data.frame(
    game_id = games$id,
    season = games$season,
    game_type = games$gameType,
    game_state = games$gameState,
    start_time_utc = format(games$startTimeUTC, format = "%Y-%m-%dT%H:%M:%SZ"),
    venue = games$venue$default,
    home_team = games$homeTeam$abbrev,
    away_team = games$awayTeam$abbrev,
    stringsAsFactors = FALSE
  )
  
  # Clean team names efficiently using vectorized operations
  games_df$home <- toupper(gsub("\\.|Utah ", "", 
                                paste(games$homeTeam$placeName$default, 
                                      games$homeTeam$commonName$default)))
  games_df$away <- toupper(gsub("\\.|Utah ", "", 
                                paste(games$awayTeam$placeName$default, 
                                      games$awayTeam$commonName$default)))
  
  # Handle special characters systematically
  games_df$home <- iconv(games_df$home, from = "UTF-8", to = "ASCII//TRANSLIT")
  games_df$away <- iconv(games_df$away, from = "UTF-8", to = "ASCII//TRANSLIT")
  
  # Parse dates and times
  games_df$game_date <- as.Date(games_df$start_time_utc)
  games_df$start_time_local <- format(
    as.POSIXct(games_df$start_time_utc, tz = "UTC"),
    format = "%I:%M %p",
    tz = Sys.timezone()
  )
  
  # Add game type descriptions
  game_type_map <- c(
    "1" = "Preseason",
    "2" = "Regular Season",
    "3" = "Playoffs",
    "4" = "Playoffs Round 2",
    "5" = "Conference Finals",
    "6" = "Stanley Cup Final"
  )
  games_df$game_type_desc <- game_type_map[as.character(games_df$game_type)]
  
  # Extract scores for live/completed games (vectorized)
  is_scored <- games$gameState %in% c("LIVE", "FINAL", "OFF")
  games_df$home_score <- ifelse(is_scored, games$homeTeam$score, NA)
  games_df$away_score <- ifelse(is_scored, games$awayTeam$score, NA)
  
  # Extract TV broadcast information
  games_df$tv_broadcasts <- extract_broadcasts(games)
  
  log_message(sprintf("Parsed %d games for %s", nrow(games_df), date_str))
  return(games_df)
}

#' Extract TV broadcast information from games data
#' @param games Games data from API
#' @return Character vector of broadcast information
extract_broadcasts <- function(games) {
  n_games <- length(games$id)
  broadcasts <- character(n_games)
  
  # Process each game's broadcasts
  for (i in seq_len(n_games)) {
    tryCatch({
      if (!is.null(games$tvBroadcasts[[i]]) && length(games$tvBroadcasts[[i]]) > 0) {
        tv_data <- games$tvBroadcasts[[i]]
        
        # Check for national broadcasts first
        if ("market" %in% names(tv_data) && "network" %in% names(tv_data)) {
          national_idx <- which(tv_data$market == "N")
          
          if (length(national_idx) > 0) {
            # National broadcast takes precedence
            broadcasts[i] <- paste(unique(tv_data$network[national_idx]), collapse = ", ")
          } else {
            # Collect home and away broadcasts
            broadcast_parts <- character()
            
            # Home broadcasts
            home_idx <- which(tv_data$market == "H")
            if (length(home_idx) > 0) {
              home_networks <- paste(unique(tv_data$network[home_idx]), collapse = ", ")
              broadcast_parts <- c(broadcast_parts, paste0("Home: ", home_networks))
            }
            
            # Away broadcasts
            away_idx <- which(tv_data$market == "A")
            if (length(away_idx) > 0) {
              away_networks <- paste(unique(tv_data$network[away_idx]), collapse = ", ")
              broadcast_parts <- c(broadcast_parts, paste0("Away: ", away_networks))
            }
            
            broadcasts[i] <- paste(broadcast_parts, collapse = "; ")
          }
        }
      }
    }, error = function(e) {
      # Log warning but continue processing
      log_message(sprintf("Error extracting broadcast for game %d: %s", i, e$message), "WARN")
    })
  }
  
  # Replace empty strings with NA
  broadcasts[broadcasts == ""] <- NA
  
  return(broadcasts)
}

#' Get team standings efficiently
#' @param team_abbrevs Vector of team abbreviations
#' @return Data frame with team records
get_team_records <- function(team_abbrevs) {
  if (is.null(team_abbrevs) || length(team_abbrevs) == 0) return(NULL)
  
  log_message("Fetching team standings")
  
  standings_data <- api_request("/v1/standings/now")
  if (is.null(standings_data)) {
    log_message("Failed to retrieve standings", "WARN")
    return(NULL)
  }
  
  # Extract all standings data at once
  all_teams <- data.frame(
    team = standings_data$standings$teamAbbrev$default,
    wins = standings_data$standings$wins,
    losses = standings_data$standings$losses,
    otLosses = standings_data$standings$otLosses,
    points = standings_data$standings$points,
    stringsAsFactors = FALSE
  )
  
  # Filter to only requested teams
  all_teams[all_teams$team %in% team_abbrevs, ]
}

#' Enhance games with team records
#' @param games_df Data frame of games
#' @return Enhanced data frame with team records
enhance_games_with_records <- function(games_df) {
  if (is.null(games_df) || nrow(games_df) == 0) return(games_df)
  
  # Get unique teams
  teams <- unique(c(games_df$home_team, games_df$away_team))
  records <- get_team_records(teams)
  
  if (is.null(records)) return(games_df)
  
  # Use single join operation with proper suffixes
  games_enhanced <- games_df %>%
    left_join(records, by = c("home_team" = "team"), suffix = c("", "_home")) %>%
    rename_with(~paste0("home_", .x), matches("^(wins|losses|otLosses|points)$")) %>%
    left_join(records, by = c("away_team" = "team"), suffix = c("", "_away")) %>%
    rename_with(~paste0("away_", .x), matches("^(wins|losses|otLosses|points)$"))
  
  # Create record strings
  games_enhanced <- games_enhanced %>%
    mutate(
      home_record = sprintf("%d-%d-%d", home_wins, home_losses, home_otLosses),
      away_record = sprintf("%d-%d-%d", away_wins, away_losses, away_otLosses)
    )
  
  return(games_enhanced)
}

# MAIN EXECUTION ----------------------------------------------------------

#' Retrieve NHL schedule for specified dates
#' @param dates Date or vector of dates to retrieve schedules for. 
#'              Can be Date objects or character strings in "YYYY-MM-DD" format.
#'              If NULL (default), retrieves today and tomorrow's schedules.
#' @return Data frame with all games for the specified dates (invisibly)
#' @examples
#' # Get today and tomorrow's games
#' retrieve_nhl_schedule()
#' 
#' # Get games for a specific date
#' retrieve_nhl_schedule("2025-06-01")
#' 
#' # Get games for multiple dates
#' retrieve_nhl_schedule(c("2025-06-01", "2025-06-02", "2025-06-03"))
#' 
#' # Using Date objects
#' retrieve_nhl_schedule(Sys.Date() + 0:7)  # Next week's games
retrieve_nhl_schedule <- function(dates = NULL) {
  # Default to today and tomorrow if no dates provided
  if (is.null(dates)) {
    dates <- c(Sys.Date(), Sys.Date() + 1)
    log_message("No dates specified, retrieving schedule for today and tomorrow")
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
  
  log_message(sprintf("Starting NHL schedule retrieval for %d date(s): %s", 
                      length(dates), 
                      paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")))
  
  # Initialize list to store games from all dates
  all_games_list <- list()
  
  # Keep track of dates we've already fetched data for
  fetched_weeks <- list()
  
  # Process each date
  for (i in seq_along(dates)) {
    current_date <- dates[i]
    
    # Check if we already have schedule data for this date
    schedule <- NULL
    for (week_data in fetched_weeks) {
      if (current_date %in% as.Date(week_data$gameWeek$date)) {
        schedule <- week_data
        log_message(sprintf("Using cached data for %s", format(current_date, format = "%Y-%m-%d")))
        break
      }
    }
    
    # If we don't have the data, fetch it
    if (is.null(schedule)) {
      schedule <- get_schedule_by_date(current_date)
      
      if (is.null(schedule)) {
        log_message(sprintf("Failed to retrieve schedule data for %s", 
                            format(current_date, format = "%Y-%m-%d")), "WARN")
        next
      }
      
      # Store this week's data for potential reuse
      fetched_weeks[[length(fetched_weeks) + 1]] <- schedule
    }
    
    # Parse games for this date
    games <- parse_daily_games(schedule, current_date)
    
    if (!is.null(games) && nrow(games) > 0) {
      all_games_list[[length(all_games_list) + 1]] <- games
    }
  }
  
  # Combine all games
  all_games <- bind_rows(all_games_list)
  
  if (is.null(all_games) || nrow(all_games) == 0) {
    log_message("No games found for any of the specified dates")
    return(invisible(NULL))
  }
  
  log_message(sprintf("Found %d total games across all dates", nrow(all_games)))
  
  # Enhance with standings
  enhanced_games <- enhance_games_with_records(all_games)
  
  # Log broadcast summary
  games_with_broadcasts <- sum(!is.na(enhanced_games$tv_broadcasts))
  if (games_with_broadcasts > 0) {
    log_message(sprintf("Found broadcast information for %d out of %d games", 
                        games_with_broadcasts, nrow(enhanced_games)))
  }
  
  # Create filename with date range
  if (length(dates) == 1) {
    date_suffix <- format(dates[1], format = "%Y-%m-%d")
  } else {
    date_suffix <- sprintf("%s_to_%s", 
                           format(min(dates), format = "%Y-%m-%d"),
                           format(max(dates), format = "%Y-%m-%d"))
  }
  
  filename <- sprintf("%s_nhl_games_%s.csv",
                      format(Sys.time(), format = "%Y%m%d_%H%M%S"),
                      date_suffix)
  output_path <- file.path(config$output_dir, filename)
  
  write.csv(enhanced_games, output_path, row.names = FALSE)
  log_message(sprintf("Schedule saved to: %s", output_path))
  
  # Return the data frame invisibly
  invisible(enhanced_games)
}

# # Run the script
# if (!interactive()) {
#   # When run as a script, use default behavior (today and tomorrow)
#   retrieve_nhl_schedule()
# }