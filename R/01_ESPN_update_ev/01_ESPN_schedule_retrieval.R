# RETRIEVE ESPN SPORTS SCHEDULE AND WIN PROBABILITIES ----------------------
# Purpose: Retrieve NFL, NBA, and MLB game schedules with win probabilities
# Author: [Your Name]
# Last Updated: 2025-06-04

# SETUP -------------------------------------------------------------------

# Suppress startup messages
suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(lubridate)
  library(stringr)
})

# CONFIGURATION -----------------------------------------------------------

config <- list(
  base_url = "http://sports.core.api.espn.com/v2/sports",
  max_retries = 3,
  retry_delay = 2,  # seconds
  timeout = 30,     # seconds
  base_data_dir = file.path(Sys.getenv("HOME"), "data"),
  log_file = "espn_schedule_retrieval.log",
  
  # Sport-specific configurations
  sports = list(
    NBA = list(
      sport = "basketball",
      league = "nba",
      has_predictor = TRUE
    ),
    NFL = list(
      sport = "football", 
      league = "nfl",
      has_predictor = TRUE
    ),
    MLB = list(
      sport = "baseball",
      league = "mlb",
      has_predictor = TRUE
    )
  )
)

# SPORT AND DIRECTORY VALIDATION ------------------------------------------

#' Create sport-specific output directory
#' @param sport_name Sport name (e.g., "NBA")
#' @return Full path to sport-specific schedule directory
create_sport_directory <- function(sport_name) {
  sport_dir <- file.path(config$base_data_dir, sport_name, "schedule")
  dir.create(sport_dir, showWarnings = FALSE, recursive = TRUE)
  return(sport_dir)
}

#' Validate and convert sport parameter
#' @param sport Sport name (NBA, NFL, MLB) - case insensitive
#' @return List with sport name, API configuration, and output directory
validate_sport <- function(sport) {
  if (is.null(sport) || is.na(sport) || sport == "") {
    stop("Sport parameter is required. Must be one of: ", paste(names(config$sports), collapse = ", "))
  }
  
  # Convert to uppercase for case-insensitive matching
  sport_upper <- toupper(sport)
  
  if (!sport_upper %in% names(config$sports)) {
    stop("Invalid sport '", sport, "'. Must be one of: ", paste(names(config$sports), collapse = ", "))
  }
  
  sport_config <- config$sports[[sport_upper]]
  output_dir <- create_sport_directory(sport_upper)
  
  log_message(sprintf("Using sport: %s (API: %s/%s)", sport_upper, sport_config$sport, sport_config$league), output_dir = output_dir)
  log_message(sprintf("Output directory: %s", output_dir), output_dir = output_dir)
  
  return(list(name = sport_upper, config = sport_config, output_dir = output_dir))
}

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

# API FUNCTIONS WITH RETRY LOGIC ------------------------------------------

#' Make API request with retry logic
#' @param url Full URL to request
#' @param max_retries Maximum number of retry attempts
#' @param output_dir Output directory for logging
#' @return Parsed JSON response or NULL on failure
api_request <- function(url, max_retries = config$max_retries, output_dir = NULL) {
  
  for (attempt in 1:max_retries) {
    tryCatch({
      response <- GET(
        url,
        timeout(config$timeout),
        add_headers("Accept" = "application/json")
      )
      
      if (status_code(response) == 200) {
        return(fromJSON(content(response, "text", encoding = "UTF-8")))
      } else if (status_code(response) %in% c(429, 500, 502, 503, 504)) {
        # Retryable errors
        if (attempt < max_retries) {
          log_message(sprintf("API request failed (attempt %d/%d): Status %d. Retrying...", 
                              attempt, max_retries, status_code(response)), "WARN", output_dir = output_dir)
          Sys.sleep(config$retry_delay * attempt)  # Exponential backoff
        }
      } else {
        # Non-retryable error
        log_message(sprintf("API request failed: Status %d", status_code(response)), "ERROR", output_dir = output_dir)
        return(NULL)
      }
    }, error = function(e) {
      log_message(sprintf("API request error (attempt %d/%d): %s", 
                          attempt, max_retries, e$message), "WARN", output_dir = output_dir)
      if (attempt < max_retries) {
        Sys.sleep(config$retry_delay * attempt)
      }
    })
  }
  
  log_message("API request failed after all retries", "ERROR", output_dir = output_dir)
  return(NULL)
}

# ESPN-SPECIFIC FUNCTIONS -------------------------------------------------

#' Get ESPN events for a specific date and sport
#' @param date Date in Date format or YYYY-MM-DD string
#' @param sport_info Sport information from validate_sport()
#' @return Events data or NULL on failure
get_espn_events <- function(date, sport_info) {
  # Format date for ESPN (YYYYMMDD)
  if (is.character(date)) {
    date <- as.Date(date)
  }
  date_str <- format(date, format = "%Y%m%d")
  
  # Build URL
  sport_config <- sport_info$config
  url <- sprintf("%s/%s/leagues/%s/events?lang=en&region=us&dates=%s",
                 config$base_url, sport_config$sport, sport_config$league, date_str)
  
  log_message(sprintf("Fetching %s events for %s", sport_info$name, format(date, format = "%Y-%m-%d")), 
              output_dir = sport_info$output_dir)
  
  api_request(url, output_dir = sport_info$output_dir)
}

#' Get win probabilities for a specific event
#' @param event_url Event URL from ESPN
#' @param sport_info Sport information from validate_sport()
#' @return List with predictor and odds data or NULL on failure
get_event_probabilities <- function(event_url, sport_info) {
  # Extract game ID from event URL
  game_id <- gsub('.*events/([0-9]+).*', '\\1', event_url)
  
  # Remove query parameters from event URL
  base_event_url <- gsub('\\?.*', '', event_url)
  
  result <- list()
  
  # Get predictor data (win probabilities)
  if (sport_info$config$has_predictor) {
    predictor_url <- sprintf("%s/competitions/%s/predictor?lang=en&region=us", 
                             base_event_url, game_id)
    result$predictor <- api_request(predictor_url, output_dir = sport_info$output_dir)
  }
  
  # Get odds data
  odds_url <- sprintf("%s/competitions/%s/odds?lang=en&region=us", 
                      base_event_url, game_id)
  result$odds <- api_request(odds_url, output_dir = sport_info$output_dir)
  
  return(result)
}

#' Parse events and extract game information
#' @param events_data Events data from API
#' @param date Date of the events
#' @param sport_info Sport information from validate_sport()
#' @return Data frame of games or NULL if no games
parse_espn_games <- function(events_data, date, sport_info) {
  if (is.null(events_data) || events_data$count == 0) {
    log_message(sprintf("No %s games scheduled for %s", sport_info$name, format(date, "%Y-%m-%d")),
                output_dir = sport_info$output_dir)
    return(NULL)
  }
  
  # Initialize list to store game data
  games_list <- list()
  
  # Process each event
  for (i in seq_along(events_data$items[["$ref"]])) {
    print(i)
    if(sport_info$name == "MLB"){
      event_url <- events_data[["items"]][["$ref"]][[i]][1]
    } else {
      event_url <- events_data[["items"]][["$ref"]][[i]]
    }
    
    # Get event details
    event_data <- api_request(event_url, output_dir = sport_info$output_dir)
    if (is.null(event_data)) next
    
    # Extract basic game info
    game_info <- data.frame(
      game_id = event_data$id,
      date = as.Date(date),
      start_time_utc = event_data$date,
      sport = sport_info$name,
      name = event_data$name,
      short_name = event_data$shortName,
      stringsAsFactors = FALSE
    )
    
    # Extract home and away teams from short name (short codes)
    if (grepl(" @ ", game_info$short_name)) {
      teams <- strsplit(game_info$short_name, " @ ")[[1]]
      game_info$away_team <- trimws(teams[1])
      game_info$home_team <- trimws(teams[2])
    }
    
    # Extract full team names from game name
    if (grepl(" vs\\. ", game_info$name)) {
      full_teams <- strsplit(game_info$name, " vs\\. ")[[1]]
      game_info$away_team_full <- trimws(full_teams[1])
      game_info$home_team_full <- trimws(full_teams[2])
    } else if (grepl(" at ", game_info$name)) {
      full_teams <- strsplit(game_info$name, " at ")[[1]]
      game_info$away_team_full <- trimws(full_teams[1])
      game_info$home_team_full <- trimws(full_teams[2])
    }
    
    # Get competition details
    if (!is.null(event_data$competitions) && length(event_data$competitions) > 0) {
      comp <- event_data$competitions
      
      # Extract venue
      if (!is.null(comp$venue)) {
        venue_data <- api_request(comp$venue$`$ref`, output_dir = sport_info$output_dir)
        if (!is.null(venue_data)) {
          game_info$venue <- venue_data$fullName
          game_info$venue_city <- venue_data$address$city
          game_info$venue_state <- venue_data$address$state
        }
      }
      
      # Extract competitors
      if (!is.null(comp$competitors) && length(comp$competitors) >= 2) {
        for (j in seq_along(comp$competitors)) {
          competitor <- comp$competitors[[j]]
          if (competitor$homeAway == "home") {
            game_info$home_team_id <- competitor$id
          } else {
            game_info$away_team_id <- competitor$id
          }
        }
      }
    }
    
    # Get win probabilities
    prob_data <- get_event_probabilities(event_url, sport_info)
    
    if (!is.null(prob_data$predictor)) {
      # Try to extract statistics safely
      tryCatch({
        stats_vec <- setNames(prob_data$predictor$awayTeam$statistics$value, 
                              c("away_wp", "matchup_quality", "home_wp", "pred_pt_diff", 
                                "away_exp_pts", "home_exp_pts"))
        
        # Extract win probabilities
        if (!is.null(stats_vec["home_wp"])) {
          game_info$home_win_prob <- stats_vec["home_wp"]
        }
        if (!is.null(stats_vec["away_wp"])) {
          game_info$away_win_prob <- stats_vec["away_wp"]
        }
        
        # Extract predicted scores if available
        if (!is.null(stats_vec["home_exp_pts"])) {
          game_info$home_pred_score <- stats_vec["home_exp_pts"]
        }
        if (!is.null(stats_vec["away_exp_pts"])) {
          game_info$away_pred_score <- stats_vec["away_exp_pts"]
        }
      }, error = function(e) {
        log_message(sprintf("Error extracting predictor data for game %s: %s", 
                            game_info$game_id, e$message), "WARN", output_dir = sport_info$output_dir)
      })
    }
    
    # Extract odds information if available
    if (!is.null(prob_data$odds) && !is.null(prob_data$odds$items)) {
      tryCatch({
        # Get the first odds provider (usually consensus or most reliable)
        if (length(prob_data$odds$items) > 0) {
          odds_item <- prob_data$odds$items
          
          # Get provider info
          if (!is.null(odds_item$provider)) {
            if(sport_info$name == "NBA"){
              provider_data <- odds_item$provider[-1]
            } else {
              provider_data <- odds_item$provider
            }
            # provider_data <- api_request(odds_item$provider$`$ref`, output_dir = sport_info$output_dir)
            if (!is.null(provider_data)) {
              game_info$odds_provider <- provider_data$name
            }
          }
          
          # Extract spread and over/under if available
          if (!is.null(odds_item$spread)) {
            game_info$spread <- odds_item$spread
          }
          if (!is.null(odds_item$overUnder)) {
            game_info$over_under <- odds_item$overUnder
          }
        }
      }, error = function(e) {
        log_message(sprintf("Error extracting odds data for game %s: %s", 
                            game_info$game_id, e$message), "WARN", output_dir = sport_info$output_dir)
      })
    }
    
    games_list[[i]] <- game_info
  }
  
  # Combine all games
  games_df <- bind_rows(games_list)
  
  if (nrow(games_df) == 0) {
    return(NULL)
  }
  
  # Parse and format times
  games_df$game_date <- as.Date(games_df$start_time_utc)
  games_df$start_time_local <- format(
    as.POSIXct(games_df$start_time_utc, tz = "UTC"),
    format = "%I:%M %p",
    tz = Sys.timezone()
  )
  
  log_message(sprintf("Parsed %d %s games for %s", nrow(games_df), sport_info$name, format(date, "%Y-%m-%d")),
              output_dir = sport_info$output_dir)
  
  return(games_df)
}

# MAIN EXECUTION FUNCTION -------------------------------------------------

#' Retrieve ESPN schedule and win probabilities for specified dates
#' @param dates Date or vector of dates to retrieve schedules for. 
#'              Can be Date objects or character strings in "YYYY-MM-DD" format.
#'              If NULL (default), retrieves today and tomorrow's schedules.
#' @param sport Sport to retrieve (NBA, NFL, MLB). Default is NBA.
#' @return Data frame with all games and win probabilities (invisibly)
#' @examples
#' # Get today and tomorrow's NBA games
#' retrieve_espn_schedule()
#' 
#' # Get NFL games for a specific date
#' retrieve_espn_schedule("2025-06-01", sport = "NFL")
#' 
#' # Get MLB games for multiple dates
#' retrieve_espn_schedule(c("2025-06-01", "2025-06-02"), sport = "MLB")
retrieve_espn_schedule <- function(dates = NULL, sport = "NBA") {
  # Validate sport parameter
  sport_info <- validate_sport(sport)
  sport_name <- sport_info$name
  output_dir <- sport_info$output_dir
  
  # Default to today and tomorrow if no dates provided
  if (is.null(dates)) {
    dates <- c(Sys.Date(), Sys.Date() + 1)
    log_message(sprintf("No dates specified, retrieving %s schedule for today and tomorrow", sport_name),
                output_dir = output_dir)
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
  
  log_message(sprintf("Starting %s schedule retrieval for %d date(s): %s", 
                      sport_name,
                      length(dates), 
                      paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")),
              output_dir = output_dir)
  
  # Initialize list to store games from all dates
  all_games_list <- list()
  
  # Process each date
  for (i in seq_along(dates)) {
    current_date <- dates[i]
    
    # Get events for this date
    events <- get_espn_events(current_date, sport_info)
    
    if (is.null(events)) {
      log_message(sprintf("Failed to retrieve %s events for %s", 
                          sport_name, format(current_date, "%Y-%m-%d")), "WARN",
                  output_dir = output_dir)
      next
    }
    
    # Parse games for this date
    games <- parse_espn_games(events, current_date, sport_info)
    
    if (!is.null(games) && nrow(games) > 0) {
      all_games_list[[length(all_games_list) + 1]] <- games
    }
  }
  
  # Combine all games
  all_games <- bind_rows(all_games_list)
  
  if (is.null(all_games) || nrow(all_games) == 0) {
    log_message(sprintf("No %s games found for any of the specified dates", sport_name),
                output_dir = output_dir)
    return(invisible(NULL))
  }
  
  log_message(sprintf("Found %d total %s games across all dates", nrow(all_games), sport_name),
              output_dir = output_dir)
  
  # Sort by date and start time
  all_games <- all_games %>%
    arrange(game_date, start_time_utc)
  
  # Create summary of games with probabilities
  games_with_probs <- sum(!is.na(all_games$home_win_prob) | !is.na(all_games$away_win_prob))
  if (games_with_probs > 0) {
    log_message(sprintf("Found win probabilities for %d out of %d games", 
                        games_with_probs, nrow(all_games)),
                output_dir = output_dir)
  }
  
  # Create filename with date range and sport
  if (length(dates) == 1) {
    date_suffix <- format(dates[1], format = "%Y-%m-%d")
  } else {
    date_suffix <- sprintf("%s_to_%s", 
                           format(min(dates), format = "%Y-%m-%d"),
                           format(max(dates), format = "%Y-%m-%d"))
  }
  
  filename <- sprintf("%s_%s_games_%s.csv",
                      format(Sys.time(), format = "%Y%m%d_%H%M%S"),
                      tolower(sport_name),
                      date_suffix)
  output_path <- file.path(output_dir, filename)
  
  write.csv(all_games, output_path, row.names = FALSE)
  log_message(sprintf("%s schedule saved to: %s", sport_name, output_path),
              output_dir = output_dir)
  
  # Return the data frame invisibly
  invisible(all_games)
}

# UTILITY FUNCTIONS -------------------------------------------------------

#' Display schedule summary
#' @param schedule_df Data frame returned by retrieve_espn_schedule
display_schedule_summary <- function(schedule_df) {
  if (is.null(schedule_df) || nrow(schedule_df) == 0) {
    cat("No games to display\n")
    return()
  }
  
  cat("\n", "="*60, "\n", sep = "")
  cat(sprintf("SCHEDULE SUMMARY: %s\n", unique(schedule_df$sport)))
  cat("="*60, "\n\n", sep = "")
  
  # Group by date
  by_date <- split(schedule_df, schedule_df$game_date)
  
  for (date in names(by_date)) {
    games <- by_date[[date]]
    cat(sprintf("Date: %s (%d games)\n", date, nrow(games)))
    cat("-"*40, "\n", sep = "")
    
    for (i in seq_len(nrow(games))) {
      game <- games[i, ]
      cat(sprintf("  %s: %s @ %s",
                  game$start_time_local,
                  game$away_team,
                  game$home_team))
      
      # Add win probabilities if available
      if (!is.na(game$away_win_prob) && !is.na(game$home_win_prob)) {
        cat(sprintf(" (%.1f%% vs %.1f%%)", 
                    game$away_win_prob, 
                    game$home_win_prob))
      }
      
      # Add spread if available
      if (!is.na(game$spread)) {
        cat(sprintf(" [%+.1f]", game$spread))
      }
      
      cat("\n")
    }
    cat("\n")
  }
}

# CONVENIENCE WRAPPER FUNCTIONS -------------------------------------------

#' Retrieve NBA schedule
#' @param dates Dates to process (NULL for today and tomorrow)
#' @return Data frame with NBA schedule (invisibly)
retrieve_nba_schedule <- function(dates = NULL) {
  retrieve_espn_schedule(dates = dates, sport = "NBA")
}

#' Retrieve NFL schedule
#' @param dates Dates to process (NULL for today and tomorrow)
#' @return Data frame with NFL schedule (invisibly)
retrieve_nfl_schedule <- function(dates = NULL) {
  retrieve_espn_schedule(dates = dates, sport = "NFL")
}

#' Retrieve MLB schedule
#' @param dates Dates to process (NULL for today and tomorrow)
#' @return Data frame with MLB schedule (invisibly)
retrieve_mlb_schedule <- function(dates = NULL) {
  retrieve_espn_schedule(dates = dates, sport = "MLB")
}

# RUN SCRIPT ---------------------------------------------------------------

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
        if (toupper(arg) %in% names(config$sports)) {
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
    retrieve_espn_schedule(dates = dates, sport = sport)
  }, error = function(e) {
    message(sprintf("Fatal error: %s", e$message))
    quit(status = 1)
  })
}

# Run examples if script is sourced interactively
if (interactive()) {
  cat("ESPN Schedule Retrieval Script loaded.\n")
  cat("Examples:\n")
  cat("  # Get today and tomorrow's NBA games:\n")
  cat("  nba_games <- retrieve_espn_schedule()\n\n")
  cat("  # Get NFL games for next week:\n")
  cat("  nfl_games <- retrieve_espn_schedule(Sys.Date() + 0:7, sport = 'NFL')\n\n")
  cat("  # Display schedule summary:\n")
  cat("  display_schedule_summary(nba_games)\n")
}