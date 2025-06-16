# NHL SCHEDULE AND SCORES COLLECTOR ---------------------------------------
# Purpose: Efficiently collect NHL schedule and scores with minimal API calls
# Author: Professional implementation with data.table
# Last Updated: 2025-12-19

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
})

# Source utilities - use more robust path detection
if (!exists("nhl_config")) {
  # Try relative path first
  if (file.exists("utils/common.R")) {
    source("utils/common.R")
  } else if (file.exists("../utils/common.R")) {
    source("../utils/common.R")
  } else {
    stop("Cannot find common.R - please ensure working directory is correct")
  }
}

if (!exists("api_get")) {
  # Try relative path first
  if (file.exists("utils/api_client.R")) {
    source("utils/api_client.R")
  } else if (file.exists("../utils/api_client.R")) {
    source("../utils/api_client.R")
  } else {
    stop("Cannot find api_client.R - please ensure working directory is correct")
  }
}

# HELPER FUNCTIONS ---------------------------------------------------------

#' Parse games from NHL API response
#' @param api_data Raw API response
#' @param dates Dates to filter for
#' @return data.table of parsed games
parse_nhl_games <- function(api_data, dates) {
  if (is.null(api_data) || is.null(api_data$gameWeek)) {
    log_message("No game week data in API response", "WARN")
    return(data.table())
  }
  
  # Extract all games across all dates in the week
  all_games <- list()
  
  for (i in seq_along(api_data$gameWeek$date)) {
    date_str <- api_data$gameWeek$date[i]
    games_list <- api_data$gameWeek$games[[i]]
    
    if (is.null(games_list) || length(games_list) == 0) next
    
    # Convert to data.table efficiently
    games_dt <- as.data.table(games_list)
    
    if (nrow(games_dt) == 0) next
    
    # Extract nested team data efficiently
    games_dt[, `:=`(
      home_team = homeTeam.abbrev,
      away_team = awayTeam.abbrev,
      home_full = paste(homeTeam.placeName.default, homeTeam.commonName.default),
      away_full = paste(awayTeam.placeName.default, awayTeam.commonName.default),
      home_score = ifelse(gameState %in% c("LIVE", "FINAL", "OFF"), homeTeam.score, NA_integer_),
      away_score = ifelse(gameState %in% c("LIVE", "FINAL", "OFF"), awayTeam.score, NA_integer_),
      venue = venue.default,
      local_timezone = venueTimezone,
      broadcast = tvBroadcasts[[1]]$network[1]
    )]
    
    all_games[[length(all_games) + 1]] <- games_dt
  }
  
  if (length(all_games) == 0) {
    return(data.table())
  }
  
  # Combine all games
  combined <- rbindlist(all_games, fill = TRUE)
  
  # Parse dates and filter
  combined[, game_date_est := as.Date(format(as.POSIXct(startTimeUTC, tz = "UTC"), tz = "America/New_York"))]
  combined <- combined[game_date_est %in% dates]
  
  if (nrow(combined) == 0) {
    return(data.table())
  }
  
  # Select and rename columns efficiently
  result <- combined[, .(
    game_id = id,
    season = season,
    game_type = gameType,
    game_state = gameState,
    start_time_utc = format(as.POSIXct(startTimeUTC, tz = "UTC"), format = "%Y-%m-%dT%H:%M:%SZ"),
    venue = venue,
    home_team = home_team,
    away_team = away_team,
    home = standardize_team_name(home_full),
    away = standardize_team_name(away_full),
    game_date_est = game_date_est,
    start_time_est = format(as.POSIXct(startTimeUTC, tz = "UTC"), 
                              format = "%H:%M", 
                              tz = "America/New_York"),
    home_score = as.integer(home_score),
    away_score = as.integer(away_score),
    local_timezone = local_timezone,
    broadcast = broadcast
  )]
  
  return(result)
}

#' Get game type description
#' @param game_type Numeric game type code
#' @return Character description
get_game_type_desc <- function(game_type) {
  type_map <- c(
    "1" = "Preseason",
    "2" = "Regular Season", 
    "3" = "Playoffs",
    "4" = "Playoffs Round 2",
    "5" = "Conference Finals",
    "6" = "Stanley Cup Final"
  )
  
  return(type_map[as.character(game_type)])
}

# #' Parse TV broadcasts efficiently
# #' @param games_list Raw games list from API
# #' @return Character vector of broadcast info
# parse_tv_broadcasts <- function(games_list) {
#   if (is.null(games_list) || length(games_list) == 0) {
#     return(character(0))
#   }
  
#   broadcasts <- character(length(games_list))
  
#   for (i in seq_along(games_list)) {
#     tv_data <- games_list[[i]]$tvBroadcasts
    
#     if (is.null(tv_data) || length(tv_data) == 0) {
#       broadcasts[i] <- NA_character_
#       next
#     }
    
#     # Convert to data.table for efficient processing
#     tv_dt <- as.data.table(tv_data)
    
#     if (nrow(tv_dt) == 0 || !"market" %in% names(tv_dt) || !"network" %in% names(tv_dt)) {
#       broadcasts[i] <- NA_character_
#       next
#     }
    
#     # National broadcasts take precedence
#     national <- tv_dt[market == "N"]
#     if (nrow(national) > 0) {
#       broadcasts[i] <- paste(unique(national$network), collapse = ", ")
#     } else {
#       # Format home and away broadcasts
#       parts <- character()
      
#       home_bc <- tv_dt[market == "H"]
#       if (nrow(home_bc) > 0) {
#         parts <- c(parts, paste0("Home: ", paste(unique(home_bc$network), collapse = ", ")))
#       }
      
#       away_bc <- tv_dt[market == "A"]
#       if (nrow(away_bc) > 0) {
#         parts <- c(parts, paste0("Away: ", paste(unique(away_bc$network), collapse = ", ")))
#       }
      
#       broadcasts[i] <- if (length(parts) > 0) paste(parts, collapse = "; ") else NA_character_
#     }
#   }
  
#   return(broadcasts)
# }

#' Get team standings efficiently
#' @param team_abbrevs Vector of team abbreviations
#' @return data.table with team standings
get_team_standings <- function(team_abbrevs) {
  if (length(team_abbrevs) == 0) return(data.table())
  
  log_message("Fetching current standings", "DEBUG")
  
  standings_data <- api_get("/v1/standings/now", api_type = "nhl")
  
  if (is.null(standings_data) || is.null(standings_data$standings)) {
    log_message("Failed to retrieve standings", "WARN")
    return(data.table())
  }
  
  # Convert to data.table
  standings_dt <- as.data.table(standings_data$standings)
  
  # Extract team abbreviations properly
  standings_dt[, team := teamAbbrev.default]
  
  # Filter to requested teams and select columns
  result <- standings_dt[team %in% team_abbrevs, .(
    team = team,
    wins = wins,
    losses = losses,
    otLosses = otLosses,
    points = points
  )]
  
  return(result)
}

# MAIN COLLECTION FUNCTION -------------------------------------------------

#' Collect NHL schedule and scores data
#' @param dates Vector of dates to collect
#' @param include_broadcasts Whether to include TV broadcast info
#' @param include_standings Whether to include team standings
#' @return data.table with schedule and scores
collect_nhl_schedule <- function(dates, include_standings = TRUE) {
  log_message("Starting NHL schedule collection", "INFO")
  
  # Validate dates
  dates <- parse_date_args(dates)
  
  # Group dates by week to minimize API calls
  date_dt <- data.table(date = dates)
  date_dt[, week_start := floor_date(date, "week", week_start = 2)]
  weeks_needed <- sort(unique(date_dt$week_start))
  
  log_message(sprintf("Need to fetch %d week(s) of data for %d date(s)", 
                      length(weeks_needed), length(dates)), "INFO")
  
  # log_message(sprintf("Need to fetch %d week(s) of data for %d date(s)", 
  #                     length(weeks_needed), length(dates)), "INFO")
  
  # Collect data for each week
  all_games <- list()
  
  for (i in seq_along(weeks_needed)) {
    week_start <- weeks_needed[i]
    week_str <- format(week_start,  format = "%Y-%m-%d")
    log_message(sprintf("Fetching week starting %s", week_str), "DEBUG")
    
    # API call
    api_data <- api_get(sprintf("/v1/schedule/%s", week_str), api_type = "nhl")
    
    if (is.null(api_data)) {
      log_message(sprintf("Failed to fetch data for week %s", week_str), "WARN")
      next
    }
    
    # Parse games for this week
    week_games <- parse_nhl_games(api_data, dates)
    
    # # Add TV broadcasts if requested
    # if (include_broadcasts && nrow(week_games) > 0) {
    #   # Need to go back to raw data for broadcasts
    #   broadcasts <- character()
    #   for (i in seq_along(api_data$gameWeek$date)) {
    #     if (api_data$gameWeek$date[i] %in% format(dates, format = "%Y-%m-%d")) {
    #       games_list <- api_data$gameWeek$games[[i]]
    #       if (!is.null(games_list)) {
    #         broadcasts <- c(broadcasts, parse_tv_broadcasts(games_list))
    #       }
    #     }
    #   }
    #   if (length(broadcasts) == nrow(week_games)) {
    #     week_games[, tv_broadcasts := broadcasts]
    #   }
    # }
    
    if (nrow(week_games) > 0) {
      all_games[[length(all_games) + 1]] <- week_games
    }
  }
  
  # Combine all weeks
  if (length(all_games) == 0) {
    log_message("No games found for specified dates", "WARN")
    return(data.table())
  }
  
  games_dt <- rbindlist(all_games, fill = TRUE)
  
  # Add game type descriptions
  games_dt[, game_type_desc := get_game_type_desc(game_type)]
  
  # Add standings if requested
  if (include_standings && nrow(games_dt) > 0) {
    unique_teams <- unique(c(games_dt$home_team, games_dt$away_team))
    standings <- get_team_standings(unique_teams)
    
    if (nrow(standings) > 0) {
      # Merge standings for home teams
      games_dt <- merge(games_dt, standings, 
                        by.x = "home_team", by.y = "team", 
                        all.x = TRUE, suffixes = c("", "_home"))
      setnames(games_dt, 
               c("wins", "losses", "otLosses", "points"),
               c("home_wins", "home_losses", "home_otLosses", "home_points"))
      
      # Merge standings for away teams  
      games_dt <- merge(games_dt, standings,
                        by.x = "away_team", by.y = "team",
                        all.x = TRUE, suffixes = c("", "_away"))
      setnames(games_dt,
               c("wins", "losses", "otLosses", "points"),
               c("away_wins", "away_losses", "away_otLosses", "away_points"))
      
      # Create record strings
      games_dt[, `:=`(
        home_record = sprintf("%d-%d-%d", home_wins, home_losses, home_otLosses),
        away_record = sprintf("%d-%d-%d", away_wins, away_losses, away_otLosses)
      )]
    }
  }
  
  # Order columns logically
  setcolorder(games_dt, c(
    "game_id", "season", "game_type", "game_type_desc", "game_state",
    "game_date_est", "start_time_utc", "start_time_est", "venue",
    "home_team", "away_team", "home", "away",
    "home_score", "away_score"
  ))
  
  # Sort by date and game time
  setorder(games_dt, game_date_est, start_time_utc)
  
  log_message(sprintf("Retrieved %d games across %d dates", 
                      nrow(games_dt), length(unique(games_dt$game_date_est))), "SUCCESS")
  
  # Validate data quality
  validate_data_quality(games_dt, "schedule data")
  
  return(games_dt)
}
