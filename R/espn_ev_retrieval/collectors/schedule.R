# ESPN SCHEDULE AND SCORES COLLECTOR --------------------------------------
# Purpose: Efficiently collect ESPN schedule, scores, and win probabilities
# Author: Professional implementation with data.table
# Last Updated: 2025-06-21

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(here)
})

# Source utilities using here()
source(here("R", "espn_ev_retrieval", "utils", "common.R"))
source(here("R", "espn_ev_retrieval", "utils", "api_client.R"))

# SPORT CONFIGURATION ------------------------------------------------------

sport_configs <- list(
  NBA = list(
    sport = "basketball",
    league = "nba",
    season_type = 2  # Regular season
  ),
  NFL = list(
    sport = "football", 
    league = "nfl",
    season_type = 2  # Regular season
  ),
  MLB = list(
    sport = "baseball",
    league = "mlb",
    season_type = 2  # Regular season
  )
)

# HELPER FUNCTIONS ---------------------------------------------------------

#' Build ESPN events endpoint URL
#' @param date Date to query
#' @param sport_config Sport configuration
#' @return Character URL endpoint
build_events_endpoint <- function(date, sport_config) {
  date_str <- format(as.Date(date), format = "%Y%m%d")
  sprintf("/%s/leagues/%s/events?dates=%s&limit=100",
          sport_config$sport, sport_config$league, date_str)
}

#' Extract starter information from competitors
#' @param competitors List of competitor data
#' @param sport Sport name
#' @return List with home_starter and away_starter
extract_starters <- function(competitors, sport) {
  starters <- list(home_starter = NA_character_, away_starter = NA_character_)

  if (is.null(competitors) || length(competitors) < 2) {
    return(starters)
  }
  
  for (comp in competitors) {
    if (is.null(comp$probables)) next
    
    # Extract based on sport
    starter_name <- NA_character_
    
    if (sport == "MLB" && !is.null(comp$probables$playerId)) {
      # MLB: Get probable pitcher
      starter_name <- comp$probables$fullName[1]
    } else if (sport == "NFL" && !is.null(comp$leaders)) {
      # NFL: Get QB from leaders
      qb_leader <- comp$leaders[comp$leaders$name == "passingLeader", ]
      if (nrow(qb_leader) > 0) {
        starter_name <- qb_leader$athlete$displayName[1]
      }
    } else if (sport == "NBA" && !is.null(comp$leaders)) {
      # NBA: Get points leader as star player
      pts_leader <- comp$leaders[comp$leaders$name == "pointsLeader", ]
      if (nrow(pts_leader) > 0) {
        starter_name <- pts_leader$athlete$displayName[1]
      }
    }
    
    # Assign to correct team
    if (comp$homeAway == "home") {
      starters$home_starter <- starter_name
    } else {
      starters$away_starter <- starter_name
    }
  }
  
  return(starters)
}

#' Convert a data.frame of nested resources to a list of row-wise lists
#' @param x object that may be a data.frame or list
#' @return list of row-wise lists or the original object if not a data.frame
df_to_row_list <- function(x) {
  if (!is.data.frame(x)) return(x)
  lapply(seq_len(nrow(x)), function(i) {
    row <- x[i, , drop = FALSE]
    lapply(row, function(col) {
      if (is.data.frame(col) && nrow(col) == 1) as.list(col[1, , drop = TRUE]) else col
    })
  })
}

#' Safely fetch a team's display name
#' @param team_obj team object which may only contain a `$ref`
#' @return character display name or NA
get_team_name <- function(team_obj) {
  if (is.null(team_obj)) return(NA_character_)
  if (!is.null(team_obj$displayName)) return(team_obj$displayName)
  if (!is.null(team_obj$`$ref`)) {
    team_data <- api_get(team_obj$`$ref`, api_type = "espn", use_cache = TRUE)
    if (!is.null(team_data$displayName)) return(team_data$displayName)
  }
  NA_character_
}

#' Safely fetch score value if provided via reference
#' @param score_obj score object or numeric value
#' @return integer score or NA
get_score_value <- function(score_obj) {
  if (is.null(score_obj)) return(NA_integer_)
  if (is.numeric(score_obj)) return(as.integer(score_obj))
  if (is.list(score_obj) && !is.null(score_obj$value)) return(as.integer(score_obj$value))
  if (is.list(score_obj) && !is.null(score_obj$`$ref`)) {
    score_data <- api_get(score_obj$`$ref`, api_type = "espn", use_cache = TRUE)
    if (!is.null(score_data$value)) return(as.integer(score_data$value))
  }
  NA_integer_
}

#' Parse ESPN events response
#' @param events_data Raw events API response
#' @param sport Sport name
#' @return data.table of parsed events
parse_espn_events <- function(events_data, sport) {
  if (is.null(events_data) || is.null(events_data$items) || length(events_data$items) == 0) {
    log_message("No events found in response", "DEBUG")
    return(data.table())
  }

  # `events_data$items` can come back either as a list of items or a data.frame
  # (when there is only a single event).  Handle both representations and
  # extract the `$ref` links cleanly.
  items <- events_data$items
  if (is.data.frame(items)) {
    event_refs <- items$`$ref`
  } else if (is.list(items)) {
    event_refs <- vapply(items, function(x) x$`$ref`, character(1))
  } else {
    log_message("Unknown events structure returned by API", "WARN")
    return(data.table())
  }

  log_message(sprintf("Processing %d events", length(event_refs)), "DEBUG")

  games_list <- list()

  for (event_ref in event_refs) {
    
    if (is.null(event_ref)) next
    
    # Fetch detailed event data
    event_data <- api_get(event_ref, api_type = "espn", use_cache = TRUE)
    
    if (is.null(event_data) || is.null(event_data$competitions)) next
    
    comp <- event_data$competitions
    if (is.data.frame(comp)) {
      comp <- as.list(comp[1, , drop = TRUE])
    } else if (is.list(comp)) {
      comp <- comp[[1]]
    }

    competitors <- df_to_row_list(comp$competitors)
    if (is.null(competitors) || length(competitors) < 2) next

    home_idx <- which(vapply(competitors, function(x) x$homeAway, character(1)) == "home")
    away_idx <- which(vapply(competitors, function(x) x$homeAway, character(1)) == "away")

    if (length(home_idx) == 0 || length(away_idx) == 0) next

    home <- competitors[[home_idx[1]]]
    away <- competitors[[away_idx[1]]]
    
    # Extract starters
    starters <- extract_starters(comp$competitors, sport)
    
    # Parse game data
    game_dt <- data.table(
      game_id = as.character(event_data$id),
      sport = sport,
      game_date = as.Date(event_data$date),
      game_time = format(as.POSIXct(event_data$date, tz = "UTC"), format = "%H:%M", tz = "America/New_York"),
      home_team = standardize_team_name(get_team_name(home$team)),
      away_team = standardize_team_name(get_team_name(away$team)),
      home_score = NA_integer_,
      away_score = NA_integer_,
      game_state = if (!is.null(comp$status$type$name)) comp$status$type$name else NA_character_,
      venue = ifelse(!is.null(comp$venue$fullName), comp$venue$fullName, NA_character_),
      broadcast = NA_character_,
      home_win_prob = NA_real_,
      away_win_prob = NA_real_,
      home_starter = starters$home_starter,
      away_starter = starters$away_starter
    )
    
    # Extract scores for completed games
    if (!is.null(comp$status$type$completed) && isTRUE(comp$status$type$completed)) {
      game_dt$home_score <- get_score_value(home$score)
      game_dt$away_score <- get_score_value(away$score)
    }
    
    # Extract broadcast info
    if (!is.null(comp$broadcasts)) {
      broadcasts <- df_to_row_list(comp$broadcasts)
      if (length(broadcasts) > 0) {
        names <- vapply(broadcasts, function(x) x$media$shortName, character(1), USE.NAMES = FALSE)
        game_dt$broadcast <- paste(names, collapse = ", ")
      }
    }
    
    # Extract win probabilities from predictor
    if (!is.null(event_data$predictor)) {
      predictor <- event_data$predictor
      if (!is.null(predictor$`$ref`)) {
        predictor <- api_get(predictor$`$ref`, api_type = "espn", use_cache = TRUE)
      }
      if (!is.null(predictor$homeTeam$gameProjection)) {
        game_dt$home_win_prob <- predictor$homeTeam$gameProjection / 100
      }
      if (!is.null(predictor$awayTeam$gameProjection)) {
        game_dt$away_win_prob <- predictor$awayTeam$gameProjection / 100
      }
    }
    
    games_list[[length(games_list) + 1]] <- game_dt
  }
  
  if (length(games_list) == 0) {
    return(data.table())
  }
  
  # Combine all games
  games_dt <- rbindlist(games_list, fill = TRUE)
  
  # Validate probabilities
  games_dt[home_win_prob + away_win_prob > 0, `:=`(
    prob_sum = home_win_prob + away_win_prob
  )]
  
  # Normalize if needed
  games_dt[!is.na(prob_sum) & abs(prob_sum - 1) > 0.01, `:=`(
    home_win_prob = home_win_prob / prob_sum,
    away_win_prob = away_win_prob / prob_sum
  )]
  
  games_dt[, prob_sum := NULL]
  
  return(games_dt)
}

# MAIN COLLECTION FUNCTION -------------------------------------------------

#' Collect ESPN schedule and scores data
#' @param dates Vector of dates to collect
#' @param sport Sport name ("NBA", "NFL", "MLB")
#' @return data.table with schedule and scores
collect_espn_schedule <- function(dates, sport = "NBA") {
  log_message(sprintf("Starting %s schedule collection", sport), "INFO")
  
  # Validate sport
  sport <- toupper(sport)
  if (!sport %in% names(sport_configs)) {
    log_message(sprintf("Invalid sport: %s", sport), "ERROR")
    return(data.table())
  }
  
  sport_config <- sport_configs[[sport]]
  
  # Validate dates
  dates <- parse_date_args(dates)
  
  log_message(sprintf("Collecting %s schedule for %d dates", sport, length(dates)), "INFO")
  
  # Collect data for each date
  all_games <- list()
  
  for (date in dates) {
    date_str <- format(date, format = "%Y-%m-%d")
    log_message(sprintf("Fetching %s games for %s", sport, date_str), "DEBUG")
    
    # Build endpoint
    endpoint <- build_events_endpoint(date, sport_config)
    
    # API call
    events_data <- api_get(endpoint, api_type = "espn", use_cache = TRUE)
    
    if (is.null(events_data)) {
      log_message(sprintf("Failed to retrieve data for %s", date_str), "WARN")
      next
    }
    
    # Parse events
    games_dt <- parse_espn_events(events_data, sport)
    
    if (nrow(games_dt) > 0) {
      all_games[[length(all_games) + 1]] <- games_dt
      log_message(sprintf("Found %d games on %s", nrow(games_dt), date_str), "DEBUG")
    }
  }
  
  if (length(all_games) == 0) {
    log_message("No games found for any date", "WARN")
    return(data.table())
  }
  
  # Combine all results
  schedule_dt <- rbindlist(all_games, fill = TRUE)
  
  # Sort by date and time
  setorder(schedule_dt, game_date, game_time)
  
  # Log summary
  log_message(sprintf("Retrieved %d %s games across %d dates", 
                      nrow(schedule_dt), sport, length(dates)), "SUCCESS")
  
  # Log games with probabilities
  games_with_prob <- schedule_dt[!is.na(home_win_prob) | !is.na(away_win_prob)]
  log_message(sprintf("Found win probabilities for %d games (%.1f%%)", 
                      nrow(games_with_prob),
                      100 * nrow(games_with_prob) / nrow(schedule_dt)), "INFO")
  
  # Validate data quality
  validate_schedule_data(schedule_dt, sport)
  
  return(schedule_dt)
}

#' Validate schedule data
#' @param dt data.table with schedule data
#' @param sport Sport name
validate_schedule_data <- function(dt, sport) {
  # Check for duplicate games
  dups <- dt[duplicated(dt[, .(game_date, home_team, away_team)])]
  if (nrow(dups) > 0) {
    log_message(sprintf("Found %d duplicate games", nrow(dups)), "WARN")
  }
  
  # Check probability completeness
  missing_prob <- dt[is.na(home_win_prob) & is.na(away_win_prob)]
  if (nrow(missing_prob) > 0) {
    log_message(sprintf("%d games missing win probabilities", nrow(missing_prob)), "DEBUG")
  }
  
  # Sport-specific validation
  if (sport == "MLB") {
    missing_pitchers <- dt[is.na(home_starter) | is.na(away_starter)]
    if (nrow(missing_pitchers) > 0) {
      log_message(sprintf("%d games missing probable pitchers", nrow(missing_pitchers)), "DEBUG")
    }
  } else if (sport == "NFL") {
    missing_qbs <- dt[is.na(home_starter) | is.na(away_starter)]
    if (nrow(missing_qbs) > 0) {
      log_message(sprintf("%d games missing starting QBs", nrow(missing_qbs)), "DEBUG")
    }
  }
}
