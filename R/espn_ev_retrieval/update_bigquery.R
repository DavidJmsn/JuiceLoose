# UPDATE BIGQUERY DATABASE FOR ESPN ----------------------------------------
# Purpose: Efficiently update ESPN data in BigQuery using Arrow for multi-file
#          datasets and data.table for processing
# Author: Professional implementation with Arrow and data.table
# Last Updated: 2025-06-21

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(bigrquery)
  library(DBI)
  library(gargle)
  library(lubridate)
  library(dplyr)
  library(here)
})

# Source utilities using here()
source(here("R", "espn_ev_retrieval", "utils", "common.R"))

# CONFIGURATION ------------------------------------------------------------

bq_config <- list(
  # BigQuery settings
  project_id = Sys.getenv("PROJECT_ID"),
  dataset = Sys.getenv("DATASET"),
  service_json = Sys.getenv("SERVICE_ACCOUNT"),
  
  # Sport-specific table names
  nba_completed_table = "nba_completed_games",
  nba_upcoming_table = "nba_upcoming_games",
  nfl_completed_table = "nfl_completed_games", 
  nfl_upcoming_table = "nfl_upcoming_games",
  mlb_completed_table = "mlb_completed_games",
  mlb_upcoming_table = "mlb_upcoming_games",
  
  # Data paths (for multi-file datasets)
  ev_path = file.path(espn_config$data_dir, "expected_value"),
  scores_path = file.path(espn_config$data_dir, "scores"),
  rds_path = file.path(espn_config$data_dir, "rds_files"),
  
  # Arrow configuration
  use_arrow_dataset = TRUE,
  batch_size = 10000  # For reading Arrow datasets in batches
)

# ARROW SCHEMAS ------------------------------------------------------------

#' Define Arrow schema for expected values dataset
#' @param sport Sport name for player column
#' @return Arrow schema
get_ev_schema <- function(sport = "NBA") {
  # Determine player column name
  player_col <- switch(sport,
    NFL = "QB",
    MLB = "pitcher",
    NBA = "star_player",
    "star_player"  # default
  )
  
  # Create base fields
  fields <- list(
    game_id = string(),
    team = string(),
    home = string(),
    away = string(),
    game_date_est = date32(),
    game_time_est = string(),
    venue = string(),
    broadcast = string(),
    game_state = string(),
    home_or_away = string(),
    book = string(),
    price = float64(),
    win_probability = float64(),
    expected_value = float64(),
    kelly_criterion = float64(),
    implied_win_prob = float64(),
    price_mean = float64(),
    price_sd = float64(),
    n_books = int64(),
    team_score = int64(),
    opponent_score = int64(),
    retrieval_time = string()
  )
  
  # Add player column with status
  fields[[player_col]] <- string()
  fields[[paste0(player_col, "_status")]] <- string()
  
  # Create schema
  do.call(schema, fields)
}

#' Define Arrow schema for scores dataset
#' @return Arrow schema
get_scores_schema <- function() {
  schema(
    game_id = string(),
    sport = string(),
    game_date = date32(),
    game_time = string(),
    home_team = string(),
    away_team = string(),
    home_score = int64(),
    away_score = int64(),
    game_state = string(),
    venue = string(),
    broadcast = string(),
    home_win_prob = float64(),
    away_win_prob = float64(),
    home_starter = string(),
    away_starter = string()
  )
}

# BIGQUERY FUNCTIONS -------------------------------------------------------

#' Authenticate with BigQuery
#' @return TRUE on success, FALSE on failure
authenticate_bigquery <- function() {
  tryCatch({
    # Deauth first
    bigrquery::bq_deauth()
    
    # Check service account
    if (is.null(bq_config$service_json) || bq_config$service_json == "") {
      log_message("SERVICE_ACCOUNT environment variable not set", "ERROR")
      return(FALSE)
    }
    
    # Create token
    token <- gargle::credentials_service_account(
      path = bq_config$service_json,
      scopes = c(
        "https://www.googleapis.com/auth/userinfo.email",
        "https://www.googleapis.com/auth/bigquery"
      )
    )
    
    # Authenticate
    bigrquery::bq_auth(token = token)
    
    log_message("BigQuery authenticated successfully", "SUCCESS")
    return(TRUE)
    
  }, error = function(e) {
    log_message(sprintf("BigQuery authentication failed: %s", e$message), "ERROR")
    return(FALSE)
  })
}

#' Get existing data from BigQuery
#' @param table_name Table name
#' @return data.table or NULL
get_existing_data <- function(table_name) {
  tryCatch({
    query <- sprintf("SELECT * FROM `%s.%s.%s`",
                     bq_config$project_id,
                     bq_config$dataset,
                     table_name)
    
    result <- bq_project_query(bq_config$project_id, query) %>%
      bq_table_download()
    
    setDT(result)
    log_message(sprintf("Retrieved %d rows from %s", nrow(result), table_name), "INFO")
    
    return(result)
    
  }, error = function(e) {
    log_message(sprintf("Failed to retrieve data from %s: %s", 
                        table_name, e$message), "WARN")
    return(data.table())
  })
}

# DATA LOADING FUNCTIONS ---------------------------------------------------

#' Load EV dataset for a specific sport
#' @param sport Sport name
#' @return data.table or NULL
load_ev_dataset_sport <- function(sport) {
  sport_lower <- tolower(sport)
  
  tryCatch({
    # Get sport-specific CSV files
    ev_files <- list.files(bq_config$ev_path, 
                          pattern = sprintf("*_%s_expected_values.csv$", sport_lower),
                          full.names = TRUE)
    
    if (length(ev_files) == 0) {
      log_message(sprintf("No %s EV files found", sport), "WARN")
      return(NULL)
    }
    
    log_message(sprintf("Found %d %s EV files to process", length(ev_files), sport), "DEBUG")
    
    # Read all files and combine
    all_data <- list()
    
    for (file in ev_files) {
      dt <- fread(file)
      all_data[[length(all_data) + 1]] <- dt
    }
    
    # Combine all data
    ev_dt <- rbindlist(all_data, fill = TRUE)
    
    # Ensure proper date/time columns
    if ("game_date_est" %in% names(ev_dt) && is.character(ev_dt$game_date_est)) {
      ev_dt[, game_date_est := as.Date(game_date_est)]
    }
    
    if ("game_time_est" %in% names(ev_dt) && is.character(ev_dt$game_time_est)) {
      ev_dt[, game_time_est := as.character(as.POSIXct(game_time_est))]
    }
    
    if ("retrieval_time" %in% names(ev_dt) && is.character(ev_dt$retrieval_time)) {
      ev_dt[, retrieval_time := as.character(as.POSIXct(retrieval_time))]
    }
    
    log_message(sprintf("Loaded %d %s EV records", nrow(ev_dt), sport), "SUCCESS")
    
    return(ev_dt)
    
  }, error = function(e) {
    log_message(sprintf("Failed to load %s EV dataset: %s", sport, e$message), "ERROR")
    return(NULL)
  })
}

#' Load scores dataset for a specific sport
#' @param sport Sport name
#' @return data.table or NULL
load_scores_dataset_sport <- function(sport) {
  sport_lower <- tolower(sport)
  
  tryCatch({
    # Get sport-specific score files
    scores_files <- list.files(bq_config$scores_path,
                              pattern = sprintf("*_%s_scores.csv$", sport_lower),
                              full.names = TRUE)
    
    if (length(scores_files) == 0) {
      log_message(sprintf("No %s scores files found", sport), "WARN")
      return(data.table())
    }
    
    log_message(sprintf("Found %d %s scores files to process", length(scores_files), sport), "DEBUG")
    
    # Read all files and combine
    all_scores <- list()
    
    for (file in scores_files) {
      dt <- fread(file)
      all_scores[[length(all_scores) + 1]] <- dt
    }
    
    # Combine all data
    scores_dt <- rbindlist(all_scores, fill = TRUE)
    
    log_message(sprintf("Loaded %d %s score records", nrow(scores_dt), sport), "SUCCESS")
    
    return(scores_dt)
    
  }, error = function(e) {
    log_message(sprintf("Failed to load %s scores dataset: %s", sport, e$message), "ERROR")
    return(data.table())
  })
}

# DATA PROCESSING FUNCTIONS ------------------------------------------------

#' Merge EV data with scores
#' @param ev_dt Expected value data
#' @param scores_dt Scores data
#' @return Merged data.table
merge_ev_scores <- function(ev_dt, scores_dt) {
  if (is.null(ev_dt) || nrow(ev_dt) == 0) {
    log_message("No EV data to merge", "ERROR")
    return(NULL)
  }
  
  if (!is.null(scores_dt) && nrow(scores_dt) > 0) {
    # Prepare scores for merge
    scores_merge <- scores_dt[game_state %in% c("FINAL", "STATUS_FINAL"), .(
      game_id,
      home_score_final = home_score,
      away_score_final = away_score,
      game_state_final = game_state
    )]
    
    # Use data.table merge for efficiency
    merged <- merge(ev_dt, scores_merge, by = "game_id", all.x = TRUE)
    
    # Calculate winner field
    merged[, winner := fcase(
      is.na(home_score_final) | is.na(away_score_final), NA,
      home_or_away == "home" & home_score_final > away_score_final, TRUE,
      home_or_away == "home" & home_score_final < away_score_final, FALSE,  
      home_or_away == "away" & away_score_final > home_score_final, TRUE,
      home_or_away == "away" & away_score_final < home_score_final, FALSE,
      default = NA
    )]
    
    log_message(sprintf("Merged with scores: %d records, %d with results", 
                        nrow(merged), sum(!is.na(merged$winner))), "INFO")
    
  } else {
    # No scores to merge
    merged <- copy(ev_dt)
    merged[, `:=`(
      home_score_final = NA_integer_,
      away_score_final = NA_integer_, 
      game_state_final = NA_character_,
      winner = NA
    )]
    
    log_message("No scores data to merge", "WARN")
  }
  
  return(merged)
}

#' Split data into completed and upcoming games
#' @param merged_dt Merged data
#' @return List with completed and upcoming data.tables
split_games <- function(merged_dt) {
  # Completed games have final scores and results
  completed <- merged_dt[!is.na(game_state_final) & game_state_final %in% c("FINAL", "STATUS_FINAL")]
  
  # Upcoming games don't have final results  
  upcoming <- merged_dt[is.na(game_state_final) | !game_state_final %in% c("FINAL", "STATUS_FINAL")]
  
  log_message(sprintf("Split data: %d completed, %d upcoming games",
                      nrow(completed), nrow(upcoming)), "INFO")
  
  return(list(
    completed = completed,
    upcoming = upcoming  
  ))
}

#' Select essential columns for BigQuery
#' @param dt Data table
#' @param table_type "completed" or "upcoming"
#' @param sport Sport name
#' @return Data table with only essential columns
select_essential_columns <- function(dt, table_type, sport = "NBA") {
  # Get player column name
  player_col <- switch(sport,
    NFL = "QB",
    MLB = "pitcher",
    NBA = "star_player",
    "star_player"
  )
  
  if (table_type == "completed") {
    # COMPLETED GAMES: Only columns used by Shiny app
    essential <- c(
      # Core identifiers
      "game_id", "team", "home", "away", "game_date_est", "game_state",
      
      # Betting analysis
      "book", "price", "win_probability", "expected_value", "kelly_criterion",
      
      # Results
      "winner", "home_score_final", "away_score_final",
      
      # Context
      "home_or_away", "retrieval_time"
    )
    
  } else {
    # UPCOMING GAMES: Display and betting columns
    essential <- c(
      # Core identifiers
      "game_id", "team", "home", "away", "game_date_est", "game_time_est",
      
      # Betting analysis
      "book", "price", "win_probability", "expected_value", "kelly_criterion", 
      
      # Display context
      "venue", player_col, paste0(player_col, "_status"),
      
      # Metadata
      "home_or_away", "retrieval_time"
    )
  }
  
  # Only keep columns that exist
  existing_cols <- intersect(essential, names(dt))
  result <- dt[, ..existing_cols]
  
  log_message(sprintf("%s table: keeping %d/%d columns (%.0f%% reduction)", 
                      table_type, length(existing_cols), ncol(dt),
                      (1 - length(existing_cols)/ncol(dt)) * 100), "INFO")
  
  return(result)
}

# UPDATE FUNCTIONS ---------------------------------------------------------

#' Update completed games table
#' @param completed_games New completed games data
#' @param sport Sport name
#' @return TRUE on success
update_completed_games <- function(completed_games, sport) {
  table_name <- paste0(tolower(sport), "_completed_games")
  
  if (nrow(completed_games) == 0) {
    log_message(sprintf("No %s completed games to update", sport), "INFO")
    return(TRUE)
  }
  
  tryCatch({
    # Get existing data
    existing <- get_existing_data(table_name)
    
    if (nrow(existing) > 0) {
      # Find truly new games (avoid duplicates)
      key_cols <- c("game_id", "team", "book", "retrieval_time")
      existing_key_cols <- intersect(key_cols, names(existing))
      new_key_cols <- intersect(key_cols, names(completed_games))
      
      if (length(existing_key_cols) == length(new_key_cols) && 
          length(existing_key_cols) > 0) {
        setkeyv(existing, existing_key_cols)
        setkeyv(completed_games, new_key_cols)
        new_only <- completed_games[!existing]
      } else {
        # If key columns don't match, append all
        new_only <- completed_games
      }
      
      if (nrow(new_only) == 0) {
        log_message(sprintf("No new %s completed games to add", sport), "INFO")
        return(TRUE)
      }
      
      # Append new data
      table_ref <- paste0(bq_config$project_id, ".", bq_config$dataset, ".", table_name)
      
      bq_table_upload(
        table_ref,
        new_only,
        write_disposition = "WRITE_APPEND"
      )
      
      log_message(sprintf("Appended %d new %s completed game records", nrow(new_only), sport), "SUCCESS")
      
    } else {
      # First time - write all
      table_ref <- paste0(bq_config$project_id, ".", bq_config$dataset, ".", table_name)
      
      bq_table_upload(
        table_ref,
        completed_games,
        write_disposition = "WRITE_TRUNCATE"
      )
      
      log_message(sprintf("Created %s completed games table with %d records", 
                          sport, nrow(completed_games)), "SUCCESS")
    }
    
    return(TRUE)
    
  }, error = function(e) {
    log_message(sprintf("Failed to update %s completed games: %s", sport, e$message), "ERROR")
    return(FALSE)
  })
}

#' Update upcoming games table
#' @param upcoming_games New upcoming games data
#' @param sport Sport name
#' @return TRUE on success
update_upcoming_games <- function(upcoming_games, sport) {
  table_name <- paste0(tolower(sport), "_upcoming_games")
  
  if (nrow(upcoming_games) == 0) {
    log_message(sprintf("No %s upcoming games to update", sport), "INFO")
    return(TRUE)
  }
  
  tryCatch({
    # Upcoming games table is always overwritten
    table_ref <- paste0(bq_config$project_id, ".", bq_config$dataset, ".", table_name)
    
    bq_table_upload(
      table_ref,
      upcoming_games,
      write_disposition = "WRITE_TRUNCATE"
    )
    
    log_message(sprintf("Updated %s upcoming games table with %d records", 
                        sport, nrow(upcoming_games)), "SUCCESS")
    
    return(TRUE)
    
  }, error = function(e) {
    log_message(sprintf("Failed to update %s upcoming games: %s", sport, e$message), "ERROR")
    return(FALSE)
  })
}

#' Save RDS files for local Shiny app
#' @param completed Completed games data
#' @param upcoming Upcoming games data
#' @param sport Sport name
save_rds_files <- function(completed, upcoming, sport) {
  sport_lower <- tolower(sport)
  
  # Create directory if needed
  dir.create(bq_config$rds_path, showWarnings = FALSE, recursive = TRUE)
  
  tryCatch({
    # Save completed games
    if (nrow(completed) > 0) {
      file_path <- file.path(bq_config$rds_path, sprintf("%s_completed_games.rds", sport_lower))
      saveRDS(completed, file_path)
      log_message(sprintf("Saved %s_completed_games.rds (%d records)", sport_lower, nrow(completed)), "DEBUG")
    }
    
    # Save upcoming games
    if (nrow(upcoming) > 0) {
      file_path <- file.path(bq_config$rds_path, sprintf("%s_upcoming_games.rds", sport_lower))
      saveRDS(upcoming, file_path)
      log_message(sprintf("Saved %s_upcoming_games.rds (%d records)", sport_lower, nrow(upcoming)), "DEBUG")
    }
    
    return(TRUE)
    
  }, error = function(e) {
    log_message(sprintf("Failed to save RDS files: %s", e$message), "WARN")
    return(FALSE)
  })
}

# MAIN FUNCTION ------------------------------------------------------------

#' Main function to update BigQuery for a sport
#' @param sport Sport name ("NBA", "NFL", "MLB")
#' @return TRUE on success, FALSE on failure
update_bigquery_espn <- function(sport = "NBA") {
  # Initialize
  init_logging(sprintf("%s_bigquery_update", tolower(sport)))
  check_delay_arg()
  
  log_message(sprintf("=== Starting %s BigQuery Update ===", sport), "INFO")
  
  # Load data
  ev_dt <- load_ev_dataset_sport(sport)
  if (is.null(ev_dt)) {
    log_message(sprintf("Cannot proceed without %s EV data", sport), "ERROR")
    return(FALSE)
  }
  
  scores_dt <- load_scores_dataset_sport(sport)
  
  # Merge data
  merged_dt <- merge_ev_scores(ev_dt, scores_dt)
  if (is.null(merged_dt)) {
    log_message("Failed to merge data", "ERROR")
    return(FALSE)
  }
  
  # Split games
  games <- split_games(merged_dt)
  
  # Apply column optimization
  games$completed <- select_essential_columns(games$completed, "completed", sport)
  games$upcoming <- select_essential_columns(games$upcoming, "upcoming", sport)
  
  # Authenticate with BigQuery
  if (!authenticate_bigquery()) {
    log_message("Cannot authenticate with BigQuery", "ERROR")
    return(FALSE)
  }
  
  # Update tables
  success <- TRUE
  
  # Update completed games
  if (!update_completed_games(games$completed, sport)) {
    success <- FALSE
  }
  
  # Update upcoming games
  if (!update_upcoming_games(games$upcoming, sport)) {
    success <- FALSE
  }
  
  # Save RDS files for local Shiny app
  save_rds_files(games$completed, games$upcoming, sport)
  
  if (success) {
    log_message(sprintf("%s BigQuery update completed successfully", sport), "SUCCESS")
    
    # Log summary statistics
    log_message(sprintf("Final stats - Completed: %d records (%d cols), Upcoming: %d records (%d cols)",
                        nrow(games$completed), ncol(games$completed),
                        nrow(games$upcoming), ncol(games$upcoming)), "INFO")
  } else {
    log_message(sprintf("%s BigQuery update completed with errors", sport), "ERROR")
  }
  
  return(success)
}

# RUN IF CALLED DIRECTLY ---------------------------------------------------

if (!interactive()) {
  # Get sport from command line
  args <- commandArgs(trailingOnly = TRUE)
  
  # Extract sport argument
  sport <- "NBA"  # Default
  for (arg in args) {
    if (arg %in% c("NBA", "NFL", "MLB")) {
      sport <- arg
      break
    }
  }
  
  success <- update_bigquery_espn(sport)
  if (!success) {
    quit(status = 1)
  }
}
