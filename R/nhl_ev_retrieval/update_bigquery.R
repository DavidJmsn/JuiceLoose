# UPDATE BIGQUERY DATABASE ------------------------------------------------
# Purpose: Efficiently update NHL data in BigQuery using Arrow for multi-file
#          datasets and data.table for processing
# Author: Professional implementation with Arrow and data.table
# Last Updated: 2025-12-19

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(bigrquery)
  library(DBI)
  library(gargle)
  library(lubridate)
  library(dplyr)
})

# Get script directory
if (interactive()) {
  script_dir <- getwd()
} else {
  script_dir <- dirname(sys.frame(1)$ofile)
}

# Source utilities
source(file.path(script_dir, "utils", "common.R"))

# CONFIGURATION ------------------------------------------------------------

bq_config <- list(
  # BigQuery settings
  project_id = Sys.getenv("PROJECT_ID"),
  dataset = Sys.getenv("DATASET"),
  service_json = Sys.getenv("SERVICE_ACCOUNT"),
  
  # Table names
  completed_table = "completed_games",
  upcoming_table = "upcoming_games",
  
  # Data paths (for multi-file datasets)
  ev_path = file.path(nhl_config$data_dir, "expected_value"),
  scores_path = file.path(nhl_config$data_dir, "scores"),
  rds_path = file.path(nhl_config$data_dir, "rds_files"),
  
  # Arrow configuration
  use_arrow_dataset = TRUE,
  batch_size = 10000  # For reading Arrow datasets in batches
)

# ARROW SCHEMAS ------------------------------------------------------------

#' Define Arrow schema for expected values dataset
#' @return Arrow schema
get_ev_schema <- function() {
  schema(
    game_id = int64(),
    team = string(),
    home = string(),
    away = string(),
    game_date_est = date32(),
    game_time_est = string(),
    # game_time_est = timestamp("s", "UTC"),
    venue = string(),
    broadcast = string(),
    game_state = string(),
    home_or_away = string(),
    goalie = string(),
    goalie_status = string(),
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
    retrieval_time = timestamp("s", "UTC")
  )
}

#' Define Arrow schema for scores dataset
#' @return Arrow schema
get_scores_schema <- function() {
  schema(
    game_id = int64(),
    season = int64(),
    game_type = int64(),
    game_type_desc = string(),
    game_state = string(),
    game_date_est = date32(),
    start_time_utc = string(),
    start_time_est = string(),
    # start_time_utc = timestamp("s", "UTC"),
    # start_time_est = timestamp("s", "UTC"),
    venue = string(),
    home_team = string(),
    away_team = string(),
    home = string(),
    away = string(),
    home_score = int64(),
    away_score = int64(),
    local_timezone = string(),
    broadcast = string()
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

# DATA LOADING FUNCTIONS WITH ARROW ----------------------------------------

#' Load EV dataset using Arrow for multi-file support
#' @return data.table or NULL
load_ev_dataset_arrow <- function() {
  tryCatch({
    log_message("Loading EV dataset with Arrow", "INFO")
    
    # Get all CSV files in the directory
    ev_files <- list.files(bq_config$ev_path, 
                          pattern = "*.csv$",
                          full.names = TRUE)
    
    if (length(ev_files) == 0) {
      log_message("No EV files found", "ERROR")
      return(NULL)
    }
    
    log_message(sprintf("Found %d EV files to process", length(ev_files)), "DEBUG")
    
    # Open as Arrow dataset
    ev_dataset <- open_dataset(
      bq_config$ev_path,
      format = "csv",
      schema = get_ev_schema(),
      skip = 1  # Skip header row
    )
    
    # Collect to data.table
    ev_dt <- ev_dataset %>%
      select(all_of(names(get_ev_schema()))) %>%
      collect() %>%
      as.data.table()
    
    log_message(sprintf("Loaded %d EV records from %d files", 
                       nrow(ev_dt), length(ev_files)), "SUCCESS")
    
    return(ev_dt)
    
  }, error = function(e) {
    log_message(sprintf("Failed to load EV dataset with Arrow: %s", e$message), "ERROR")
    # Fallback to single file loading
    return(load_ev_dataset_fallback())
  })
}

#' Fallback function to load most recent EV file
#' @return data.table or NULL
load_ev_dataset_fallback <- function() {
  log_message("Falling back to single file loading", "WARN")
  
  tryCatch({
    ev_files <- list.files(bq_config$ev_path, 
                          pattern = "*.csv$",
                          full.names = TRUE)
    
    if (length(ev_files) == 0) {
      log_message("No EV files found", "ERROR")
      return(NULL)
    }
    
    # Use most recent file
    latest_file <- ev_files[order(file.mtime(ev_files), decreasing = TRUE)][1]
    
    log_message(sprintf("Loading EV data from: %s", basename(latest_file)), "INFO")
    
    # Read with data.table
    ev_dt <- fread(latest_file)
    
    # Ensure proper date/time columns
    if ("game_date_est" %in% names(ev_dt) && is.character(ev_dt$game_date_est)) {
      ev_dt[, game_date_est := as.Date(game_date_est)]
    }
    
    if ("game_time_est" %in% names(ev_dt) && is.character(ev_dt$game_time_est)) {
      ev_dt[, game_time_est := as.POSIXct(game_time_est)]
    }
    
    if ("retrieval_time" %in% names(ev_dt) && is.character(ev_dt$retrieval_time)) {
      ev_dt[, retrieval_time := as.POSIXct(retrieval_time)]
    }
    
    log_message(sprintf("Loaded %d EV records", nrow(ev_dt)), "DEBUG")
    
    return(ev_dt)
    
  }, error = function(e) {
    log_message(sprintf("Failed to load EV dataset: %s", e$message), "ERROR")
    return(NULL)
  })
}

#' Load scores dataset using Arrow for multi-file support
#' @return data.table or NULL
load_scores_dataset_arrow <- function() {
  tryCatch({
    log_message("Loading scores dataset with Arrow", "INFO")
    
    # Get all CSV files
    scores_files <- list.files(bq_config$scores_path,
                              pattern = "*.csv$",
                              full.names = TRUE)
    
    if (length(scores_files) == 0) {
      log_message("No scores files found", "WARN")
      return(data.table())
    }
    
    log_message(sprintf("Found %d scores files to process", length(scores_files)), "DEBUG")
    
    # Open as Arrow dataset
    scores_dataset <- open_dataset(
      bq_config$scores_path,
      format = "csv",
      schema = get_scores_schema(),
      skip = 1
    )
    
    # Collect to data.table
    scores_dt <- scores_dataset %>%
      select(all_of(names(get_scores_schema()))) %>%
      collect() %>%
      as.data.table()
    
    log_message(sprintf("Loaded %d score records from %d files", 
                       nrow(scores_dt), length(scores_files)), "SUCCESS")
    
    return(scores_dt)
    
  }, error = function(e) {
    log_message(sprintf("Failed to load scores dataset with Arrow: %s", e$message), "ERROR")
    # Fallback to single file loading
    return(load_scores_dataset_fallback())
  })
}

#' Fallback function to load most recent scores file
#' @return data.table or NULL
load_scores_dataset_fallback <- function() {
  log_message("Falling back to single file loading for scores", "WARN")
  
  tryCatch({
    scores_files <- list.files(bq_config$scores_path,
                              pattern = "*.csv$",
                              full.names = TRUE)
    
    if (length(scores_files) == 0) {
      log_message("No scores files found", "WARN")
      return(data.table())
    }
    
    # Use most recent file
    latest_file <- scores_files[order(file.mtime(scores_files), decreasing = TRUE)][1]
    
    log_message(sprintf("Loading scores from: %s", basename(latest_file)), "INFO")
    
    scores_dt <- fread(latest_file)
    
    # Ensure proper date columns
    if ("game_date_est" %in% names(scores_dt) && is.character(scores_dt$game_date_est)) {
      scores_dt[, game_date_est := as.Date(game_date_est)]
    }
    
    log_message(sprintf("Loaded %d score records", nrow(scores_dt)), "DEBUG")
    
    return(scores_dt)
    
  }, error = function(e) {
    log_message(sprintf("Failed to load scores: %s", e$message), "ERROR")
    return(data.table())
  })
}

# DATA PROCESSING FUNCTIONS ------------------------------------------------

#' Merge EV and scores data efficiently
#' @param ev_dt EV data
#' @param scores_dt Scores data
#' @return Merged data.table
merge_ev_scores <- function(ev_dt, scores_dt) {
  log_message("Merging EV and scores data", "INFO")
  
  # Validate inputs
  if (is.null(ev_dt) || nrow(ev_dt) == 0) {
    log_message("No EV data to merge", "ERROR")
    return(NULL)
  }
  
  # Simple merge on game_id
  if (nrow(scores_dt) > 0) {
    # Ensure game_id is the same type in both datasets
    ev_dt[, game_id := as.integer(game_id)]
    scores_dt[, game_id := as.integer(game_id)]
    
    # Add scores data as additional columns
    scores_merge <- scores_dt[, .(
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
  completed <- merged_dt[!is.na(game_state_final) & game_state_final %in% c("FINAL", "OFF")]
  
  # Upcoming games don't have final results  
  upcoming <- merged_dt[is.na(game_state_final) | !game_state_final %in% c("FINAL", "OFF")]
  
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
#' @return Data table with only essential columns
select_essential_columns <- function(dt, table_type) {
  
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
      "venue", "goalie", "goalie_status",
      
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
#' @return TRUE on success
update_completed_games <- function(completed_games) {
  if (nrow(completed_games) == 0) {
    log_message("No completed games to update", "INFO")
    return(TRUE)
  }
  
  tryCatch({
    # Get existing data
    existing <- get_existing_data(bq_config$completed_table)
    
    if (nrow(existing) > 0) {
      # Find truly new games (avoid duplicates)
      key_cols <- c("game_id", "team", "book", "retrieval_time")
      existing_key_cols <- intersect(key_cols, names(existing))
      new_key_cols <- intersect(key_cols, names(completed_games))
      
      if (length(existing_key_cols) == length(new_key_cols) && 
          length(existing_key_cols) > 0) {
        setkey(existing, existing_key_cols)
        setkey(completed_games, new_key_cols)
        new_only <- completed_games[!existing]
      } else {
        # If key columns don't match, append all
        new_only <- completed_games
      }
      
      if (nrow(new_only) == 0) {
        log_message("No new completed games to add", "INFO")
        return(TRUE)
      }
      
      # Append new data
      table_ref <- paste0(bq_config$project_id, ".", bq_config$dataset, ".", 
                         bq_config$completed_table)
      
      bq_table_upload(
        table_ref,
        new_only,
        write_disposition = "WRITE_APPEND"
      )
      
      log_message(sprintf("Appended %d new completed game records", nrow(new_only)), "SUCCESS")
      
    } else {
      # First time - write all
      table_ref <- paste0(bq_config$project_id, ".", bq_config$dataset, ".", 
                         bq_config$completed_table)
      
      bq_table_upload(
        table_ref,
        completed_games,
        write_disposition = "WRITE_TRUNCATE"
      )
      
      log_message(sprintf("Created completed games table with %d records", 
                          nrow(completed_games)), "SUCCESS")
    }
    
    return(TRUE)
    
  }, error = function(e) {
    log_message(sprintf("Failed to update completed games: %s", e$message), "ERROR")
    return(FALSE)
  })
}

#' Update upcoming games table
#' @param upcoming_games New upcoming games data
#' @return TRUE on success
update_upcoming_games <- function(upcoming_games) {
  if (nrow(upcoming_games) == 0) {
    log_message("No upcoming games to update", "INFO")
    return(TRUE)
  }
  
  tryCatch({
    # Upcoming games table is always overwritten
    table_ref <- paste0(bq_config$project_id, ".", bq_config$dataset, ".", 
                       bq_config$upcoming_table)
    
    bq_table_upload(
      table_ref,
      upcoming_games,
      write_disposition = "WRITE_TRUNCATE"
    )
    
    log_message(sprintf("Updated upcoming games table with %d records", 
                        nrow(upcoming_games)), "SUCCESS")
    
    return(TRUE)
    
  }, error = function(e) {
    log_message(sprintf("Failed to update upcoming games: %s", e$message), "ERROR")
    return(FALSE)
  })
}

#' Save RDS files for local Shiny app
#' @param completed Completed games data
#' @param upcoming Upcoming games data
save_rds_files <- function(completed, upcoming) {
  # Create directory if needed
  dir.create(bq_config$rds_path, showWarnings = FALSE, recursive = TRUE)
  
  tryCatch({
    # Save completed games
    if (nrow(completed) > 0) {
      saveRDS(completed, file.path(bq_config$rds_path, "completed_games.rds"))
      log_message(sprintf("Saved completed_games.rds (%d records)", nrow(completed)), "DEBUG")
    }
    
    # Save upcoming games
    if (nrow(upcoming) > 0) {
      saveRDS(upcoming, file.path(bq_config$rds_path, "upcoming_games.rds"))
      log_message(sprintf("Saved upcoming_games.rds (%d records)", nrow(upcoming)), "DEBUG")
    }
    
    return(TRUE)
    
  }, error = function(e) {
    log_message(sprintf("Failed to save RDS files: %s", e$message), "WARN")
    return(FALSE)
  })
}

# MAIN FUNCTION ------------------------------------------------------------

#' Main function to update BigQuery
#' @return TRUE on success, FALSE on failure
update_bigquery_nhl <- function() {
  # Initialize
  init_logging("bigquery_update")
  check_delay_arg()
  
  log_message("=== Starting NHL BigQuery Update ===", "INFO")
  
  # Load data using Arrow (with fallback)
  if (bq_config$use_arrow_dataset) {
    ev_dt <- load_ev_dataset_arrow()
  } else {
    ev_dt <- load_ev_dataset_fallback()
  }
  
  if (is.null(ev_dt)) {
    log_message("Cannot proceed without EV data", "ERROR")
    return(FALSE)
  }
  
  if (bq_config$use_arrow_dataset) {
    scores_dt <- load_scores_dataset_arrow()
  } else {
    scores_dt <- load_scores_dataset_fallback()
  }
  
  # Merge data
  merged_dt <- merge_ev_scores(ev_dt, scores_dt)
  if (is.null(merged_dt)) {
    log_message("Failed to merge data", "ERROR")
    return(FALSE)
  }
  
  # Split games
  games <- split_games(merged_dt)
  
  # Apply column optimization
  games$completed <- select_essential_columns(games$completed, "completed")
  games$upcoming <- select_essential_columns(games$upcoming, "upcoming")
  
  # Authenticate with BigQuery
  if (!authenticate_bigquery()) {
    log_message("Cannot authenticate with BigQuery", "ERROR")
    return(FALSE)
  }
  
  # Update tables
  success <- TRUE
  
  # Update completed games
  if (!update_completed_games(games$completed)) {
    success <- FALSE
  }
  
  # Update upcoming games
  if (!update_upcoming_games(games$upcoming)) {
    success <- FALSE
  }
  
  # Save RDS files for local Shiny app
  save_rds_files(games$completed, games$upcoming)
  
  if (success) {
    log_message("NHL BigQuery update completed successfully", "SUCCESS")
    
    # Log summary statistics
    log_message(sprintf("Final stats - Completed: %d records (%d cols), Upcoming: %d records (%d cols)",
                        nrow(games$completed), ncol(games$completed),
                        nrow(games$upcoming), ncol(games$upcoming)), "INFO")
  } else {
    log_message("NHL BigQuery update completed with errors", "ERROR")
  }
  
  return(success)
}

# RUN IF CALLED DIRECTLY ---------------------------------------------------

if (!interactive()) {
  success <- update_bigquery_nhl()
  if (!success) {
    quit(status = 1)
  }
}
