# NHL STARTING GOALIES COLLECTOR ------------------------------------------
# Purpose: Efficiently collect starting goalie information from DailyFaceoff
# Author: Professional implementation with proven HTML parsing logic
# Last Updated: 2025-06-15

suppressPackageStartupMessages({
  library(data.table)
  library(rvest)
  library(stringr)
  library(anytime)
  library(here)
})

# Source utilities using here()
source(here("R", "nhl_ev_retrieval", "utils", "common.R"))
source(here("R", "nhl_ev_retrieval", "utils", "selenium_manager.R"))

# EXTRACTION FUNCTIONS (USING PROVEN LOGIC FROM PASTE.TXT) ----------------

#' Extract goalie data from DailyFaceoff HTML (PROVEN LOGIC)
#' @param html_text HTML source as text
#' @return data.table with goalie information
extract_goalie_data <- function(html_text) {
  tryCatch({
    # Parse HTML
    html <- read_html(html_text)
    
    # Find all game articles - EXACT selector from working script
    game_articles <- html %>%
      html_nodes("article.w-full")
    
    if (length(game_articles) == 0) {
      log_message("No game articles found in DailyFaceoff HTML", "DEBUG")
      return(data.table())
    }
    
    goalie_list <- list()
    
    for (i in seq_along(game_articles)) {
      article <- game_articles[[i]]
      game_goalies <- extract_single_game_goalies(article)
      if (!is.null(game_goalies) && nrow(game_goalies) > 0) {
        goalie_list[[length(goalie_list) + 1]] <- game_goalies
      }
    }
    
    if (length(goalie_list) == 0) {
      return(data.table())
    }
    
    # Combine all games
    goalies_dt <- rbindlist(goalie_list, fill = TRUE)
    
    return(goalies_dt)
    
  }, error = function(e) {
    log_message(sprintf("Error extracting goalie data: %s", e$message), "ERROR")
    return(data.table())
  })
}

#' Extract goalies for a single game (PROVEN LOGIC FROM PASTE.TXT)
#' @param article HTML node for game article
#' @return data.table with game's goalies or NULL
extract_single_game_goalies <- function(article) {
  tryCatch({
    # Extract teams - EXACT logic from working script
    teams_container <- article %>%
      html_node("span.text-center.text-3xl.text-white")
    
    if (is.null(teams_container)) {
      log_message("No teams container found", "DEBUG")
      return(NULL)
    }
    
    teams_text <- html_text(teams_container, trim = TRUE)
    team_parts <- str_split(teams_text, " at ")[[1]] %>% 
      str_trim() %>% 
      Filter(nzchar, .)
    
    if (length(team_parts) < 2) {
      log_message(sprintf("Invalid teams format: %s", teams_text), "DEBUG")
      return(NULL)
    }
    
    away_team <- team_parts[1]
    home_team <- team_parts[2]
    game <- paste(away_team, "at", home_team)
    
    # Extract game time - EXACT logic from working script
    game_time <- article %>%
      html_node("span.text-center.text-base.text-white") %>%
      html_text(trim = TRUE)
    
    if (is.null(game_time)) game_time <- NA_character_
    
    # Find goalie sections - EXACT selectors from working script
    goalie_sections <- article %>%
      html_nodes(".flex-grow.flex-col.justify-start.p-2.xl\\:justify-start.xl\\:flex-row-reverse,
                 .flex-grow.flex-col.justify-start.p-2.xl\\:justify-start.xl\\:flex-row")
    
    if (length(goalie_sections) == 0) {
      log_message(sprintf("No goalie sections found for game: %s", game), "DEBUG")
      return(NULL)
    }
    
    goalies <- list()
    
    for (j in seq_along(goalie_sections)) {
      section <- goalie_sections[[j]]
      
      # Extract goalie name - EXACT logic from working script
      goalie_name <- section %>%
        html_node("span.text-center.text-lg.xl\\:text-2xl") %>%
        html_text(trim = TRUE)
      
      if (is.null(goalie_name) || is.na(goalie_name) || goalie_name == "") {
        log_message("No goalie name found in section", "DEBUG")
        next
      }
      
      # Determine which team this goalie belongs to - EXACT logic from working script
      is_away <- grepl("reverse", html_attr(section, "class"))
      this_team <- if (is_away) away_team else home_team
      
      # Extract status information - EXACT logic from working script
      status_container <- section %>%
        html_node(".flex-col.font-bold.text-orange-500,
                  .flex-col.font-bold.text-red-600,
                  .flex-col.font-bold.text-green-700")
      
      status <- "Unknown"
      status_updated <- NA_character_
      
      if (!is.null(status_container)) {
        status_spans <- status_container %>%
          html_nodes("span.text-center") %>%
          html_text(trim = TRUE)
        
        # Parse status and update time - EXACT logic from working script
        for (span_text in status_spans) {
          if (grepl("\\b(AM|PM|GMT|EST|CST|PST|\\d{4})\\b", span_text)) {
            status_updated <- span_text
          } else if (span_text %in% c("Likely", "Confirmed", "Unconfirmed", "Projected")) {
            status <- span_text
          }
        }
      }
      
      # Add to list
      goalies[[length(goalies) + 1]] <- list(
        game = game,
        away = standardize_team_name(away_team),
        home = standardize_team_name(home_team),
        # game_time = format(with_tz(force_tz(anytime(game_time), Sys.timezone() %||% "UTC"), "America/New_York"), "%H:%M"),
        game_time = format(with_tz(force_tz(anytime(game_time), "GMT"), "America/New_York"), "%H:%M"),
        team = standardize_team_name(this_team),
        goalie = goalie_name,
        status = status,
        status_updated = status_updated
      )
    }
    
    if (length(goalies) == 0) {
      log_message(sprintf("No valid goalies found for game: %s", game), "DEBUG")
      return(NULL)
    }
    
    # Convert to data.table
    goalies_dt <- rbindlist(goalies, fill = TRUE)
    
    return(goalies_dt)
    
  }, error = function(e) {
    log_message(sprintf("Error extracting single game goalies: %s", e$message), "DEBUG")
    return(NULL)
  })
}

# MAIN COLLECTION FUNCTION -------------------------------------------------

#' Collect starting goalies for multiple dates
#' @param dates Vector of dates to collect
#' @return data.table with starting goalies
collect_starting_goalies <- function(dates) {
  log_message("Starting goalie collection from DailyFaceoff", "INFO")
  
  # Validate dates
  dates <- parse_date_args(dates)
  
  # Initialize Selenium session
  session <- SeleniumSession$new()
  
  if (!session$start()) {
    log_message("Failed to start Selenium session", "ERROR")
    return(data.table())
  }
  
  # Ensure cleanup
  on.exit(session$cleanup(), add = TRUE)
  
  # Build URLs for all dates
  urls <- sprintf("%s/%s", nhl_config$dailyfaceoff_url, format(dates, "%Y-%m-%d"))
  
  log_message(sprintf("Collecting goalies for %d dates", length(dates)), "INFO")
  
  # Extract data for all URLs
  results <- session$navigate_batch(urls, extract_goalie_data, delay = 3)
  
  # Process results
  all_goalies <- list()
  
  for (i in seq_along(results)) {
    if (!is.null(results[[i]]) && nrow(results[[i]]) > 0) {
      # Add date to results
      results[[i]][, date := dates[i]]
      all_goalies[[length(all_goalies) + 1]] <- results[[i]]
    } else {
      log_message(sprintf("No goalies found for %s", 
                          format(dates[i], "%Y-%m-%d")), "WARN")
    }
  }
  
  if (length(all_goalies) == 0) {
    log_message("No starting goalies found for any date", "WARN")
    return(data.table())
  }
  
  # Combine all results
  goalies_dt <- rbindlist(all_goalies, fill = TRUE)
  
  # Add metadata
  goalies_dt[, retrieval_time := Sys.time()]
  
  # Order columns
  setcolorder(goalies_dt, c(
    "date", "game", "team", "away", "home", 
    "goalie", "status", "status_updated", "game_time", "retrieval_time"
  ))
  
  # Sort
  setorder(goalies_dt, date, game, team)
  
  # Remove duplicates (by date and team)
  goalies_dt <- unique(goalies_dt, by = c("date", "team"))
  
  log_message(sprintf("Retrieved %d goalie entries across %d games", 
                      nrow(goalies_dt),
                      uniqueN(goalies_dt[, paste(date, game)])), "SUCCESS")
  
  # Validate data quality
  validate_goalie_data(goalies_dt)
  
  return(goalies_dt)
}

# DATA VALIDATION FUNCTIONS ------------------------------------------------

#' Validate goalie data quality
#' @param dt data.table with goalie data  
validate_goalie_data <- function(dt) {
  if (nrow(dt) == 0) {
    log_message("No goalie data to validate", "WARN")
    return(invisible())
  }
  
  # Check for games with missing goalies
  games_summary <- dt[, .(n_goalies = .N), by = .(date, game)]
  incomplete_games <- games_summary[n_goalies < 2]
  
  if (nrow(incomplete_games) > 0) {
    log_message(sprintf("Found %d games with incomplete goalie info", 
                        nrow(incomplete_games)), "WARN")
    
    # Log details about incomplete games
    for (i in 1:nrow(incomplete_games)) {
      game_info <- incomplete_games[i]
      log_message(sprintf("  - %s on %s: only %d goalie(s)", 
                          game_info$game, game_info$date, game_info$n_goalies), "WARN")
    }
  }
  
  # Check for duplicate goalies (same goalie for both teams)
  duplicate_goalies <- dt[, .(
    teams = paste(sort(unique(team)), collapse = " vs "),
    goalies = paste(unique(goalie), collapse = ", "),
    n_unique_goalies = uniqueN(goalie)
  ), by = .(date, game)]
  
  problem_games <- duplicate_goalies[n_unique_goalies < 2]
  
  if (nrow(problem_games) > 0) {
    log_message(sprintf("Found %d games with duplicate/missing goalies", 
                        nrow(problem_games)), "ERROR")
    
    for (i in 1:nrow(problem_games)) {
      game_info <- problem_games[i]
      log_message(sprintf("  - %s on %s: %s (goalies: %s)", 
                          game_info$game, game_info$date, 
                          game_info$teams, game_info$goalies), "ERROR")
    }
  }
  
  # Check for unknown status
  unknown_status <- dt[status == "Unknown", .N]
  if (unknown_status > 0) {
    log_message(sprintf("%d goalies have Unknown status", unknown_status), "DEBUG")
  }
  
  # Check for missing goalie names
  missing_names <- dt[is.na(goalie) | goalie == "", .N]
  if (missing_names > 0) {
    log_message(sprintf("%d entries with missing goalie names", missing_names), "WARN")
  }
  
  # Log status distribution
  status_summary <- dt[, .N, by = status][order(-N)]
  if (nrow(status_summary) > 0) {
    status_msg <- paste(sprintf("%s: %d", status_summary$status, status_summary$N), 
                        collapse = ", ")
    log_message(sprintf("Status distribution: %s", status_msg), "INFO")
  }
  
  return(invisible())
}

#' Debug function to save HTML for problematic games
#' @param html_text HTML source
#' @param date Date being processed
#' @param reason Reason for saving debug HTML
save_debug_html <- function(html_text, date, reason = "debug") {
  if (!nhl_config$debug_mode) return(invisible())
  
  debug_dir <- file.path(nhl_config$data_dir, "debug")
  dir.create(debug_dir, showWarnings = FALSE, recursive = TRUE)
  
  debug_file <- file.path(debug_dir, 
                          sprintf("goalies_%s_%s_%s.html", 
                                  format(date, "%Y%m%d"), 
                                  reason,
                                  format(Sys.time(), "%H%M%S")))
  
  writeLines(html_text, debug_file)
  log_message(sprintf("Saved debug HTML: %s", debug_file), "DEBUG")
  
  return(invisible())
}
