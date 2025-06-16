# NHL WIN PROBABILITIES COLLECTOR -----------------------------------------
# Purpose: Efficiently collect win probabilities from MoneyPuck
# Author: Professional implementation with batch processing
# Last Updated: 2025-12-19

suppressPackageStartupMessages({
  library(data.table)
  library(rvest)
  library(stringr)
  library(here)
})

# Source utilities using here()
source(here("R", "nhl_ev_retrieval", "utils", "common.R"))
source(here("R", "nhl_ev_retrieval", "utils", "selenium_manager.R"))

# EXTRACTION FUNCTIONS -----------------------------------------------------

#' Extract win probability data from MoneyPuck HTML
#' @param html_text HTML source as text
#' @return data.table with win probabilities
extract_moneypuck_data <- function(html_text) {
  tryCatch({
    # Parse HTML
    html <- read_html(html_text)
    
    # Find game rows - try multiple selectors
    game_rows <- html %>%
      html_nodes("table tbody tr, #includedContent table tbody tr, .game-row, tr[class*='game']")
    
    if (length(game_rows) == 0) {
      log_message("No game rows found in MoneyPuck HTML", "DEBUG")
      return(data.table())
    }
    
    games_list <- list()
    
    for (row in game_rows) {
      game_data <- extract_single_game(row)
      if (!is.null(game_data)) {
        games_list[[length(games_list) + 1]] <- game_data
      }
    }
    
    if (length(games_list) == 0) {
      return(data.table())
    }
    
    # Convert to data.table
    games_dt <- rbindlist(games_list)
    
    # Create rows for both teams
    result <- rbind(
      # Away team rows
      games_dt[, .(
        away = away_team,
        home = home_team,
        team = away_team,
        win_probability = away_prob,
        game_time = game_time
      )],
      # Home team rows
      games_dt[, .(
        away = away_team,
        home = home_team,
        team = home_team,
        win_probability = home_prob,
        game_time = game_time
      )]
    )
    
    # Sort by game and team
    setorder(result, away, home, team)
    
    return(result)
    
  }, error = function(e) {
    log_message(sprintf("Error extracting MoneyPuck data: %s", e$message), "ERROR")
    return(data.table())
  })
}

#' Extract data for a single game row
#' @param row HTML node for game row
#' @return List with game data or NULL
extract_single_game <- function(row) {
  tryCatch({
    # Extract team images/names
    team_imgs <- row %>%
      html_nodes("img[alt]") %>%
      html_attr("alt")
    
    if (length(team_imgs) != 2) {
      return(NULL)
    }
    
    away_team <- clean_team_name(team_imgs[1])
    home_team <- clean_team_name(team_imgs[2])
    
    # Extract probabilities - try multiple methods
    prob_values <- extract_probabilities(row)
    
    if (is.null(prob_values) || length(prob_values) < 2) {
      return(NULL)
    }
    
    # Extract game time/status
    game_time <- extract_game_time(row)
    
    return(list(
      away_team = away_team,
      home_team = home_team,
      away_prob = prob_values[1],
      home_prob = prob_values[2],
      game_time = game_time
    ))
    
  }, error = function(e) {
    log_message(sprintf("Error extracting single game: %s", e$message), "DEBUG")
    return(NULL)
  })
}

#' Extract probability values from row
#' @param row HTML node
#' @return Numeric vector of probabilities or NULL
extract_probabilities <- function(row) {
  # Method 1: Look for h2 elements with percentages
  h2_probs <- row %>%
    html_nodes("h2") %>%
    html_text(trim = TRUE) %>%
    str_extract("\\d+(?:\\.\\d+)?(?=%)") %>%
    na.omit() %>%
    as.numeric() / 100
  
  if (length(h2_probs) >= 2) {
    return(h2_probs[1:2])
  }
  
  # Method 2: Look for cells with percentage class
  pct_cells <- row %>%
    html_nodes(".probability, .win-prob, .percentage, td.text-center") %>%
    html_text(trim = TRUE)
  
  # Extract numeric values
  pct_values <- str_extract(pct_cells, "\\d+(?:\\.\\d+)?") %>%
    na.omit() %>%
    as.numeric()
  
  # Filter reasonable probability values
  pct_values <- pct_values[pct_values > 0 & pct_values <= 100]
  
  if (length(pct_values) >= 2) {
    # If values are > 1, assume they're percentages
    if (all(pct_values > 1)) {
      return(pct_values[1:2] / 100)
    } else {
      return(pct_values[1:2])
    }
  }
  
  return(NULL)
}

#' Extract game time from row
#' @param row HTML node
#' @return Character game time or "TBD"
extract_game_time <- function(row) {
  # Look for time patterns
  all_text <- row %>%
    html_nodes("td, span") %>%
    html_text(trim = TRUE)
  
  for (text in all_text) {
    # Check for time pattern (e.g., "7:00 PM ET")
    if (grepl("\\d{1,2}:\\d{2}\\s*(AM|PM|ET|EST|EDT)", text)) {
      text <- strftime(strptime(text, format = "%I:%M %p"), format = "%H:%M")
      return(text)
    }
    # Check for game status
    if (grepl("^(Final|LIVE|In Progress|Postponed)", text, ignore.case = TRUE)) {
      return(text)
    }
  }
  
  return("TBD")
}

#' Clean team name
#' @param name Raw team name
#' @return Cleaned team name
clean_team_name <- function(name) {
  # Remove periods and standardize
  cleaned <- gsub("\\.", "", name)
  cleaned <- standardize_team_name(cleaned)
  return(cleaned)
}

# MAIN COLLECTION FUNCTION -------------------------------------------------

#' Collect win probabilities for multiple dates
#' @param dates Vector of dates to collect
#' @return data.table with win probabilities
collect_win_probabilities <- function(dates) {
  log_message("Starting win probability collection from MoneyPuck", "INFO")
  
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
  urls <- sprintf("%s?date=%s", nhl_config$moneypuck_url, format(dates, "%Y-%m-%d"))
  
  log_message(sprintf("Collecting probabilities for %d dates", length(dates)), "INFO")
  
  # Extract data for all URLs
  results <- session$navigate_batch(urls, extract_moneypuck_data, delay = 3)
  
  # Process results
  all_probabilities <- list()
  
  for (i in seq_along(results)) {
    if (!is.null(results[[i]]) && nrow(results[[i]]) > 0) {
      # Add date to results
      results[[i]][, date := dates[i]]
      all_probabilities[[length(all_probabilities) + 1]] <- results[[i]]
    } else {
      log_message(sprintf("No probabilities found for %s", 
                          format(dates[i], "%Y-%m-%d")), "WARN")
    }
  }
  
  if (length(all_probabilities) == 0) {
    log_message("No win probabilities found for any date", "WARN")
    return(data.table())
  }
  
  # Combine all results
  probabilities_dt <- rbindlist(all_probabilities, fill = TRUE)
  
  # Standardize team names
  probabilities_dt[, `:=`(
    home = standardize_team_name(home),
    away = standardize_team_name(away),
    team = standardize_team_name(team)
  )]
  
  # Add metadata
  probabilities_dt[, `:=`(
    retrieval_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ", tz = "America/New_York"),
    game = paste0(away, " at ", home)
  )]
  
  # Order columns
  setcolorder(probabilities_dt, c(
    "date", "game", "away", "home", "team", 
    "win_probability", "game_time", "retrieval_time"
  ))
  
  # Sort
  setorder(probabilities_dt, date, game, team)
  
  log_message(sprintf("Retrieved probabilities for %d teams across %d games", 
                      nrow(probabilities_dt),
                      uniqueN(probabilities_dt[, paste(date, game)])), "SUCCESS")
  
  # Validate data quality
  validate_probability_data(probabilities_dt)
  
  return(probabilities_dt)
}

#' Validate probability data
#' @param dt data.table with probability data
validate_probability_data <- function(dt) {
  # Check for duplicate entries
  dups <- dt[duplicated(dt[, .(date, team)])]
  if (nrow(dups) > 0) {
    log_message(sprintf("Found %d duplicate probability entries", nrow(dups)), "WARN")
  }
  
  # Check probability sums
  game_probs <- dt[, .(
    total_prob = sum(win_probability),
    n_teams = .N
  ), by = .(date, game)]
  
  invalid_games <- game_probs[n_teams == 2 & (total_prob < 0.95 | total_prob > 1.05)]
  
  if (nrow(invalid_games) > 0) {
    log_message(sprintf("Found %d games with invalid probability sums", 
                        nrow(invalid_games)), "WARN")
  }
  
  # Check for reasonable probability values
  extreme_probs <- dt[win_probability < 0.05 | win_probability > 0.95]
  if (nrow(extreme_probs) > 0) {
    log_message(sprintf("Found %d extreme probability values (<5%% or >95%%)", 
                        nrow(extreme_probs)), "DEBUG")
  }
}
