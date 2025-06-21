# ESPN BETTING ODDS COLLECTOR ----------------------------------------------
# Purpose: Efficiently collect betting odds for NBA/NFL/MLB with date range API
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

# SPORT MAPPINGS -----------------------------------------------------------

sport_mappings <- list(
  NBA = "basketball_nba",
  NFL = "americanfootball_nfl", 
  MLB = "baseball_mlb"
)

# PARSING FUNCTIONS --------------------------------------------------------

#' Parse nested JSON odds data using pure data.table approach
#' @param raw_data Raw API response (data.frame with nested list-columns)
#' @return data.table with flattened odds data
parse_odds_efficient <- function(raw_data) {
  if (is.null(raw_data) || nrow(raw_data) == 0) {
    log_message("No odds data to parse", "DEBUG")
    return(data.table())
  }
  
  tryCatch({
    # Convert to data.table and add game_id for tracking
    games_dt <- as.data.table(raw_data)
    games_dt[, game_id := .I]
    
    # Step 1: Unnest bookmakers
    # Extract game-level info without bookmakers column
    game_info <- games_dt[, .(
      game_id, 
      sport_key, 
      sport_title,
      commence_time,
      home_team,
      away_team
    )]
    
    # Extract bookmakers list-column
    bookmakers_list <- games_dt$bookmakers
    
    # Process each game's bookmakers
    all_odds <- list()
    
    for (i in seq_along(bookmakers_list)) {
      game_bookmakers <- bookmakers_list[[i]]
      
      if (is.null(game_bookmakers) || length(game_bookmakers) == 0) next
      
      # Convert bookmakers to data.table
      bookmakers_dt <- as.data.table(game_bookmakers)
      bookmakers_dt[, game_id := i]
      
      # Process each bookmaker's markets
      for (j in seq_len(nrow(bookmakers_dt))) {
        book_name <- bookmakers_dt$title[j]
        markets <- bookmakers_dt$markets[[j]]
        
        if (is.null(markets) || length(markets) == 0) next
        
        # Convert markets to data.table
        markets_dt <- as.data.table(markets)
        
        # Process each market
        for (k in seq_len(nrow(markets_dt))) {
          market_key <- markets_dt$key[k]
          outcomes <- markets_dt$outcomes[[k]]
          
          if (is.null(outcomes) || length(outcomes) == 0) next
          
          # Convert outcomes to data.table and add metadata
          outcomes_dt <- as.data.table(outcomes)
          outcomes_dt[, `:=`(
            game_id = i,
            book = book_name,
            market = market_key
          )]
          
          all_odds[[length(all_odds) + 1]] <- outcomes_dt
        }
      }
    }
    
    # Combine all odds
    if (length(all_odds) == 0) {
      log_message("No odds data extracted", "DEBUG")
      return(data.table())
    }
    
    odds_dt <- rbindlist(all_odds, fill = TRUE)
    
    # Merge with game info
    final_dt <- merge(odds_dt, game_info, by = "game_id", all.x = TRUE)
    
    # Parse dates
    final_dt[, `:=`(
      game_date = as.Date(commence_time),
      game_time = format(as.POSIXct(commence_time, tz = "UTC"), format = "%H:%M", tz = "America/New_York"),
      retrieved_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ", tz = "America/New_York")
    )]
    
    # Select and rename final columns
    result <- final_dt[, .(
      game_date,
      game_time,
      home_team = toupper(home_team),
      away_team = toupper(away_team),
      market,
      book,
      name = toupper(name),
      price,
      retrieved_time
    )]
    
    return(result)
    
  }, error = function(e) {
    log_message(sprintf("Error parsing odds data: %s", e$message), "ERROR")
    return(data.table())
  })
}

#' Calculate aggregate statistics for odds
#' @param odds_dt data.table with odds data
#' @return data.table with aggregated stats
calculate_odds_stats <- function(odds_dt) {
  if (nrow(odds_dt) == 0) return(odds_dt)
  
  # Calculate stats by team/market
  stats <- odds_dt[, .(
    price_mean = mean(price, na.rm = TRUE),
    price_min = min(price, na.rm = TRUE), 
    price_max = max(price, na.rm = TRUE),
    price_sd = sd(price, na.rm = TRUE),
    n_books = .N
  ), by = .(game_date, game_time, home_team, away_team, market, name)]
  
  # Merge back with original data
  result <- merge(odds_dt, stats, 
                  by = c("game_date", "game_time", "home_team", "away_team", "market", "name"),
                  all.x = TRUE)
  
  return(result)
}

#' Get best odds for each team/market
#' @param odds_dt data.table with all odds
#' @return data.table with best odds only
get_best_odds <- function(odds_dt) {
  if (nrow(odds_dt) == 0) return(odds_dt)
  
  # First ensure we have the stats
  if (!"price_mean" %in% names(odds_dt)) {
    odds_dt <- calculate_odds_stats(odds_dt)
  }
  
  # Filter to h2h market
  h2h_odds <- odds_dt[market == "h2h"]
  
  if (nrow(h2h_odds) == 0) return(data.table())
  
  # Remove duplicate book entries (keep first)
  h2h_odds <- unique(h2h_odds, by = c("game_date", "game_time", 
                                      "home_team", "away_team", "market", "name", "book"))
  
  # Get best price per team/market
  best_odds <- h2h_odds[, .SD[which.max(price)], 
                       by = .(game_date, game_time, home_team, away_team, market, name)]
  
  return(best_odds)
}

# MAIN COLLECTION FUNCTION -------------------------------------------------

#' Collect betting odds for date range
#' @param dates Vector of dates to collect
#' @param sport Sport name ("NBA", "NFL", "MLB")
#' @param markets Character vector of markets (default: "h2h")
#' @param regions Character regions code (default: "us")
#' @return data.table with betting odds
collect_espn_odds <- function(dates, sport = "NBA", markets = "h2h", regions = "us") {
  log_message(sprintf("Starting %s odds collection", sport), "INFO")
  
  # Validate sport
  sport <- toupper(sport)
  if (!sport %in% names(sport_mappings)) {
    log_message(sprintf("Invalid sport: %s", sport), "ERROR")
    return(data.table())
  }
  
  sport_key <- sport_mappings[[sport]]
  
  # Validate inputs
  dates <- parse_date_args(dates)
  
  # Check API key
  if (is.null(espn_config$odds_api_key) || espn_config$odds_api_key == "") {
    log_message("ODDS_API_KEY environment variable not set", "ERROR")
    return(data.table())
  }
  
  # Get date range
  date_range <- c(min(dates), max(dates) + 1)
  
  # Build query parameters
  query_params <- list(
    apiKey = espn_config$odds_api_key,
    regions = regions,
    markets = paste(markets, collapse = ","),
    oddsFormat = "decimal",
    dateFormat = "iso",
    commenceTimeFrom = format(as.POSIXct(date_range[1], tz = "UTC"), 
                              format = "%Y-%m-%dT00:00:00Z"),
    commenceTimeTo = format(as.POSIXct(date_range[2] + 1, tz = "UTC") - 1, 
                            format = "%Y-%m-%dT23:59:59Z")
  )
  
  log_message(sprintf("Fetching odds for date range: %s to %s", 
                      format(date_range[1], format = "%Y-%m-%d"),
                      format(date_range[2], format = "%Y-%m-%d")), "INFO")
  
  # Make API request
  endpoint <- sprintf("/v4/sports/%s/odds", sport_key)
  raw_data <- api_get(endpoint, api_type = "odds", query = query_params)
  
  if (is.null(raw_data)) {
    log_message("Failed to retrieve odds data", "ERROR")
    return(data.table())
  }
  
  log_message(sprintf("Retrieved %d events from odds API", length(raw_data)), "INFO")
  
  # Parse the data
  odds_tmp <- parse_odds_efficient(raw_data)
  
  if (nrow(odds_tmp) == 0) {
    log_message("No odds data after parsing", "WARN")
    return(data.table())
  }
  
  # Filter to requested dates
  odds_tmp <- odds_tmp[game_date %in% dates]
  
  if (nrow(odds_tmp) == 0) {
    log_message("No odds found for specified dates", "WARN")
    return(data.table())
  }
  
  # Standardize team names
  odds_tmp[, `:=`(
    home_team = standardize_team_name(home_team),
    away_team = standardize_team_name(away_team),
    name = standardize_team_name(name)
  )]
  
  # Calculate aggregate statistics
  odds_with_stats <- calculate_odds_stats(odds_tmp)
  
  # Get best odds if h2h market
  if ("h2h" %in% markets) {
    h2h_odds <- odds_with_stats[market == "h2h"]
    
    if (nrow(h2h_odds) > 0) {
      h2h_best <- get_best_odds(odds_with_stats)
      
      # Replace h2h odds with best only
      odds_dt <- rbind(
        odds_with_stats[market != "h2h"],
        h2h_best,
        fill = TRUE
      )
    } else {
      odds_dt <- odds_with_stats
    }
  } else {
    odds_dt <- odds_with_stats
  }
  
  # Sort by date, game, team
  setorder(odds_dt, game_date, game_time, home_team, away_team, name)
  
  # Log summary
  log_message(sprintf("Retrieved odds for %d teams across %d games", 
                      nrow(odds_dt),
                      uniqueN(odds_dt[, paste(game_date, home_team, away_team)])), "SUCCESS")
  
  # Log bookmaker coverage
  books_summary <- odds_dt[market == "h2h", .(n_books = uniqueN(book)), 
                          by = .(game_date, home_team, away_team)]
  avg_books <- mean(books_summary$n_books)
  log_message(sprintf("Average %.1f bookmakers per game", avg_books), "INFO")
  
  # Validate data quality
  validate_odds_data(odds_dt)
  
  return(odds_dt)
}

#' Validate odds data
#' @param dt data.table with odds data
validate_odds_data <- function(dt) {
  # Check for reasonable odds ranges
  invalid_odds <- dt[price < 1.01 | price > 100]
  if (nrow(invalid_odds) > 0) {
    log_message(sprintf("Found %d odds outside reasonable range (1.01-100)", 
                        nrow(invalid_odds)), "WARN")
  }
  
  # Check for missing prices
  missing_prices <- dt[is.na(price)]
  if (nrow(missing_prices) > 0) {
    log_message(sprintf("Found %d records with missing prices", 
                        nrow(missing_prices)), "WARN")
  }
  
  # Check implied probability sums
  if ("h2h" %in% unique(dt$market)) {
    h2h_games <- unique(dt[market == "h2h", .(game_date, home_team, away_team)])
    
    for (i in seq_len(nrow(h2h_games))) {
      game <- h2h_games[i]
      game_odds <- dt[market == "h2h" & 
                     game_date == game$game_date & 
                     home_team == game$home_team & 
                     away_team == game$away_team]
      
      if (nrow(game_odds) == 2) {
        implied_sum <- sum(1/game_odds$price)
        if (implied_sum < 0.95 || implied_sum > 1.15) {
          log_message(sprintf("Game %s vs %s has unusual implied probability sum: %.3f",
                              game$home_team, game$away_team, implied_sum), "DEBUG")
        }
      }
    }
  }
}
