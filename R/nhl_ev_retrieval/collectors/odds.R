# NHL BETTING ODDS COLLECTOR ----------------------------------------------
# Purpose: Efficiently collect NHL betting odds with date range API
# Author: Professional implementation with data.table
# Last Updated: 2025-12-19

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(here)
})

# Source utilities using here()
source(here("R", "nhl_ev_retrieval", "utils", "common.R"))
source(here("R", "nhl_ev_retrieval", "utils", "api_client.R"))

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
    game_info <- games_dt[, .(game_id, id, sport_key, sport_title, commence_time, home_team, away_team)]
    
    # Extract and unnest bookmakers
    bookmakers_list <- list()
    for (i in seq_len(nrow(games_dt))) {
      game_bookmakers <- games_dt$bookmakers[[i]]
      if (!is.null(game_bookmakers) && nrow(game_bookmakers) > 0) {
        # Add game_id and bookmaker_id to track relationships
        game_bookmakers_dt <- as.data.table(game_bookmakers)
        game_bookmakers_dt[, `:=`(game_id = i, bookmaker_id = .I)]
        bookmakers_list[[i]] <- game_bookmakers_dt
      }
    }
    
    if (length(bookmakers_list) == 0) {
      log_message("No bookmakers found in odds data", "WARN")
      return(data.table())
    }
    
    # Combine all bookmakers
    bookmakers_dt <- rbindlist(bookmakers_list, fill = TRUE)
    
    # Step 2: Unnest markets
    markets_list <- list()
    for (i in seq_len(nrow(bookmakers_dt))) {
      bookmaker_markets <- bookmakers_dt$markets[[i]]
      if (!is.null(bookmaker_markets) && nrow(bookmaker_markets) > 0) {
        # bookmaker_markets is already a data.frame of markets
        market_dt <- as.data.table(bookmaker_markets)
        market_dt[, `:=`(
          game_id = bookmakers_dt$game_id[i],
          bookmaker_id = bookmakers_dt$bookmaker_id[i],
          market_id = .I  # Use row number as market_id
        )]
        markets_list[[length(markets_list) + 1]] <- market_dt
      }
    }
    
    if (length(markets_list) == 0) {
      log_message("No markets found in odds data", "WARN")
      return(data.table())
    }
    
    # Combine all markets
    markets_dt <- rbindlist(markets_list, fill = TRUE)
    
    # Step 3: Unnest outcomes
    outcomes_list <- list()
    for (i in seq_len(nrow(markets_dt))) {
      market_outcomes <- markets_dt$outcomes[[i]]
      if (!is.null(market_outcomes) && nrow(market_outcomes) > 0) {
        outcomes_dt <- as.data.table(market_outcomes)
        outcomes_dt[, `:=`(
          game_id = markets_dt$game_id[i],
          bookmaker_id = markets_dt$bookmaker_id[i],
          market_id = markets_dt$market_id[i]
        )]
        outcomes_list[[length(outcomes_list) + 1]] <- outcomes_dt
      }
    }
    
    if (length(outcomes_list) == 0) {
      log_message("No outcomes found in odds data", "WARN")
      return(data.table())
    }
    
    # Combine all outcomes
    outcomes_dt <- rbindlist(outcomes_list, fill = TRUE)
    
    # Step 4: Join everything back together
    # Join outcomes with markets (excluding outcomes column)
    markets_clean <- markets_dt[, !c("outcomes")]
    setnames(markets_clean, "key", "market", skip_absent = TRUE)
    setnames(markets_clean, "last_update", "market_last_update", skip_absent = TRUE)
    
    odds_dt <- merge(outcomes_dt, markets_clean, 
                     by = c("game_id", "bookmaker_id", "market_id"), 
                     all.x = TRUE)
    
    # Join with bookmakers (excluding markets column)
    bookmakers_clean <- bookmakers_dt[, !c("markets")]
    setnames(bookmakers_clean, "key", "book", skip_absent = TRUE)
    setnames(bookmakers_clean, "title", "book_title", skip_absent = TRUE)
    # setnames(bookmakers_clean, "last_update", "book_last_update", skip_absent = TRUE)
    
    odds_dt <- merge(odds_dt, bookmakers_clean,
                     by = c("game_id", "bookmaker_id"),
                     all.x = TRUE)
    
    # Join with game info
    odds_dt <- merge(odds_dt, game_info,
                     by = "game_id",
                     all.x = TRUE)
    
    # Step 5: Clean up and format
    # Remove ID columns used for tracking
    odds_dt[, `:=`(game_id = NULL, bookmaker_id = NULL, market_id = NULL)]
    
    # Parse timestamps
    if ("commence_time" %in% names(odds_dt)) {
      odds_dt[, `:=`(
        game_time = format(as.POSIXct(commence_time, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
                                      format = "%H:%M", tz = "America/New_York"),
        game_date = as.Date(format(as.POSIXct(commence_time, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
                                   format = "%Y-%m-%dT%H:%M:%SZ", tz = "America/New_York"))
      )]
    }
    
    # if ("book_last_update" %in% names(odds_dt)) {
    #   odds_dt[, book_last_update := format(as.POSIXct(book_last_update, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"), tz = "America/New_York")]
    # }
    
    if ("market_last_update" %in% names(odds_dt)) {
      odds_dt[, market_last_update := format(as.POSIXct(market_last_update, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"), tz = "America/New_York")]
    }
    
    # Ensure numeric columns are properly typed
    if ("price" %in% names(odds_dt)) {
      odds_dt[, price := as.numeric(price)]
    }
    
    if ("point" %in% names(odds_dt)) {
      odds_dt[, point := as.numeric(point)]
    }
    
    log_message(sprintf("Successfully parsed %d odds records", nrow(odds_dt)), "DEBUG")
    
    return(odds_dt)
    
  }, error = function(e) {
    log_message(sprintf("Error in parse_odds_efficient: %s", e$message), "ERROR")
    log_message(sprintf("Error details: %s", toString(e)), "DEBUG")
    return(data.table())
  })
}

#' Get best odds per team/market
#' @param h2h_odds data.table with all odds
#' @return data.table with best odds only
get_best_odds <- function(h2h_odds) {
  if (nrow(h2h_odds) == 0) return(h2h_odds)
  
  # Calculate price statistics by team/market
  h2h_odds[, `:=`(
    price_mean = mean(price, na.rm = TRUE),
    price_sd = sd(price, na.rm = TRUE),
    price_max = max(price, na.rm = TRUE),
    price_min = min(price, na.rm = TRUE),
    n_books = .N
  ), by = .(home_team, away_team, market, name, game_date, game_time)]
  
  # Get best price per team/market
  best_odds <- h2h_odds[, .SD[which.max(price)], 
                       by = .(home_team, away_team, market, name, game_date, game_time)]
  
  return(best_odds)
}

# MAIN COLLECTION FUNCTION -------------------------------------------------

#' Collect NHL betting odds for date range
#' @param dates Vector of dates to collect
#' @param markets Character vector of markets (default: "h2h")
#' @param regions Character regions code (default: "us")
#' @return data.table with betting odds
collect_nhl_odds <- function(dates, markets = "h2h", regions = "us") {
  log_message("Starting NHL odds collection", "INFO")
  
  # Validate inputs
  dates <- parse_date_args(dates)
  
  # Check API key
  if (is.null(nhl_config$odds_api_key) || nhl_config$odds_api_key == "") {
    log_message("ODDS_API_KEY environment variable not set", "ERROR")
    return(data.table())
  }
  
  # Get date range
  date_range <- c(min(dates), max(dates)+1)
  
  # Build query parameters
  query_params <- list(
    apiKey = nhl_config$odds_api_key,
    regions = regions,
    markets = paste(markets, collapse = ","),
    oddsFormat = "decimal",
    dateFormat = "iso",
    commenceTimeFrom = format(as.POSIXct(date_range[1], tz = "UTC"), 
                              "%Y-%m-%dT00:00:00Z"),
    commenceTimeTo = format(as.POSIXct(date_range[2] + 1, tz = "UTC") - 1, 
                            "%Y-%m-%dT23:59:59Z")
  )
  
  log_message(sprintf("Fetching odds for date range: %s to %s", 
                      format(date_range[1], format = "%Y-%m-%d"),
                      format(date_range[2], format = "%Y-%m-%d")), "INFO")
  
  # Make API request
  endpoint <- sprintf("/v4/sports/icehockey_nhl/odds")
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
  
  # Get best odds if h2h market
  if ("h2h" %in% markets) {
    h2h_odds <- odds_tmp[market == "h2h"]
    
    if (nrow(h2h_odds) > 0) {
      h2h_best <- get_best_odds(h2h_odds)
      
      # Replace h2h odds with best only
      odds_dt <- rbind(
        odds_tmp[market != "h2h"],
        h2h_best,
        fill = TRUE
      )
    }
  }
  
  # Add metadata
  odds_dt[, retrieved_time := format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ", tz = "America/New_York")]
  
  # Select and order columns
  final_cols <- c(
    "game_date", "game_time", "home_team", "away_team", "name", "market",
    "price", "point", "book", "price_mean", "price_min", "price_sd", "n_books",
     "retrieved_time"
  )
  
  # Keep only columns that exist
  existing_cols <- intersect(final_cols, names(odds_dt))
  odds_dt <- odds_dt[, ..existing_cols]
  
  # Sort
  setorder(odds_dt, game_date, game_time, home_team, away_team, market, name, price, book, retrieved_time)
  
  log_message(sprintf("Retrieved %d odds entries for %d games across %d dates", 
                      nrow(odds_dt),
                      uniqueN(odds_dt[, paste(home_team, away_team)]),
                      uniqueN(odds_dt$game_date)), "SUCCESS")
  
  # # Log market breakdown
  # market_summary <- odds_dt[, .N, by = market]
  # log_message(sprintf("Market breakdown: %s", 
  #                     paste(sprintf("%s=%d", market_summary$market, market_summary$N), 
  #                           collapse = ", ")), "INFO")
  
  # # Log bookmaker count
  # log_message(sprintf("Found odds from %d different bookmakers", 
  #                     uniqueN(odds_dt$book)), "INFO")
  
  return(odds_dt)
}
