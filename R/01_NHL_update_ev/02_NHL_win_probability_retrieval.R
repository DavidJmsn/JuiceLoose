# RETRIEVE NHL WIN PROBABILITIES ----------------------------------------------
# Purpose: Retrieve NHL win probabilities from MoneyPuck with robust error handling
# Author: David Jamieson (optimized version)
# E-mail: david.jmsn@icloud.com
# Last Updated: 2025-05-31

# SETUP ------------------------------------------------------------------------

# Suppress startup messages
suppressPackageStartupMessages({
  library(RSelenium)
  library(rvest)
  library(dplyr)
  library(stringr)
  library(tibble)
  library(lubridate)
})

# CONFIGURATION ----------------------------------------------------------------

config <- list(
  # Selenium configuration
  selenium_host     = "localhost",
  selenium_port     = 4444L,
  browser_name      = "firefox",
  docker_image      = "selenium/standalone-firefox:3.141.59",
  docker_memory     = "2g",
  docker_path       = "/opt/homebrew/bin/docker",
  
  # Scraping configuration
  base_url          = "https://moneypuck.com/index.html",
  page_load_timeout = 15,  # seconds (increased for reliability)
  implicit_wait     = 8,   # seconds (increased for dynamic content)
  additional_wait   = 3,   # extra wait after page load
  max_retries       = 3,
  retry_delay       = 2,   # seconds
  
  # Output configuration
  output_dir        = file.path(Sys.getenv("HOME"), "data/NHL/win_probability"),
  log_file          = "nhl_win_probabilities_retrieval.log"
)

# Create output directory if it doesn't exist
dir.create(config$output_dir, showWarnings = FALSE, recursive = TRUE)

# LOGGING FUNCTIONS ------------------------------------------------------------

log_message <- function(message, level = "INFO") {
  timestamp <- format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
  log_entry <- sprintf("[%s] %s: %s", timestamp, level, message)
  
  # Print to console based on level
  if (level == "ERROR") {
    message(log_entry)
  } else if (level != "DEBUG") {  # Skip DEBUG messages in console
    cat(log_entry, "\n")
  }
  
  # Write to log file
  log_path <- file.path(config$output_dir, config$log_file)
  cat(log_entry, "\n", file = log_path, append = TRUE)
}

# DOCKER MANAGEMENT FUNCTIONS --------------------------------------------------

#' Start Docker container for Selenium
#' @return Container ID or NULL on failure
start_selenium_container <- function() {
  log_message("Starting Selenium Docker container")
  
  # Check if Docker is available
  docker_check <- tryCatch({
    system2(config$docker_path, "version", stdout = FALSE, stderr = FALSE)
  }, error = function(e) {
    log_message("Docker not found or not running", "ERROR")
    return(NULL)
  })
  
  if (!is.null(docker_check) && docker_check != 0) {
    log_message("Docker is not running", "ERROR")
    return(NULL)
  }
  
  # Stop any existing containers on the same port
  stop_existing_containers()
  
  # Start new container
  cmd <- sprintf(
    'run -d -p %d:4444 --shm-size="%s" %s',
    config$selenium_port,
    config$docker_memory,
    config$docker_image
  )
  
  container_id <- tryCatch({
    system2(config$docker_path, cmd, stdout = TRUE, stderr = TRUE)
  }, error = function(e) {
    log_message(sprintf("Failed to start container: %s", e$message), "ERROR")
    return(NULL)
  })
  
  if (length(container_id) > 0 && nchar(container_id[1]) > 10) {
    # Extract container ID (first 12 characters)
    container_id <- substr(container_id[1], 1, 12)
    log_message(sprintf("Started container: %s", container_id))
    
    # Wait for container to be ready
    Sys.sleep(5)
    
    return(container_id)
  }
  
  return(NULL)
}

#' Stop Docker container
#' @param container_id Container ID to stop
stop_selenium_container <- function(container_id = NULL) {
  if (is.null(container_id)) {
    # Get running container
    container_id <- get_running_container_id()
  }
  
  if (!is.null(container_id) && nchar(container_id) > 0) {
    log_message(sprintf("Stopping container: %s", container_id))
    system2(config$docker_path, c("stop", container_id), 
            stdout = FALSE, stderr = FALSE)
    
    # Also remove the container
    system2(config$docker_path, c("rm", container_id), 
            stdout = FALSE, stderr = FALSE)
  }
}

#' Stop any existing containers on the Selenium port
stop_existing_containers <- function() {
  # Find containers using port 4444
  cmd <- sprintf("ps --filter 'publish=%d' -q", config$selenium_port)
  existing <- system2(config$docker_path, cmd, stdout = TRUE, stderr = FALSE)
  
  if (length(existing) > 0 && nchar(existing[1]) > 0) {
    log_message("Stopping existing containers on port 4444", "WARN")
    for (container in existing) {
      stop_selenium_container(container)
    }
  }
}

#' Get running container ID
#' @return Container ID or NULL
get_running_container_id <- function() {
  container_info <- system2(config$docker_path, "ps", stdout = TRUE, stderr = FALSE)
  
  if (length(container_info) > 1) {
    # Parse container ID from second line
    container_id <- str_split(container_info[2], "\\s+")[[1]][1]
    return(container_id)
  }
  
  return(NULL)
}

# SELENIUM CONNECTION FUNCTIONS ------------------------------------------------

#' Create Selenium remote driver with retry logic
#' @param max_retries Maximum number of connection attempts
#' @return Remote driver object or NULL on failure
create_remote_driver <- function(max_retries = config$max_retries) {
  for (attempt in 1:max_retries) {
    tryCatch({
      remDr <- remoteDriver(
        remoteServerAddr = config$selenium_host,
        port = config$selenium_port,
        browserName = config$browser_name
      )
      
      # Try to open the browser
      remDr$open(silent = TRUE)
      
      # Set timeouts
      remDr$setTimeout(type = "page load", milliseconds = config$page_load_timeout * 1000)
      remDr$setTimeout(type = "implicit", milliseconds = config$implicit_wait * 1000)
      
      log_message("Connected to Selenium successfully")
      return(remDr)
      
    }, error = function(e) {
      log_message(sprintf("Connection attempt %d/%d failed: %s", 
                          attempt, max_retries, e$message), "WARN")
      
      if (attempt < max_retries) {
        Sys.sleep(config$retry_delay * attempt)
      }
    })
  }
  
  log_message("Failed to connect to Selenium after all retries", "ERROR")
  return(NULL)
}

# SCRAPING FUNCTIONS -----------------------------------------------------------

#' Extract win probability data from MoneyPuck page
#' @param html Parsed HTML of the page
#' @return Data frame with win probabilities
extract_win_probability_data <- function(html) {
  games_list <- list()
  
  # Try multiple selectors for game rows
  game_rows <- html %>%
    html_nodes("table tbody tr, #includedContent table tbody tr, .game-row")
  
  if (length(game_rows) == 0) {
    log_message("No game rows found with standard selectors", "WARN")
    return(data.frame())
  }
  
  for (i in seq_along(game_rows)) {
    row <- game_rows[[i]]
    
    tryCatch({
      # Extract team logos/names
      team_imgs <- row %>%
        html_nodes("img[alt]") %>%
        html_attr("alt")
      
      # Skip if we don't have exactly 2 teams
      if (length(team_imgs) != 2) next
      
      away_team <- team_imgs[1]
      home_team <- team_imgs[2]
      
      # Extract probabilities
      # Look for percentage values in h2 elements or table cells
      prob_elements <-  row %>%
        html_nodes("h2") %>%
        html_text() %>%
        str_trim() %>%
        str_extract("\\d+(?:\\.\\d+)?%")
      
      # Remove NA values
      percentages <- prob_elements[!is.na(prob_elements)]
      
      if (length(percentages) >= 2) {
        away_prob <- percentages[1]
        home_prob <- percentages[2]
      } else {
        # Try alternative extraction method
        prob_cells <- row %>%
          html_nodes(".probability, .win-prob, td.text-center")
        
        if (length(prob_cells) >= 2) {
          away_prob <- html_text(prob_cells[1], trim = TRUE)
          home_prob <- html_text(prob_cells[2], trim = TRUE)
          
          # Ensure they have % sign
          if (!grepl("%", away_prob)) away_prob <- paste0(away_prob, "%")
          if (!grepl("%", home_prob)) home_prob <- paste0(home_prob, "%")
        } else {
          next  # Skip this game if we can't find probabilities
        }
      }
      
      # Extract game time/status
      time_elements <- row %>%
        html_nodes("td") %>%
        html_text(trim = TRUE)
      
      game_time <- NA_character_
      for (elem in time_elements) {
        if (grepl("\\d{1,2}:\\d{2}\\s*(PM|AM|ET)", elem)) {
          game_time <- elem
          break
        } else if (grepl("Final|LIVE|remaining", elem, ignore.case = TRUE)) {
          game_time <- elem
          break
        }
      }
      
      if (is.na(game_time)) game_time <- "TBD"
      
      # Clean team names (remove periods)
      away_team <- gsub("\\.", "", away_team)
      home_team <- gsub("\\.", "", home_team)
      
      games_list[[length(games_list) + 1]] <- list(
        away_team = away_team,
        home_team = home_team,
        away_prob = away_prob,
        home_prob = home_prob,
        game_time = game_time
      )
      
      log_message(sprintf("Found game: %s (%s) @ %s (%s) - %s",
                          away_team, away_prob,
                          home_team, home_prob,
                          game_time), "DEBUG")
      
    }, error = function(e) {
      log_message(sprintf("Error extracting game data from row %d: %s", 
                          i, e$message), "DEBUG")
    })
  }
  
  if (length(games_list) == 0) {
    return(data.frame())
  }
  
  # Create data frame with both team perspectives
  win_prob_df <- bind_rows(
    # Away team rows
    tibble(
      game = sapply(games_list, function(x) paste0(x$away_team, " at ", x$home_team)),
      away = sapply(games_list, function(x) x$away_team),
      home = sapply(games_list, function(x) x$home_team),
      team = sapply(games_list, function(x) x$away_team),
      win_probability = sapply(games_list, function(x) x$away_prob),
      game_time = sapply(games_list, function(x) x$game_time)
    ),
    # Home team rows
    tibble(
      game = sapply(games_list, function(x) paste0(x$away_team, " at ", x$home_team)),
      away = sapply(games_list, function(x) x$away_team),
      home = sapply(games_list, function(x) x$home_team),
      team = sapply(games_list, function(x) x$home_team),
      win_probability = sapply(games_list, function(x) x$home_prob),
      game_time = sapply(games_list, function(x) x$game_time)
    )
  ) %>%
    arrange(game, team)
  
  return(win_prob_df)
}

#' Scrape win probabilities for a single date
#' @param date Date to check for games
#' @return Data frame with win probabilities
scrape_win_probabilities <- function(date = Sys.Date()) {
  container_id <- NULL
  remDr <- NULL
  date_str <- format(date, format = "%Y-%m-%d")
  
  # Ensure cleanup happens
  on.exit({
    if (!is.null(remDr)) {
      tryCatch({
        remDr$close()
        log_message("Closed Selenium browser", "DEBUG")
      }, error = function(e) {})
    }
    if (!is.null(container_id)) {
      stop_selenium_container(container_id)
      log_message("Stopped Docker container", "DEBUG")
    }
  })
  
  # Start Docker container
  container_id <- start_selenium_container()
  if (is.null(container_id)) {
    stop("Failed to start Selenium container")
  }
  
  # Create remote driver
  remDr <- create_remote_driver()
  if (is.null(remDr)) {
    stop("Failed to connect to Selenium")
  }
  
  # Construct URL with date parameter
  url_with_date <- sprintf("%s?date=%s", config$base_url, date_str)
  
  # Navigate to MoneyPuck
  log_message(sprintf("Navigating to %s", url_with_date))
  tryCatch({
    remDr$navigate(url_with_date)
  }, error = function(e) {
    stop("Failed to navigate to MoneyPuck: ", e$message)
  })
  
  # Wait for page to load
  Sys.sleep(config$implicit_wait)
  
  # Additional wait for dynamic content
  if (config$additional_wait > 0) {
    log_message(sprintf("Waiting additional %d seconds for dynamic content", 
                        config$additional_wait), "DEBUG")
    Sys.sleep(config$additional_wait)
  }
  
  # Get page source
  page_source <- remDr$getPageSource()
  page <- read_html(page_source[[1]])
  
  # Extract win probabilities
  win_prob_df <- extract_win_probability_data(page)
  
  if (nrow(win_prob_df) > 0) {
    # Add date and retrieval time
    win_prob_df <- win_prob_df %>%
      mutate(
        date = date_str,
        retrieval_time = format(Sys.time(), format = "%Y-%m-%dT%H:%M:%SZ")
      )
  } else {
    log_message(sprintf("No games found for %s. This could be an off-day or outside MoneyPuck's date range.", 
                        date_str), "WARN")
  }
  
  # Save HTML for debugging if environment variable is set
  if (Sys.getenv("NHL_DEBUG_MODE") == "TRUE") {
    debug_file <- file.path(config$output_dir, sprintf("debug_probabilities_%s.html", date_str))
    writeLines(as.character(page), debug_file)
    log_message(sprintf("Saved page HTML to %s for debugging", debug_file), "DEBUG")
  }
  
  return(win_prob_df)
}

# MAIN EXECUTION ---------------------------------------------------------------

#' Retrieve NHL win probabilities for specified dates
#' @param dates Date or vector of dates to retrieve probabilities for. 
#'              Can be Date objects or character strings in "YYYY-MM-DD" format.
#'              If NULL (default), retrieves today's probabilities.
#' @param save_file Whether to save results to CSV (default: TRUE)
#' @return Data frame with win probabilities for all dates (invisibly)
#' @examples
#' # Get today's probabilities
#' retrieve_win_probabilities()
#' 
#' # Get probabilities for specific date
#' retrieve_win_probabilities("2025-06-01")
#' 
#' # Get probabilities for multiple dates
#' retrieve_win_probabilities(c("2025-06-01", "2025-06-02", "2025-06-03"))
#' 
#' # Get probabilities for next week
#' retrieve_win_probabilities(Sys.Date() + 0:6)
#' 
#' # Get historical playoff probabilities
#' playoff_dates <- seq(as.Date("2025-05-01"), as.Date("2025-05-15"), by = "day")
#' retrieve_win_probabilities(playoff_dates)
#' 
#' # Get probabilities without saving
#' probs <- retrieve_win_probabilities(dates = Sys.Date(), save_file = FALSE)
retrieve_win_probabilities <- function(dates = NULL, save_file = TRUE) {
  # Default to today if no dates provided
  if (is.null(dates)) {
    dates <- Sys.Date()
    log_message("No dates specified, retrieving probabilities for today")
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
  
  log_message(sprintf("Starting win probability retrieval for %d date(s): %s", 
                      length(dates), 
                      paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")))
  
  # Initialize list to store probabilities from all dates
  all_probabilities_list <- list()
  
  # Process each date
  for (i in seq_along(dates)) {
    current_date <- dates[i]
    
    log_message(sprintf("Processing date %d of %d: %s", 
                        i, length(dates), format(current_date, format = "%Y-%m-%d")))
    
    # Scrape probabilities for this date
    date_probabilities <- tryCatch({
      scrape_win_probabilities(current_date)
    }, error = function(e) {
      log_message(sprintf("Failed to retrieve probabilities for %s: %s", 
                          format(current_date, format = "%Y-%m-%d"), e$message), "ERROR")
      return(data.frame())
    })
    
    if (!is.null(date_probabilities) && nrow(date_probabilities) > 0) {
      all_probabilities_list[[length(all_probabilities_list) + 1]] <- date_probabilities
    }
    
    # Add a small delay between dates to avoid overwhelming the server
    if (i < length(dates)) {
      log_message("Pausing between dates...", "DEBUG")
      Sys.sleep(2)
    }
  }
  
  # Combine all probabilities
  if (length(all_probabilities_list) == 0) {
    log_message("No win probabilities found for any of the specified dates")
    return(invisible(NULL))
  }
  
  all_probabilities <- bind_rows(all_probabilities_list)
  
  # Remove any duplicate entries (same team, same game, same date)
  all_probabilities <- all_probabilities %>%
    distinct(date, game, team, .keep_all = TRUE) %>%
    arrange(date, game, team)
  
  log_message(sprintf("Retrieved probabilities for %d teams across %d games on %d date(s)", 
                      nrow(all_probabilities), 
                      length(unique(paste(all_probabilities$date, all_probabilities$game))),
                      length(unique(all_probabilities$date))))
  
  # Save to file if requested
  if (save_file) {
    # Create filename with date range
    if (length(dates) == 1) {
      date_suffix <- format(dates[1], format = "%Y-%m-%d")
    } else {
      date_suffix <- sprintf("%s_to_%s", 
                             format(min(dates), format = "%Y-%m-%d"),
                             format(max(dates), format = "%Y-%m-%d"))
    }
    
    filename <- sprintf("%s_win_probabilities_%s.csv",
                        format(Sys.time(), format = "%Y%m%d_%H%M%S"),
                        date_suffix)
    output_path <- file.path(config$output_dir, filename)
    
    write.csv(all_probabilities, output_path, row.names = FALSE)
    log_message(sprintf("Win probabilities saved to: %s", output_path))
  }
  
  # Return data frame invisibly
  invisible(all_probabilities)
}

# TEST FUNCTION ----------------------------------------------------------------

#' Test win probability retrieval with detailed debugging
#' @param date Date to test (default: tomorrow)
#' @param save_html Whether to save HTML for debugging (default: TRUE)
#' @export
test_win_probabilities <- function(date = Sys.Date() + 1, save_html = TRUE) {
  # Enable debug mode temporarily
  old_debug <- Sys.getenv("NHL_DEBUG_MODE")
  if (save_html) {
    Sys.setenv(NHL_DEBUG_MODE = "TRUE")
  }
  
  cat("\n=== Testing Win Probability Retrieval ===\n")
  cat("Date:", format(date, format = "%Y-%m-%d"), "\n")
  cat("Debug mode:", ifelse(save_html, "ENABLED", "DISABLED"), "\n")
  cat("Check the console output for detailed debugging information\n\n")
  
  # Run retrieval
  result <- tryCatch({
    retrieve_win_probabilities(date, save_file = FALSE)
  }, error = function(e) {
    log_message(sprintf("Test failed with error: %s", e$message), "ERROR")
    NULL
  }, finally = {
    # Restore debug mode
    Sys.setenv(NHL_DEBUG_MODE = old_debug)
  })
  
  if (!is.null(result) && nrow(result) > 0) {
    cat("\n✓ Test successful! Found", nrow(result), "team entries.\n\n")
    
    # Display summary
    cat("Games found:\n")
    unique_games <- unique(result$game)
    for (game in unique_games) {
      cat("  •", game, "\n")
      game_data <- result %>% filter(game == !!game)
      cat("    - Away:", game_data$win_probability[1], "\n")
      cat("    - Home:", game_data$win_probability[2], "\n")
    }
    
    cat("\nFull results:\n")
    print(result)
    
    # Check for data quality issues
    unique_probs <- unique(result$win_probability)
    if (length(unique_probs) < nrow(result) / 2) {
      cat("\n⚠ WARNING: Found duplicate probabilities. This might indicate an extraction issue.\n")
      cat("Unique probabilities found:", paste(unique_probs, collapse = ", "), "\n")
    }
    
    # Check if probabilities sum to ~100%
    for (game in unique_games) {
      game_data <- result %>% filter(game == !!game)
      if (nrow(game_data) == 2) {
        prob_sum <- sum(as.numeric(gsub("%", "", game_data$win_probability)))
        if (abs(prob_sum - 100) > 2) {
          cat("\n⚠ WARNING: Probabilities for", game, "sum to", prob_sum, "% (expected ~100%)\n")
        }
      }
    }
    
  } else {
    cat("\n✗ Test failed - no data retrieved.\n")
    cat("This could mean:\n")
    cat("  • No games scheduled for this date\n")
    cat("  • Date is outside MoneyPuck's supported range\n")
    cat("  • Website structure has changed\n")
    cat("  • Connection issues\n")
    cat("\nCheck the log file for more information:\n")
    cat("  ", file.path(config$output_dir, config$log_file), "\n")
    
    if (save_html) {
      debug_file <- file.path(config$output_dir, 
                              sprintf("debug_probabilities_%s.html", format(date, format = "%Y-%m-%d")))
      if (file.exists(debug_file)) {
        cat("\nDebug HTML saved to:\n  ", debug_file, "\n")
      }
    }
  }
  
  invisible(result)
}

# If run non-interactively, retrieve today's probabilities
if (!interactive()) {
  retrieve_win_probabilities()
}