# RETRIEVE NHL STARTING GOALIES ------------------------------------------------
# Purpose: Retrieve NHL starting goalie information from DailyFaceoff with robust error handling
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
  base_url          = "https://www.dailyfaceoff.com/starting-goalies",
  page_load_timeout = 15,   # seconds (increased for reliability)
  implicit_wait     = 8,    # seconds (increased for dynamic content)
  additional_wait   = 3,    # extra wait after page load
  max_retries       = 3,
  retry_delay       = 2,    # seconds
  
  # Output configuration
  output_dir        = file.path(Sys.getenv("HOME"), "data/NHL/starting_goalies"),
  log_file          = "nhl_starting_goalies_retrieval.log"
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

#' Extract starting goalie information from HTML
#' @param html Parsed HTML of the page
#' @return Data frame with starting goalie information
extract_goalie_data <- function(html) {
  goalie_list <- list()
  
  # Find all game articles
  game_articles <- html %>%
    html_nodes("article.w-full")
  
  for (i in seq_along(game_articles)) {
    article <- game_articles[[i]]
    
    tryCatch({
      # Extract teams
      teams_container <- article %>%
        html_node("span.text-center.text-3xl.text-white")
      
      if (is.null(teams_container)) next
      
      teams_text <- html_text(teams_container, trim = TRUE)
      team_parts <- str_split(teams_text, " at ")[[1]] %>% 
        str_trim() %>% 
        Filter(nzchar, .)
      
      if (length(team_parts) < 2) next
      
      away_team <- team_parts[1]
      home_team <- team_parts[2]
      game <- paste(away_team, "at", home_team)
      
      # Extract game time
      game_time <- article %>%
        html_node("span.text-center.text-base.text-white") %>%
        html_text(trim = TRUE)
      
      if (is.null(game_time)) game_time <- NA_character_
      
      # Find goalie sections
      goalie_sections <- article %>%
        html_nodes(".flex-grow.flex-col.justify-start.p-2.xl\\:justify-start.xl\\:flex-row-reverse,
                   .flex-grow.flex-col.justify-start.p-2.xl\\:justify-start.xl\\:flex-row")
      
      for (j in seq_along(goalie_sections)) {
        section <- goalie_sections[[j]]
        
        # Extract goalie name
        goalie_name <- section %>%
          html_node("span.text-center.text-lg.xl\\:text-2xl") %>%
          html_text(trim = TRUE)
        
        if (is.null(goalie_name) || is.na(goalie_name)) next
        
        # Determine which team this goalie belongs to
        is_away <- grepl("reverse", html_attr(section, "class"))
        this_team <- if (is_away) away_team else home_team
        
        # Extract status information
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
          
          # Parse status and update time
          for (span_text in status_spans) {
            if (grepl("\\b(AM|PM|GMT|EST|CST|PST|\\d{4})\\b", span_text)) {
              status_updated <- span_text
            } else if (span_text %in% c("Likely", "Confirmed", "Unconfirmed", "Projected")) {
              status <- span_text
            }
          }
        }
        
        # Add to list
        goalie_list[[length(goalie_list) + 1]] <- list(
          game = game,
          away = toupper(away_team),
          home = toupper(home_team),
          start_time = game_time,
          team = toupper(this_team),
          goalie = goalie_name,
          status = status,
          status_updated = status_updated
        )
      }
      
    }, error = function(e) {
      log_message(sprintf("Error extracting game data: %s", e$message), "DEBUG")
    })
  }
  
  # Convert to data frame
  if (length(goalie_list) > 0) {
    goalie_df <- bind_rows(goalie_list) %>%
      mutate(
        current_time = Sys.time()
      )
    
    # Set timezone attributes
    if (nrow(goalie_df) > 0) {
      attr(goalie_df$start_time, "tzone") <- "America/New_York"
      attr(goalie_df$status_updated, "tzone") <- "America/New_York"
      attr(goalie_df$current_time, "tzone") <- "America/New_York"
    }
    
    return(goalie_df)
  }
  
  return(data.frame())
}

#' Scrape starting goalies for a single date
#' @param date Date to retrieve goalies for
#' @return Data frame with starting goalies
scrape_starting_goalies <- function(date = Sys.Date()) {
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
  
  # Construct URL with date
  url_with_date <- sprintf("%s/%s", config$base_url, date_str)
  log_message(sprintf("Navigating to %s", url_with_date))
  
  tryCatch({
    remDr$navigate(url_with_date)
  }, error = function(e) {
    stop("Failed to navigate to DailyFaceoff: ", e$message)
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
  
  # Extract goalie information
  goalie_df <- extract_goalie_data(page)
  
  if (nrow(goalie_df) > 0) {
    # Add date and retrieval time
    goalie_df <- goalie_df %>%
      mutate(
        date = date_str,
        retrieval_time = format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
      )
  }
  
  # Save HTML for debugging if environment variable is set
  if (Sys.getenv("NHL_DEBUG_MODE") == "TRUE") {
    debug_file <- file.path(config$output_dir, sprintf("debug_goalies_%s.html", date_str))
    writeLines(as.character(page), debug_file)
    log_message(sprintf("Saved page HTML to %s for debugging", debug_file), "DEBUG")
  }
  
  return(goalie_df)
}

# MAIN EXECUTION ---------------------------------------------------------------

#' Retrieve NHL starting goalies for specified dates
#' @param dates Date or vector of dates to retrieve goalies for. 
#'              Can be Date objects or character strings in "YYYY-MM-DD" format.
#'              If NULL (default), retrieves today's goalies.
#' @param save_file Whether to save results to CSV (default: TRUE)
#' @return Data frame with starting goalies for all dates (invisibly)
#' @examples
#' # Get today's starting goalies
#' retrieve_starting_goalies()
#' 
#' # Get starting goalies for specific date
#' retrieve_starting_goalies("2025-06-01")
#' 
#' # Get starting goalies for multiple dates
#' retrieve_starting_goalies(c("2025-06-01", "2025-06-02", "2025-06-03"))
#' 
#' # Get starting goalies for next week
#' retrieve_starting_goalies(Sys.Date() + 0:6)
#' 
#' # Get historical playoff goalies
#' playoff_dates <- seq(as.Date("2025-05-01"), as.Date("2025-05-15"), by = "day")
#' retrieve_starting_goalies(playoff_dates)
#' 
#' # Get goalies without saving
#' goalies <- retrieve_starting_goalies(dates = Sys.Date(), save_file = FALSE)
retrieve_starting_goalies <- function(dates = NULL, save_file = TRUE) {
  # Default to today if no dates provided
  if (is.null(dates)) {
    dates <- Sys.Date()
    log_message("No dates specified, retrieving starting goalies for today")
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
  
  log_message(sprintf("Starting goalie retrieval for %d date(s): %s", 
                      length(dates), 
                      paste(format(dates, format = "%Y-%m-%d"), collapse = ", ")))
  
  # Initialize list to store goalies from all dates
  all_goalies_list <- list()
  
  # Process each date
  for (i in seq_along(dates)) {
    current_date <- dates[i]
    
    log_message(sprintf("Processing date %d of %d: %s", 
                        i, length(dates), format(current_date, format = "%Y-%m-%d")))
    
    # Scrape goalies for this date
    date_goalies <- tryCatch({
      scrape_starting_goalies(current_date)
    }, error = function(e) {
      log_message(sprintf("Failed to retrieve goalies for %s: %s", 
                          format(current_date, format = "%Y-%m-%d"), e$message), "ERROR")
      return(data.frame())
    })
    
    if (!is.null(date_goalies) && nrow(date_goalies) > 0) {
      all_goalies_list[[length(all_goalies_list) + 1]] <- date_goalies
    }
    
    # Add a small delay between dates to avoid overwhelming the server
    if (i < length(dates)) {
      log_message("Pausing between dates...", "DEBUG")
      Sys.sleep(2)
    }
  }
  
  # Combine all goalies
  if (length(all_goalies_list) == 0) {
    log_message("No starting goalies found for any of the specified dates")
    return(invisible(NULL))
  }
  
  all_goalies <- bind_rows(all_goalies_list)
  
  # Remove any duplicate entries (same team, same game, same date)
  all_goalies <- all_goalies %>%
    distinct(date, game, team, .keep_all = TRUE) %>%
    arrange(date, game, team)
  
  log_message(sprintf("Retrieved %d goalie entries across %d games on %d date(s)", 
                      nrow(all_goalies), 
                      length(unique(paste(all_goalies$date, all_goalies$game))),
                      length(unique(all_goalies$date))))
  
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
    
    filename <- sprintf("%s_starting_goalies_%s.csv",
                        format(Sys.time(), format = "%Y%m%d_%H%M%S"),
                        date_suffix)
    output_path <- file.path(config$output_dir, filename)
    
    write.csv(all_goalies, output_path, row.names = FALSE)
    log_message(sprintf("Starting goalies saved to: %s", output_path))
  }
  
  # Return data frame invisibly
  invisible(all_goalies)
}

# TEST FUNCTION ----------------------------------------------------------------

#' Test starting goalie retrieval with detailed debugging
#' @param date Date to test (default: tomorrow)
#' @param save_html Whether to save HTML for debugging (default: TRUE)
#' @export
test_starting_goalies <- function(date = Sys.Date() + 1, save_html = TRUE) {
  # Enable debug mode temporarily
  old_debug <- Sys.getenv("NHL_DEBUG_MODE")
  if (save_html) {
    Sys.setenv(NHL_DEBUG_MODE = "TRUE")
  }
  
  cat("\n=== Testing Starting Goalie Retrieval ===\n")
  cat("Date:", format(date, format = "%Y-%m-%d"), "\n")
  cat("Debug mode:", ifelse(save_html, "ENABLED", "DISABLED"), "\n")
  cat("Check the console output for detailed debugging information\n\n")
  
  # Run retrieval
  result <- tryCatch({
    retrieve_starting_goalies(date, save_file = FALSE)
  }, error = function(e) {
    log_message(sprintf("Test failed with error: %s", e$message), "ERROR")
    NULL
  }, finally = {
    # Restore debug mode
    Sys.setenv(NHL_DEBUG_MODE = old_debug)
  })
  
  if (!is.null(result) && nrow(result) > 0) {
    cat("\n✓ Test successful! Found", nrow(result), "goalie entries.\n\n")
    
    # Display summary
    cat("Games found:\n")
    unique_games <- unique(result$game)
    for (game in unique_games) {
      cat("  •", game, "\n")
      game_data <- result %>% filter(game == !!game)
      for (i in 1:nrow(game_data)) {
        cat("    -", game_data$team[i], ":", game_data$goalie[i], 
            "(", game_data$status[i], ")\n")
      }
    }
    
    cat("\nFull results:\n")
    print(result)
    
    # Check for data quality issues
    if (any(result$status == "Unknown")) {
      cat("\n⚠ WARNING: Some goalies have 'Unknown' status.\n")
    }
    
    if (any(is.na(result$goalie) | result$goalie == "")) {
      cat("\n⚠ WARNING: Some entries have missing goalie names.\n")
    }
    
  } else {
    cat("\n✗ Test failed - no data retrieved.\n")
    cat("This could mean:\n")
    cat("  • No games scheduled for this date\n")
    cat("  • Website structure has changed\n")
    cat("  • Connection issues\n")
    cat("\nCheck the log file for more information:\n")
    cat("  ", file.path(config$output_dir, config$log_file), "\n")
    
    if (save_html) {
      debug_file <- file.path(config$output_dir, 
                              sprintf("debug_goalies_%s.html", format(date, format = "%Y-%m-%d")))
      if (file.exists(debug_file)) {
        cat("\nDebug HTML saved to:\n  ", debug_file, "\n")
      }
    }
  }
  
  invisible(result)
}

# If run non-interactively, retrieve today's goalies
if (!interactive()) {
  retrieve_starting_goalies()
}