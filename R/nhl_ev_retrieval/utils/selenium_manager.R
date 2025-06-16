# SELENIUM SESSION MANAGER FOR NHL SCRAPING -------------------------------
# Purpose: Reusable Selenium session management with connection pooling
# Author: Professional implementation with R6 classes
# Last Updated: 2025-12-19

suppressPackageStartupMessages({
  library(R6)
  library(RSelenium)
  library(data.table)
})

# Source common utilities
source(file.path(dirname(dirname(sys.frame(1)$ofile)), "utils", "common.R"))

# DOCKER MANAGEMENT FUNCTIONS ----------------------------------------------

#' Check if Docker is available and running
#' @return TRUE if Docker is available, FALSE otherwise
check_docker_available <- function() {
  docker_path <- nhl_config$docker_path
  
  result <- tryCatch({
    system2(docker_path, "version", stdout = FALSE, stderr = FALSE)
  }, error = function(e) {
    log_message("Docker not found or not accessible", "ERROR")
    return(1)
  })
  
  return(result == 0)
}

#' Stop containers using specific port
#' @param port Port number to check
stop_containers_on_port <- function(port = nhl_config$selenium_port) {
  if (!check_docker_available()) return(invisible())
  
  # Find containers using the port
  cmd <- sprintf("ps --filter 'publish=%d' -q", port)
  containers <- system2(nhl_config$docker_path, cmd, stdout = TRUE, stderr = FALSE)
  
  if (length(containers) > 0 && nchar(containers[1]) > 0) {
    log_message(sprintf("Stopping %d existing container(s) on port %d", 
                        length(containers), port), "DEBUG")
    
    for (container_id in containers) {
      system2(nhl_config$docker_path, c("stop", container_id), 
              stdout = FALSE, stderr = FALSE)
      system2(nhl_config$docker_path, c("rm", container_id), 
              stdout = FALSE, stderr = FALSE)
    }
  }
}

# SELENIUM SESSION CLASS ---------------------------------------------------

SeleniumSession <- R6Class("SeleniumSession",
  private = list(
    container_id = NULL,
    driver = NULL,
    active = FALSE,
    start_time = NULL,
    page_count = 0,
    
    # Start Docker container
    start_container = function() {
      if (!check_docker_available()) {
        stop("Docker is not available or not running")
      }
      
      # Clean up any existing containers
      stop_containers_on_port()
      
      # Start new container
      cmd <- sprintf(
        'run -d -p %d:4444 --shm-size="%s" %s',
        nhl_config$selenium_port,
        nhl_config$docker_memory,
        nhl_config$docker_image
      )
      
      log_message("Starting Selenium Docker container", "DEBUG")
      
      output <- system2(nhl_config$docker_path, cmd, stdout = TRUE, stderr = TRUE)
      
      if (length(output) > 0 && nchar(output[1]) >= 12) {
        private$container_id <- substr(output[1], 1, 12)
        log_message(sprintf("Started container: %s", private$container_id), "DEBUG")
        
        # Wait for container to be ready
        Sys.sleep(5)
        return(TRUE)
      } else {
        log_message("Failed to start Docker container", "ERROR")
        return(FALSE)
      }
    },
    
    # Stop Docker container
    stop_container = function() {
      if (!is.null(private$container_id)) {
        log_message(sprintf("Stopping container: %s", private$container_id), "DEBUG")
        
        system2(nhl_config$docker_path, 
                c("stop", private$container_id), 
                stdout = FALSE, stderr = FALSE)
        
        system2(nhl_config$docker_path, 
                c("rm", private$container_id), 
                stdout = FALSE, stderr = FALSE)
        
        private$container_id <- NULL
      }
    },
    
    # Create remote driver connection
    create_driver = function() {
      remDr <- with_retry(
        remoteDriver,
        remoteServerAddr = "localhost",
        port = nhl_config$selenium_port,
        browserName = "firefox",
        context = "Selenium connection"
      )
      
      if (is.null(remDr)) return(NULL)
      
      # Open browser
      result <- with_retry(
        function() {
          remDr$open(silent = TRUE)
          return(TRUE)
        },
        context = "Browser open"
      )
      
      if (!result) return(NULL)
      
      # Set timeouts
      remDr$setTimeout(type = "page load", milliseconds = nhl_config$timeout * 1000)
      remDr$setTimeout(type = "implicit", milliseconds = 8000)
      
      return(remDr)
    }
  ),
  
  public = list(
    # Initialize session
    initialize = function() {
      log_message("Initializing Selenium session", "DEBUG")
    },
    
    # Start session
    start = function() {
      if (private$active) {
        log_message("Session already active", "DEBUG")
        return(TRUE)
      }
      
      # Start container
      if (!private$start_container()) {
        return(FALSE)
      }
      
      # Create driver
      private$driver <- private$create_driver()
      if (is.null(private$driver)) {
        private$stop_container()
        return(FALSE)
      }
      
      private$active <- TRUE
      private$start_time <- Sys.time()
      log_message("Selenium session started successfully", "SUCCESS")
      return(TRUE)
    },
    
    # Navigate to URL
    navigate = function(url) {
      if (!private$active) {
        log_message("Session not active", "ERROR")
        return(NULL)
      }
      
      log_message(sprintf("Navigating to: %s", url), "DEBUG")
      
      result <- with_retry(
        function() {
          private$driver$navigate(url)
          private$page_count <- private$page_count + 1
          return(TRUE)
        },
        context = sprintf("Navigation to %s", url)
      )
      
      return(result)
    },
    
    # Get page source
    get_page_source = function() {
      if (!private$active) {
        log_message("Session not active", "ERROR")
        return(NULL)
      }
      
      source <- with_retry(
        function() private$driver$getPageSource()[[1]],
        context = "Get page source"
      )
      
      return(source)
    },
    
    # Navigate to multiple URLs and extract data
    navigate_batch = function(urls, extract_fn, delay = 2) {
      if (!private$active) {
        if (!self$start()) {
          return(NULL)
        }
      }
      
      results <- list()
      total_urls <- length(urls)
      
      log_message(sprintf("Processing %d URLs in batch", total_urls), "INFO")
      
      for (i in seq_along(urls)) {
        url <- urls[i]
        
        # Navigate
        if (!self$navigate(url)) {
          log_message(sprintf("Failed to navigate to URL %d/%d", i, total_urls), "WARN")
          results[[as.character(i)]] <- NULL
          next
        }
        
        # Wait for page load
        Sys.sleep(delay)
        
        # Get page source
        page_source <- self$get_page_source()
        if (is.null(page_source)) {
          results[[as.character(i)]] <- NULL
          next
        }
        
        # Extract data
        result <- tryCatch({
          extract_fn(page_source)
        }, error = function(e) {
          log_message(sprintf("Extraction failed for URL %d/%d: %s", 
                              i, total_urls, e$message), "WARN")
          NULL
        })
        
        results[[as.character(i)]] <- result
        
        # Progress update
        if (i %% 5 == 0) {
          log_message(sprintf("Progress: %d/%d URLs processed", i, total_urls), "INFO")
        }
      }
      
      log_message(sprintf("Batch processing complete: %d successes, %d failures",
                          sum(!sapply(results, is.null)),
                          sum(sapply(results, is.null))), "INFO")
      
      return(results)
    },
    
    # Clean up session
    cleanup = function() {
      if (private$active) {
        log_message("Cleaning up Selenium session", "DEBUG")
        
        # Report session stats
        if (!is.null(private$start_time)) {
          duration <- difftime(Sys.time(), private$start_time, units = "mins")
          log_message(sprintf("Session duration: %.1f minutes, %d pages visited",
                              as.numeric(duration), private$page_count), "INFO")
        }
        
        # Close browser
        if (!is.null(private$driver)) {
          tryCatch({
            private$driver$close()
          }, error = function(e) {
            log_message("Error closing browser", "DEBUG")
          })
          private$driver <- NULL
        }
        
        # Stop container
        private$stop_container()
        
        private$active <- FALSE
        log_message("Selenium session cleaned up", "DEBUG")
      }
    },
    
    # Check if session is active
    is_active = function() {
      return(private$active)
    },
    
    # Get session statistics
    get_stats = function() {
      list(
        active = private$active,
        container_id = private$container_id,
        start_time = private$start_time,
        page_count = private$page_count,
        duration_mins = if (!is.null(private$start_time)) {
          as.numeric(difftime(Sys.time(), private$start_time, units = "mins"))
        } else NA
      )
    }
  )
)

# SELENIUM SESSION POOL (for future use) -----------------------------------

SeleniumPool <- R6Class("SeleniumPool",
  private = list(
    sessions = list(),
    max_sessions = 2
  ),
  
  public = list(
    get_session = function() {
      # Find available session or create new one
      for (session in private$sessions) {
        if (!session$is_active()) {
          session$start()
          return(session)
        }
      }
      
      # Create new session if under limit
      if (length(private$sessions) < private$max_sessions) {
        session <- SeleniumSession$new()
        session$start()
        private$sessions <- c(private$sessions, list(session))
        return(session)
      }
      
      # Return first session if at limit
      return(private$sessions[[1]])
    },
    
    cleanup_all = function() {
      for (session in private$sessions) {
        session$cleanup()
      }
      private$sessions <- list()
    }
  )
)
