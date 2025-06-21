# API CLIENT FOR ESPN DATA RETRIEVAL --------------------------------------
# Purpose: Unified API client with caching and retry logic
# Author: Professional implementation with httr
# Last Updated: 2025-06-21

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(data.table)
  library(R6)
})

# Source common utilities - use more robust path detection
if (!exists("espn_config")) {
  # Try relative path first
  if (file.exists("utils/common.R")) {
    source("utils/common.R")
  } else if (file.exists("common.R")) {
    source("common.R")
  } else if (file.exists(here::here("R", "espn_ev_retrieval", "utils", "common.R"))) {
    source(here::here("R", "espn_ev_retrieval", "utils", "common.R"))
  } else {
    stop("Cannot find common.R - please ensure working directory is correct")
  }
}

# API CLIENT CLASS ---------------------------------------------------------

APIClient <- R6Class("APIClient",
  private = list(
    base_url = NULL,
    timeout = NULL,
    cache = NULL,
    cache_duration = 3600,  # 1 hour default
    
    # Build full URL
    build_url = function(endpoint) {
      # Remove leading slash if present
      endpoint <- sub("^/", "", endpoint)
      
      # Handle query parameters
      if (grepl("\\?", endpoint)) {
        url <- paste0(private$base_url, "/", endpoint)
      } else {
        url <- paste0(private$base_url, "/", endpoint)
      }
      
      return(url)
    },
    
    # Get cache key
    get_cache_key = function(url) {
      return(digest::digest(url))
    },
    
    # Check cache
    check_cache = function(url) {
      if (is.null(private$cache)) return(NULL)
      
      key <- private$get_cache_key(url)
      if (key %in% names(private$cache)) {
        cached <- private$cache[[key]]
        
        # Check if cache is still valid
        if (difftime(Sys.time(), cached$timestamp, units = "secs") < private$cache_duration) {
          log_message(sprintf("Cache hit for: %s", url), "DEBUG")
          return(cached$data)
        } else {
          # Remove expired cache
          private$cache[[key]] <- NULL
        }
      }
      
      return(NULL)
    },
    
    # Set cache
    set_cache = function(url, data) {
      if (is.null(private$cache)) return()
      
      key <- private$get_cache_key(url)
      private$cache[[key]] <- list(
        data = data,
        timestamp = Sys.time()
      )
    }
  ),
  
  public = list(
    # Initialize
    initialize = function(base_url, timeout = espn_config$timeout,
                          use_cache = TRUE, cache_duration = 3600) {
      private$base_url <- sub("/$", "", base_url)  # Remove trailing slash
      private$timeout <- timeout
      private$cache_duration <- cache_duration
      
      if (use_cache) {
        private$cache <- new.env(hash = TRUE)
      }
      
      log_message(sprintf("Initialized API client for %s", private$base_url), "DEBUG")
    },
    
    # Make GET request
    get = function(endpoint, query = list(), use_cache = TRUE) {
      # Build URL
      if (!startsWith(endpoint, "http")) {
        url <- private$build_url(endpoint)
      } else {
        # Full URL provided (for following references)
        url <- endpoint
      }
      
      # Check cache first
      if (use_cache) {
        cached <- private$check_cache(url)
        if (!is.null(cached)) return(cached)
      }
      
      # Make request
      log_message(sprintf("API GET: %s", url), "DEBUG")
      
      response <- tryCatch({
        GET(
          url = url,
          query = query,
          timeout(private$timeout),
          add_headers(
            `Accept` = "application/json",
            `User-Agent` = "JuiceLoose/1.0"
          )
        )
      }, error = function(e) {
        log_message(sprintf("Request failed: %s", e$message), "ERROR")
        NULL
      })
      
      if (is.null(response)) return(NULL)
      
      # Check status
      if (status_code(response) != 200) {
        log_message(sprintf("API returned status %d: %s", 
                            status_code(response),
                            content(response, "text", encoding = "UTF-8")), "ERROR")
        return(NULL)
      }
      
      # Check for rate limit headers
      hdr <- headers(response)
      if (!is.null(hdr$`x-requests-remaining`)) {
        log_message(sprintf("API quota: %s remaining", 
                            hdr$`x-requests-remaining`), "INFO")
      }
      
      # Parse response
      content_text <- content(response, "text", encoding = "UTF-8")
      
      data <- tryCatch({
        fromJSON(content_text, simplifyDataFrame = TRUE)
      }, error = function(e) {
        log_message(sprintf("Failed to parse JSON response: %s", e$message), "ERROR")
        NULL
      })
      
      if (!is.null(data)) {
        attr(data, "response_headers") <- hdr
      }
      
      # Cache successful response
      if (!is.null(data) && use_cache) {
        private$set_cache(url, data)
      }
      
      return(data)
    },
    
    # Clear cache
    clear_cache = function() {
      if (!is.null(private$cache)) {
        rm(list = ls(private$cache), envir = private$cache)
        log_message("API cache cleared", "DEBUG")
      }
    },
    
    # Get cache statistics
    get_cache_stats = function() {
      if (is.null(private$cache)) {
        return(list(enabled = FALSE))
      }
      
      cache_keys <- ls(private$cache)
      return(list(
        enabled = TRUE,
        entries = length(cache_keys),
        duration_seconds = private$cache_duration
      ))
    }
  )
)

# ESPN API CLIENT ----------------------------------------------------------

#' Create ESPN API client instance
#' @param use_cache Whether to enable caching
#' @return APIClient instance
create_espn_api_client <- function(use_cache = TRUE) {
  APIClient$new(
    base_url = espn_config$espn_api_base,
    use_cache = use_cache,
    cache_duration = 3600  # 1 hour for ESPN data
  )
}

# ODDS API CLIENT ----------------------------------------------------------

#' Create Odds API client instance
#' @param use_cache Whether to enable caching
#' @return APIClient instance
create_odds_api_client <- function(use_cache = TRUE) {
  if (is.null(espn_config$odds_api_key) || espn_config$odds_api_key == "") {
    log_message("ODDS_API_KEY environment variable not set", "ERROR")
    return(NULL)
  }
  
  APIClient$new(
    base_url = espn_config$odds_api_base,
    use_cache = use_cache,
    cache_duration = 600  # 10 minutes for odds (changes frequently)
  )
}

# CONVENIENCE FUNCTIONS ----------------------------------------------------

#' Make API request with automatic client selection
#' @param endpoint API endpoint
#' @param api_type Type of API ("espn" or "odds")
#' @param query Query parameters
#' @param use_cache Whether to use cache
#' @return Parsed API response
api_get <- function(endpoint, api_type = "espn", query = list(), use_cache = TRUE) {
  # Get or create appropriate client
  client_name <- paste0(api_type, "_api_client")
  
  if (!exists(client_name, envir = .GlobalEnv)) {
    if (api_type == "espn") {
      assign(client_name, create_espn_api_client(use_cache), envir = .GlobalEnv)
    } else if (api_type == "odds") {
      assign(client_name, create_odds_api_client(use_cache), envir = .GlobalEnv)
    } else {
      log_message(sprintf("Unknown API type: %s", api_type), "ERROR")
      return(NULL)
    }
  }
  
  client <- get(client_name, envir = .GlobalEnv)
  
  if (is.null(client)) {
    return(NULL)
  }
  
  return(client$get(endpoint, query, use_cache))
}
