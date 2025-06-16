# API CLIENT FOR NHL DATA RETRIEVAL ---------------------------------------
# Purpose: Unified API client with caching and retry logic
# Author: Professional implementation with httr
# Last Updated: 2025-12-19

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(data.table)
  library(R6)
})

# Source common utilities - use more robust path detection
if (!exists("nhl_config")) {
  # Try relative path first
  if (file.exists("utils/common.R")) {
    source("utils/common.R")
  } else if (file.exists("common.R")) {
    source("common.R")
  } else {
    stop("Cannot find common.R - please ensure working directory is correct")
  }
}

# API CLIENT CLASS ---------------------------------------------------------

APIClient <- R6Class("APIClient",
  private = list(
    base_url = NULL,
    # headers = NULL,
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
    initialize = function(base_url, # headers = list(), 
                          timeout = nhl_config$timeout,
                          use_cache = TRUE, cache_duration = 3600) {
      private$base_url <- sub("/$", "", base_url)  # Remove trailing slash
      # private$headers <- headers
      private$timeout <- timeout
      private$cache_duration <- cache_duration
      
      if (use_cache) {
        private$cache <- new.env(hash = TRUE)
      }
      
      log_message(sprintf("API client initialized for: %s", private$base_url), "DEBUG")
    },
    
    # Make GET request
    get = function(endpoint, query = list(), use_cache = TRUE) {
      url <- private$build_url(endpoint)
      
      # Check cache first
      if (use_cache) {
        cached_data <- private$check_cache(url)
        if (!is.null(cached_data)) {
          return(cached_data)
        }
      }
      
      # Make request with retry
      response <- with_retry(
        function() {
          GET(
            url,
            query = query,
            # add_headers(.headers = private$headers),
            timeout(private$timeout)
          )
        },
        context = sprintf("API request to %s", url)
      )
      
      if (is.null(response)) {
        return(NULL)
      }
      
      # Check status
      if (status_code(response) != 200) {
        log_message(sprintf("API request failed with status %d: %s",
                            status_code(response), url), "ERROR")
        return(NULL)
      }
      
      # Parse response
      content_text <- content(response, "text", encoding = "UTF-8")
      
      data <- tryCatch({
        fromJSON(content_text, simplifyDataFrame = TRUE)
      }, error = function(e) {
        log_message(sprintf("Failed to parse JSON response: %s", e$message), "ERROR")
        NULL
      })
      
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

# NHL API CLIENT -----------------------------------------------------------

#' Create NHL API client instance
#' @param use_cache Whether to enable caching
#' @return APIClient instance
create_nhl_api_client <- function(use_cache = TRUE) {
  APIClient$new(
    base_url = nhl_config$nhl_api_base,
    # headers = list(Accept = "application/json"),
    use_cache = use_cache,
    cache_duration = 3600  # 1 hour for NHL data
  )
}

# ODDS API CLIENT ----------------------------------------------------------

#' Create Odds API client instance
#' @param use_cache Whether to enable caching
#' @return APIClient instance
create_odds_api_client <- function(use_cache = TRUE) {
  if (is.null(nhl_config$odds_api_key) || nhl_config$odds_api_key == "") {
    log_message("ODDS_API_KEY environment variable not set", "ERROR")
    return(NULL)
  }
  
  APIClient$new(
    base_url = nhl_config$odds_api_base,
    # headers = list("Accept" = "application/json"),
    use_cache = use_cache,
    cache_duration = 600  # 10 minutes for odds (changes frequently)
  )
}

# CONVENIENCE FUNCTIONS ----------------------------------------------------

#' Make API request with automatic client selection
#' @param endpoint API endpoint
#' @param api_type Type of API ("nhl" or "odds")
#' @param query Query parameters
#' @param use_cache Whether to use cache
#' @return Parsed API response
api_get <- function(endpoint, api_type = "nhl", query = list(), use_cache = TRUE) {
  # Get or create appropriate client
  client_name <- paste0(api_type, "_api_client")
  
  if (!exists(client_name, envir = .GlobalEnv)) {
    if (api_type == "nhl") {
      assign(client_name, create_nhl_api_client(use_cache), envir = .GlobalEnv)
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
