# SHARED UTILITIES FOR ESPN EV RETRIEVAL -----------------------------------
# Purpose: Common helpers and configuration for ESPN scripts
# Author: OpenAI ChatGPT
# Last Updated: 2025-06-20

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(here)
})

# CONFIGURATION ------------------------------------------------------------

espn_config <- list(
  data_dir = here("data", "ESPN"),
  log_file = NULL,

  espn_api_base = "https://sports.core.api.espn.com/v2/sports",
  odds_api_base = "https://api.the-odds-api.com",
  odds_api_key = Sys.getenv("ODDS_API_KEY"),

  retry_max = 3,
  retry_delay = 2,
  timeout = 30
)

# LOGGING FUNCTIONS --------------------------------------------------------

init_logging <- function(script_name = "espn_ev") {
  log_dir <- file.path(espn_config$data_dir, "logs")
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)

  log_file <- file.path(log_dir,
                        sprintf("%s_%s.log", script_name,
                                format(Sys.time(), "%Y%m%d_%H%M%S")))

  espn_config$log_file <<- log_file

  cat(sprintf("ESPN %s Script Log\n", toupper(script_name)), file = log_file)
  cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n",
      file = log_file, append = TRUE)
  cat(strrep("=", 60), "\n\n", file = log_file, append = TRUE)

  return(log_file)
}

log_message <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  entry <- sprintf("[%s] %s: %s", timestamp, level, msg)

  if (interactive() || level %in% c("ERROR", "WARN")) {
    if (level == "ERROR") {
      message(entry)
    } else if (level == "SUCCESS") {
      cat("\033[32m", entry, "\033[0m\n", sep = "")
    } else if (level == "WARN") {
      cat("\033[33m", entry, "\033[0m\n", sep = "")
    } else {
      cat(entry, "\n")
    }
  }

  if (!is.null(espn_config$log_file)) {
    cat(entry, "\n", file = espn_config$log_file, append = TRUE)
  }
}

# DATE UTILITIES -----------------------------------------------------------

parse_date_args <- function(dates = NULL) {
  if (is.null(dates)) {
    args <- commandArgs(trailingOnly = TRUE)
    if (length(args) > 0 && args[1] != "delay") {
      dates <- args[!args %in% "delay"]
    } else {
      dates <- c(Sys.Date(), Sys.Date() + 1)
      log_message("No dates specified, using today and tomorrow")
    }
  }

  if (is.character(dates)) {
    dates <- tryCatch(as.Date(dates), error = function(e) {
      log_message(sprintf("Invalid date format: %s", paste(dates, collapse = ", ")),
                  "ERROR")
      stop("Dates must be in YYYY-MM-DD format")
    })
  }

  dates <- sort(unique(as.Date(dates)))

  log_message(sprintf("Processing %d date(s): %s",
                      length(dates),
                      paste(format(dates, "%Y-%m-%d"), collapse = ", ")))

  return(dates)
}

check_delay_arg <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if ("delay" %in% args) {
    log_message("Delaying execution by 5 minutes...", "INFO")
    Sys.sleep(300)
    log_message("Resuming execution", "INFO")
  }
  invisible()
}

# TEAM NAME STANDARDIZATION ------------------------------------------------

standardize_team_name <- function(names) {
  if (is.null(names) || length(names) == 0) return(character(0))
  out <- toupper(gsub("\\.", "", names))
  out <- iconv(out, from = "UTF-8", to = "ASCII//TRANSLIT")
  out <- gsub("[^A-Z0-9 ]", "", out)
  trimws(out)
}

# CALCULATION FUNCTIONS ----------------------------------------------------

calculate_ev <- function(line, win_prob) {
  ifelse(is.na(line) | is.na(win_prob) | line <= 0 | win_prob <= 0 | win_prob > 1,
         NA_real_, line * win_prob - 1)
}

calculate_kelly <- function(line, win_prob) {
  ifelse(is.na(line) | is.na(win_prob) | line <= 1 | win_prob <= 0 | win_prob >= 1,
         NA_real_, win_prob - ((1 - win_prob) / (line - 1)))
}

# RETRY WRAPPER ------------------------------------------------------------

with_retry <- function(fn, ..., max_retries = espn_config$retry_max,
                       delay = espn_config$retry_delay, context = "operation") {
  for (attempt in 1:max_retries) {
    res <- tryCatch(fn(...), error = function(e) {
      if (attempt < max_retries) {
        log_message(sprintf("%s failed (attempt %d/%d): %s. Retrying...",
                            context, attempt, max_retries, e$message), "WARN")
        Sys.sleep(delay * attempt)
        NULL
      } else {
        log_message(sprintf("%s failed after %d attempts: %s",
                            context, max_retries, e$message), "ERROR")
        NULL
      }
    })
    if (!is.null(res)) return(res)
  }
  NULL
}

# DIRECTORY MANAGEMENT -----------------------------------------------------

ensure_directories <- function(subdirs = c("expected_value", "scores", "logs")) {
  for (s in subdirs) {
    p <- file.path(espn_config$data_dir, s)
    if (!dir.exists(p)) {
      dir.create(p, showWarnings = FALSE, recursive = TRUE)
      log_message(sprintf("Created directory: %s", p), "DEBUG")
    }
  }
}

