# MASTER SCRIPT: UPDATE ALL TEAM DATA FROM ESPN ---------------------------

# This master script orchestrates the entire team metadata update process
# for multiple sports leagues (NHL, NBA, NFL, MLB)

# Author: David Jamieson
# E-mail: david.jmsn@icloud.com
# Created: 2025-05-30

# SETUP -------------------------------------------------------------------
rm(list = ls())
library(here)

# USER CONFIGURATION ------------------------------------------------------
# Select which leagues to process
# Set to TRUE to process, FALSE to skip
PROCESS_LEAGUES <- list(
  NHL = TRUE,
  NBA = TRUE,
  NFL = TRUE,
  MLB = TRUE
)

# Advanced options
OPTIONS <- list(
  # Delay between processing leagues (seconds)
  league_delay = 2,
  
  # Skip logo downloads if teams.yml already exists and is recent
  skip_if_recent = FALSE,
  recent_threshold_hours = 24,
  
  # Continue processing other leagues if one fails
  continue_on_error = TRUE,
  
  # Verbose output
  verbose = TRUE,
  
  # Create summary report at the end
  create_summary = TRUE,
  
  # Parallel processing for logos (experimental - set to FALSE if issues)
  parallel_logos = FALSE
)

# ESPN API configurations for each league
LEAGUE_CONFIGS <- list(
  NHL = list(sport = "hockey", league_code = "nhl"),
  NBA = list(sport = "basketball", league_code = "nba"),
  NFL = list(sport = "football", league_code = "nfl"),
  MLB = list(sport = "baseball", league_code = "mlb")
)

# FUNCTIONS ---------------------------------------------------------------
# Function to check if packages are installed
checkPackages <- function() {
  required <- c("jsonlite", "httr", "data.table", "yaml", "png", "tools")
  missing <- required[!sapply(required, requireNamespace, quietly = TRUE)]
  
  if (length(missing) > 0) {
    message("\n⚠ Missing required packages: ", paste(missing, collapse = ", "))
    message("Install them with: install.packages(c(", 
            paste0("\"", missing, "\"", collapse = ", "), "))")
    return(FALSE)
  }
  return(TRUE)
}

# Function to check if data is recent
isDataRecent <- function(league, threshold_hours) {
  yaml_path <- here("data", league, "team_metadata", "teams.yml")
  
  if (!file.exists(yaml_path)) {
    return(FALSE)
  }
  
  file_age_hours <- as.numeric(difftime(Sys.time(), 
                                        file.info(yaml_path)$mtime, 
                                        units = "hours"))
  
  return(file_age_hours < threshold_hours)
}

# Function to run a single script
runScript <- function(script_name, league, league_config) {
  message("\n  Running ", script_name, "...")
  
  # Set up environment for the script
  script_env <- new.env()
  script_env$league <- league
  script_env$league_config <- league_config
  script_env$master_mode <- TRUE
  
  # Source the script in the isolated environment
  tryCatch({
    source(script_name, local = script_env)
    
    # Get result if available
    if (exists("script_result", envir = script_env)) {
      return(script_env$script_result)
    } else {
      return(list(success = TRUE))
    }
  }, error = function(e) {
    message("    ✗ Error: ", e$message)
    return(list(success = FALSE, error = e$message))
  })
}

# Function to process a single league
processLeague <- function(league, league_config, options) {
  message("\n", paste(rep("=", 60), collapse = ""))
  message("Processing ", league, " Team Data")
  message(paste(rep("=", 60), collapse = ""))
  
  # Check if we should skip based on recent data
  if (options$skip_if_recent && isDataRecent(league, options$recent_threshold_hours)) {
    message("Skipping ", league, " - data is recent (less than ", 
            options$recent_threshold_hours, " hours old)")
    return(list(
      league = league,
      status = "skipped",
      reason = "recent data"
    ))
  }
  
  results <- list(
    league = league,
    status = "processing",
    scripts = list()
  )
  
  # Run script 01: Update teams YAML
  result_01 <- runScript(here("R", "update_team_metadata", "01_update_teams_yaml.R"), league, league_config)
  results$scripts$update_yaml <- result_01
  
  if (!result_01$success) {
    results$status <- "failed"
    results$failed_at <- "R/update_team_metadata/01_update_teams_yaml.R"
    return(results)
  }
  
  # Run script 02: Update logos
  result_02 <- runScript(here("R", "update_team_metadata", "02_update_logos.R"), league, league_config)
  results$scripts$update_logos <- result_02
  
  if (!result_02$success) {
    results$status <- "failed"
    results$failed_at <- "R/update_team_metadata/02_update_logos.R"
    return(results)
  }
  
  # Run script 03: Convert logos to RDS
  result_03 <- runScript(here("R", "update_team_metadata", "03_convert_logos_to_rds.R"), league, league_config)
  results$scripts$convert_rds <- result_03
  
  if (!result_03$success) {
    results$status <- "failed"
    results$failed_at <- "R/update_team_metadata/03_convert_logos_to_rds.R"
    return(results)
  }
  
  results$status <- "completed"
  message("\n✓ Successfully completed all scripts for ", league)
  
  return(results)
}

# Function to create summary report
createSummaryReport <- function(all_results, start_time) {
  total_time <- difftime(Sys.time(), start_time, units = "mins")
  
  message("\n", paste(rep("=", 60), collapse = ""))
  message("PROCESSING SUMMARY")
  message(paste(rep("=", 60), collapse = ""))
  message("Total processing time: ", round(total_time, 1), " minutes")
  message("\nLeague Status:")
  
  summary_data <- data.frame(
    League = character(),
    Status = character(),
    Teams = integer(),
    Logos_Downloaded = integer(),
    RDS_Size_MB = numeric(),
    stringsAsFactors = FALSE
  )
  
  for (result in all_results) {
    status_symbol <- switch(result$status,
                            completed = "✓",
                            failed = "✗",
                            skipped = "○",
                            "-"
    )
    
    message("  ", status_symbol, " ", result$league, ": ", result$status)
    
    # Extract data for summary table
    teams_count <- if (!is.null(result$scripts$update_yaml$teams_count)) {
      result$scripts$update_yaml$teams_count
    } else {
      NA
    }
    
    logos_count <- if (!is.null(result$scripts$update_logos$logos_downloaded)) {
      result$scripts$update_logos$logos_downloaded
    } else {
      NA
    }
    
    rds_size <- if (!is.null(result$scripts$convert_rds$rds_size_mb)) {
      result$scripts$convert_rds$rds_size_mb
    } else {
      NA
    }
    
    summary_data <- rbind(summary_data, data.frame(
      League = result$league,
      Status = result$status,
      Teams = teams_count,
      Logos_Downloaded = logos_count,
      RDS_Size_MB = rds_size,
      stringsAsFactors = FALSE
    ))
    
    if (result$status == "failed") {
      message("      Failed at: ", result$failed_at)
    }
  }
  
  # Save summary report
  summary_file <- here("data", paste0("update_summary_",
                                     format(Sys.time(), "%Y%m%d_%H%M%S"),
                                     ".csv"))
  
  tryCatch({
    dir.create("data", showWarnings = FALSE)
    write.csv(summary_data, summary_file, row.names = FALSE)
    message("\nSummary report saved to: ", summary_file)
  }, error = function(e) {
    message("\nFailed to save summary report: ", e$message)
  })
  
  # Display summary statistics
  completed <- sum(summary_data$Status == "completed")
  failed <- sum(summary_data$Status == "failed")
  skipped <- sum(summary_data$Status == "skipped")
  
  message("\nOverall Statistics:")
  message("  Completed: ", completed, " leagues")
  message("  Failed: ", failed, " leagues")
  message("  Skipped: ", skipped, " leagues")
  
  if (completed > 0) {
    total_teams <- sum(summary_data$Teams[summary_data$Status == "completed"], na.rm = TRUE)
    total_logos <- sum(summary_data$Logos_Downloaded[summary_data$Status == "completed"], na.rm = TRUE)
    total_size <- sum(summary_data$RDS_Size_MB[summary_data$Status == "completed"], na.rm = TRUE)
    
    message("\n  Total teams processed: ", total_teams)
    message("  Total logos downloaded: ", total_logos)
    message("  Total RDS size: ", round(total_size, 1), " MB")
  }
}

# MAIN EXECUTION ----------------------------------------------------------
message("ESPN Team Data Update Master Script")
message("===================================")
message("Started at: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))

# Check packages
if (!checkPackages()) {
  stop("Missing required packages. Please install them before running.")
}

# Track start time
start_time <- Sys.time()

# Store all results
all_results <- list()

# Process each selected league
leagues_to_process <- names(PROCESS_LEAGUES)[unlist(PROCESS_LEAGUES)]

if (length(leagues_to_process) == 0) {
  stop("No leagues selected for processing. Set at least one league to TRUE in PROCESS_LEAGUES.")
}

message("\nLeagues to process: ", paste(leagues_to_process, collapse = ", "))

for (league in leagues_to_process) {
  league_config <- LEAGUE_CONFIGS[[league]]
  
  result <- tryCatch({
    processLeague(league, league_config, OPTIONS)
  }, error = function(e) {
    message("\n✗ Critical error processing ", league, ": ", e$message)
    list(
      league = league,
      status = "failed",
      error = e$message
    )
  })
  
  all_results[[league]] <- result
  
  # Check if we should continue after error
  if (result$status == "failed" && !OPTIONS$continue_on_error) {
    message("\nStopping due to error (continue_on_error = FALSE)")
    break
  }
  
  # Delay between leagues
  if (league != tail(leagues_to_process, 1)) {
    if (OPTIONS$verbose) {
      message("\nWaiting ", OPTIONS$league_delay, " seconds before next league...")
    }
    Sys.sleep(OPTIONS$league_delay)
  }
}

# Create summary report
if (OPTIONS$create_summary) {
  createSummaryReport(all_results, start_time)
}

# Final message
message("\n", paste(rep("=", 60), collapse = ""))
message("Master script completed at: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
message(paste(rep("=", 60), collapse = ""))

# Quick reference for accessing the data
message("\nQuick Reference - Access your data:")
for (league in leagues_to_process) {
  if (all_results[[league]]$status == "completed") {
    message("\n", league, ":")
    message("  Teams YAML: data/", league, "/team_metadata/teams.yml")
    message("  Logos RDS:  data/", league, "/team_metadata/", tolower(league), "_logos_preloaded.rds")
  }
}

message("\n✓ All done!")

# END ---------------------------------------------------------------------