
# UPDATE TEAMS YAML FILE FROM ESPN ----------------------------------------

# The purpose of this script is to update the teams yaml file from ESPNs data

# Author: David Jamieson
# E-mail: david.jmsn@icloud.com
# Initialized: 2025-05-29

# SETUP -------------------------------------------------------------------
# Clean environment 
rm(list = ls())

# Load libraries with error handling
required_packages <- c("jsonlite", "httr", "data.table", "yaml")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    stop(paste("Required package", pkg, "is not installed. Please install it using install.packages('", pkg, "')"))
  }
}

# VARIABLES ---------------------------------------------------------------
# Any hardcoded variables, options etc...
current_date <- format(Sys.Date(), format = "%Y%m%d")

# Create directory if it doesn't exist
data_dir <- "data/NHL/team_metadata/"
if (!dir.exists(data_dir)) {
  dir.create(data_dir, recursive = TRUE)
  message("Created directory: ", data_dir)
}

# Define output paths
output_paths <- list(
  current = file.path(data_dir, "teams.yml"),
  dated = file.path(data_dir, paste0(current_date, "_teams.yml")),
  csv = file.path(data_dir, "teams.csv"),
  csv_dated = file.path(data_dir, paste0(current_date, "_teams.csv"))
)

# FUNCTIONS ---------------------------------------------------------------
# Function to fetch team data from ESPN API
teamDict <- function() {
  # Make API request with timeout
  teamsJSON <- tryCatch({
    GET('https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams', 
        timeout(30))
  }, error = function(e) {
    stop("Network error when fetching team data: ", e$message)
  })
  
  # Check HTTP status
  if (status_code(teamsJSON) == 200) {
    # Parse JSON response
    data <- tryCatch({
      parse_json(content(teamsJSON, "text"))
    }, error = function(e) {
      stop("Failed to parse JSON response: ", e$message)
    })
    return(data)
  } else {
    stop("Failed to fetch teams data: HTTP ", status_code(teamsJSON))
  }
}

# Function to extract teams from the nested structure
extractTeams <- function(api_data) {
  # Navigate through the nested structure with error checking
  if (!is.list(api_data) || !"sports" %in% names(api_data)) {
    stop("Unexpected API response structure: 'sports' field not found")
  }
  
  sports <- api_data[["sports"]]
  if (!is.list(sports) || length(sports) < 1) {
    stop("No sports data found in API response")
  }
  
  leagues <- sports[[1]][["leagues"]]
  if (!is.list(leagues) || length(leagues) < 1) {
    stop("No leagues data found in API response")
  }
  
  teams <- leagues[[1]][["teams"]]
  if (!is.list(teams) || length(teams) == 0) {
    stop("No teams data found in API response")
  }
  
  return(teams)
}

# Function to transform teams list to dictionary keyed by abbreviation
transformToDict <- function(teams_list) {
  # Create new list keyed by team abbreviation
  teams_dict <- list()
  
  # Process each team with error handling
  for (i in seq_along(teams_list)) {
    team <- teams_list[[i]]
    
    # Validate team structure
    if (!is.list(team) || !"team" %in% names(team)) {
      warning(paste("Skipping invalid team entry at index", i))
      next
    }
    
    team_info <- team[["team"]]
    
    # Check for required fields
    if (!all(c("abbreviation", "displayName") %in% names(team_info))) {
      warning(paste("Skipping team with missing fields at index", i))
      next
    }
    
    # Add to dictionary
    abbreviation <- team_info[["abbreviation"]]
    teams_dict[[abbreviation]] <- team_info
  }
  
  # Sanity check: ensure we have teams
  if (length(teams_dict) == 0) {
    stop("No valid teams found after processing")
  }
  
  return(teams_dict)
}

# Function to create CSV data from teams dictionary
createCSVData <- function(teams_dict) {
  # Convert to data.table format
  csv_data <- lapply(teams_dict, function(x) {
    data.table(
      AB = x$abbreviation, 
      Team = x$displayName
    )
  })
  
  # Combine all rows
  csv_table <- rbindlist(csv_data)
  
  # Sort by abbreviation for consistency
  setorder(csv_table, AB)
  
  return(csv_table)
}

# EXECUTE SCRIPT ----------------------------------------------------------
message("Starting NHL teams data fetch and processing...")

# Step 1: Fetch team data from ESPN API
message("Fetching data from ESPN API...")
teams_raw <- tryCatch({
  teamDict()
}, error = function(e) {
  stop("Failed to fetch team data: ", e$message)
})
message("Successfully fetched API data")

# Step 2: Extract teams from nested structure
message("Extracting teams data...")
teams_list <- tryCatch({
  extractTeams(teams_raw)
}, error = function(e) {
  stop("Failed to extract teams: ", e$message)
})
message(paste("Found", length(teams_list), "teams"))

# Step 3: Transform to dictionary format
message("Transforming data structure...")
teams_dict <- tryCatch({
  transformToDict(teams_list)
}, error = function(e) {
  stop("Failed to transform teams data: ", e$message)
})
message(paste("Successfully processed", length(teams_dict), "teams"))

# Step 4: Write YAML files
message("Writing YAML files...")
tryCatch({
  write_yaml(teams_dict, output_paths$current)
  message("  - Written: ", output_paths$current)
  
  write_yaml(teams_dict, output_paths$dated)
  message("  - Written: ", output_paths$dated)
}, error = function(e) {
  stop("Failed to write YAML files: ", e$message)
})

# Step 5: Create and write CSV files
message("Creating CSV files...")
csv_data <- tryCatch({
  createCSVData(teams_dict)
}, error = function(e) {
  stop("Failed to create CSV data: ", e$message)
})

# Sanity check: verify CSV data
if (nrow(csv_data) != length(teams_dict)) {
  warning("CSV row count doesn't match teams count")
}

# Write CSV files with colon separator
tryCatch({
  fwrite(csv_data, output_paths$csv, sep = ':', col.names = FALSE)
  message("  - Written: ", output_paths$csv)
  
  fwrite(csv_data, output_paths$csv_dated, sep = ':', col.names = FALSE)
  message("  - Written: ", output_paths$csv_dated)
}, error = function(e) {
  stop("Failed to write CSV files: ", e$message)
})

# Final sanity checks
message("\nFinal validation:")
message("  - Teams processed: ", length(teams_dict))
message("  - CSV rows written: ", nrow(csv_data))

# Display sample of processed teams
message("\nSample of processed teams:")
sample_teams <- head(names(teams_dict), 5)
for (team_abbr in sample_teams) {
  message("  - ", team_abbr, ": ", teams_dict[[team_abbr]]$displayName)
}

message("\nScript completed successfully!")

# END ---------------------------------------------------------------------