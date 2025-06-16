# UPDATE LOGOS FROM ESPN --------------------------------------------------

# The purpose of this script is to download up to date logos for teams
# Now supports multiple leagues: NHL, NBA, NFL, MLB

# Author: David Jamieson
# E-mail: david.jmsn@icloud.com
# Initialized: 2025-05-29
# Updated: 2025-05-30 for multi-league support

# SETUP -------------------------------------------------------------------
# This script can be run standalone or called from a master script
# If called from master, league will be provided

# Check if being run standalone or from master script
if (!exists("league")) {
  # Running standalone - set defaults
  rm(list = setdiff(ls(), "league"))
  
  # Default to NHL if not specified
  league <- "NHL"
  
  # Validate league
  valid_leagues <- c("NHL", "NBA", "NFL", "MLB")
  if (!league %in% valid_leagues) {
    stop("Invalid league specified. Must be one of: ", paste(valid_leagues, collapse = ", "))
  }
}

# Load libraries with error handling
required_packages <- c("yaml", "httr", "tools")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    stop(paste("Required package", pkg, "is not installed. Please install it using install.packages('", pkg, "')"))
  }
}
library(here)

# VARIABLES ---------------------------------------------------------------
# Define paths
data_dir <- here("data", league, "team_metadata")
logos_dir <- file.path(data_dir, "logos")
teams_yaml_path <- file.path(data_dir, "teams.yml")

# Create logos directory if it doesn't exist
if (!dir.exists(logos_dir)) {
  dir.create(logos_dir, recursive = TRUE)
  message("Created logos directory: ", logos_dir)
}

# FUNCTIONS ---------------------------------------------------------------
# Function to load teams data from YAML
loadTeamsData <- function(yaml_path) {
  if (!file.exists(yaml_path)) {
    stop("Teams YAML file not found at: ", yaml_path)
  }
  
  teams_data <- tryCatch({
    yaml.load_file(yaml_path)
  }, error = function(e) {
    stop("Failed to load YAML file: ", e$message)
  })
  
  return(teams_data)
}

# Function to generate filename from logo metadata using full team name
generateLogoFilename <- function(team_info, logo_info) {
  # Get the full team name and convert to uppercase
  team_name_upper <- gsub("\\.","",toupper(team_info$displayName))
  
  # Replace spaces with underscores for filename
  team_name_file <- gsub(" ", "_", team_name_upper)
  
  # Extract rel attributes (excluding "full" as it's common to all)
  rel_attrs <- unlist(logo_info$rel)
  rel_attrs <- rel_attrs[!rel_attrs %in% c("full", "default")]
  
  # Build filename
  if (length(rel_attrs) == 0) {
    # Basic logo (just "full" and "default")
    filename <- paste0(team_name_file, ".png")
  } else {
    # Special variants (dark, scoreboard, etc.)
    suffix <- paste(toupper(rel_attrs), collapse = "_")
    filename <- paste0(team_name_file, "_", suffix, ".png")
  }
  
  return(filename)
}

# Function to download a single logo
downloadLogo <- function(url, filepath, team_name, logo_type) {
  # Make download request with timeout
  response <- tryCatch({
    GET(url, 
        timeout(30),
        write_disk(filepath, overwrite = TRUE))
  }, error = function(e) {
    return(list(error = e$message))
  })
  
  # Check if download was successful
  if (!is.null(response$error)) {
    message("    ✗ Failed to download ", logo_type, " logo: ", response$error)
    return(FALSE)
  }
  
  if (status_code(response) == 200) {
    # Verify file was created and has content
    if (file.exists(filepath) && file.size(filepath) > 0) {
      message("    ✓ Downloaded ", logo_type, " logo: ", basename(filepath))
      return(TRUE)
    } else {
      message("    ✗ Failed to save ", logo_type, " logo: File empty or not created")
      unlink(filepath)  # Remove empty file
      return(FALSE)
    }
  } else {
    message("    ✗ Failed to download ", logo_type, " logo: HTTP ", status_code(response))
    unlink(filepath)  # Remove partial file
    return(FALSE)
  }
}

# Function to process logos for a single team
processTeamLogos <- function(team_abbr, team_info, delay = 0.5) {
  message("\nProcessing ", team_info$displayName, " (", team_abbr, ")...")
  
  # Check if logos exist
  if (is.null(team_info$logos) || length(team_info$logos) == 0) {
    message("  ⚠ No logos found for ", team_info$displayName)
    return(list(success = 0, failed = 0))
  }
  
  success_count <- 0
  failed_count <- 0
  
  # Process each logo
  for (i in seq_along(team_info$logos)) {
    logo <- team_info$logos[[i]]
    
    # Skip if no href
    if (is.null(logo$href) || logo$href == "") {
      message("  ⚠ Logo ", i, " has no URL")
      failed_count <- failed_count + 1
      next
    }
    
    # Generate filename using full team name
    filename <- generateLogoFilename(team_info, logo)
    filepath <- file.path(logos_dir, filename)
    
    # Determine logo type for messages
    rel_attrs <- unlist(logo$rel)
    logo_type <- paste(rel_attrs[rel_attrs != "full"], collapse = " ")
    if (logo_type == "default") logo_type <- "default"
    
    # Download the logo
    if (downloadLogo(logo$href, filepath, team_info$displayName, logo_type)) {
      success_count <- success_count + 1
    } else {
      failed_count <- failed_count + 1
    }
    
    # Small delay to be respectful to the server
    Sys.sleep(delay)
  }
  
  return(list(success = success_count, failed = failed_count))
}

# Function to validate downloaded images
validateDownloads <- function(logos_dir) {
  files <- list.files(logos_dir, pattern = "\\.png$", full.names = TRUE)
  
  if (length(files) == 0) {
    return(list(valid = 0, invalid = 0, files = character()))
  }
  
  valid_count <- 0
  invalid_count <- 0
  invalid_files <- character()
  
  for (file in files) {
    size <- file.size(file)
    if (size > 1000) {  # Assume valid PNG is at least 1KB
      valid_count <- valid_count + 1
    } else {
      invalid_count <- invalid_count + 1
      invalid_files <- c(invalid_files, basename(file))
    }
  }
  
  return(list(
    valid = valid_count, 
    invalid = invalid_count, 
    invalid_files = invalid_files
  ))
}

# Function to create a mapping file for reference
createMappingFile <- function(teams_data, logos_dir) {
  mapping_data <- data.frame(
    abbreviation = character(),
    full_name = character(),
    default_logo = character(),
    dark_logo = character(),
    scoreboard_logo = character(),
    scoreboard_dark_logo = character(),
    stringsAsFactors = FALSE
  )
  
  for (abbr in names(teams_data)) {
    team_info <- teams_data[[abbr]]
    team_name_file <- gsub(" ", "_", gsub("\\.","",toupper(team_info$displayName)))
    
    new_row <- data.frame(
      abbreviation = abbr,
      full_name = team_info$displayName,
      default_logo = paste0(team_name_file, ".png"),
      dark_logo = paste0(team_name_file, "_DARK.png"),
      scoreboard_logo = paste0(team_name_file, "_SCOREBOARD.png"),
      scoreboard_dark_logo = paste0(team_name_file, "_SCOREBOARD_DARK.png"),
      stringsAsFactors = FALSE
    )
    
    mapping_data <- rbind(mapping_data, new_row)
  }
  
  # Save mapping file
  mapping_file <- file.path(logos_dir, "team_logo_mapping.csv")
  write.csv(mapping_data, mapping_file, row.names = FALSE)
  message("\nCreated mapping file: ", mapping_file)
  
  return(mapping_data)
}

# EXECUTE SCRIPT ----------------------------------------------------------
message("\nStarting ", league, " team logos download...")
message("================================================")

# Step 1: Load teams data
message("\nLoading teams data from: ", teams_yaml_path)
teams_data <- tryCatch({
  loadTeamsData(teams_yaml_path)
}, error = function(e) {
  stop("Failed to load teams data: ", e$message)
})
message("Successfully loaded data for ", length(teams_data), " teams")

# Step 2: Set download delay based on league (more teams = smaller delay)
download_delay <- switch(league,
                         NHL = 0.5,
                         NBA = 0.5,
                         NFL = 0.4,  # NFL has 32 teams
                         MLB = 0.5,
                         0.5  # default
)

# Step 3: Download logos for each team
total_success <- 0
total_failed <- 0
teams_processed <- 0
teams_with_errors <- 0

for (team_abbr in names(teams_data)) {
  result <- tryCatch({
    processTeamLogos(team_abbr, teams_data[[team_abbr]], delay = download_delay)
  }, error = function(e) {
    message("\n✗ Error processing ", team_abbr, ": ", e$message)
    return(list(success = 0, failed = 0))
  })
  
  total_success <- total_success + result$success
  total_failed <- total_failed + result$failed
  teams_processed <- teams_processed + 1
  
  if (result$failed > 0) {
    teams_with_errors <- teams_with_errors + 1
  }
}

# Step 4: Validate downloads
message("\n================================================")
message("Validating downloaded files...")
validation <- validateDownloads(logos_dir)

# Step 5: Create mapping file for reference
message("\nCreating team logo mapping file...")
mapping <- createMappingFile(teams_data, logos_dir)

# Step 6: Summary report
message("\n================================================")
message("DOWNLOAD SUMMARY FOR ", league)
message("================================================")
message("Teams processed: ", teams_processed, " / ", length(teams_data))
message("Teams with errors: ", teams_with_errors)
message("Logos downloaded successfully: ", total_success)
message("Logos failed to download: ", total_failed)
message("\nFile validation:")
message("  Valid image files: ", validation$valid)
message("  Invalid/corrupted files: ", validation$invalid)

if (length(validation$invalid_files) > 0) {
  message("\n  Invalid files:")
  for (file in validation$invalid_files) {
    message("    - ", file)
  }
}

# Display sample of downloaded files
message("\nSample of downloaded logos:")
sample_files <- head(list.files(logos_dir, pattern = "\\.png$"), 10)
for (file in sample_files) {
  size_kb <- round(file.size(file.path(logos_dir, file)) / 1024, 1)
  message("  - ", file, " (", size_kb, " KB)")
}

# Show example mapping
message("\nExample team name mappings:")
example_teams <- head(mapping, 5)
for (i in 1:nrow(example_teams)) {
  message("  ", example_teams$abbreviation[i], " → ", example_teams$default_logo[i])
}

message("\nAll logos saved to: ", logos_dir)
message("Mapping file saved as: team_logo_mapping.csv")
message("\nScript 02 completed successfully for ", league, "!")

# Return success status and logo count for master script
if (exists("master_mode") && master_mode) {
  script_result <- list(
    success = TRUE,
    logos_downloaded = total_success,
    logos_failed = total_failed,
    league = league
  )
}

# END ---------------------------------------------------------------------