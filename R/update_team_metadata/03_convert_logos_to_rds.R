
# CONVERT LOGOS TO .RDS FILE ----------------------------------------------

# The purpose of this script is to turn a folder full of team logos and transform it 
# into a single .RDS file that can be used in other scripts.

# Author: David Jamieson
# E-mail: david.jmsn@icloud.com
# Initialized: 2025-05-29

# SETUP -------------------------------------------------------------------
# Clean environment
rm(list = ls())

# Load required libraries
library(png)
library(yaml)
library(tools)

# VARIABLES ---------------------------------------------------------------
# Define paths
data_dir <- "data/NHL/team_metadata/"
logos_dir <- file.path(data_dir, "logos")
teams_yaml_path <- file.path(data_dir, "teams.yml")
output_rds <- file.path(data_dir, "nhl_logos_preloaded.rds")

# FUNCTIONS ---------------------------------------------------------------
# Function to read and process a single logo
readLogoFile <- function(filepath, team_name, logo_type) {
  tryCatch({
    # Read PNG file
    img <- readPNG(filepath, native = FALSE)
    
    # Get image dimensions
    dims <- dim(img)
    
    # Create metadata
    metadata <- list(
      team_name = team_name,
      logo_type = logo_type,
      width = dims[2],
      height = dims[1],
      channels = ifelse(length(dims) > 2, dims[3], 1),
      file_size = file.size(filepath),
      original_path = filepath
    )
    
    return(list(
      image = img,
      metadata = metadata,
      success = TRUE
    ))
  }, error = function(e) {
    warning(paste("Failed to read", filepath, ":", e$message))
    return(list(
      image = NULL,
      metadata = list(error = e$message),
      success = FALSE
    ))
  })
}

# Function to extract logo type from filename
getLogoType <- function(filename) {
  # Remove .png extension
  name_parts <- gsub("\\.png$", "", filename)
  
  # Check for variants
  if (grepl("_SCOREBOARD_DARK$", name_parts)) {
    return("scoreboard_dark")
  } else if (grepl("_SCOREBOARD$", name_parts)) {
    return("scoreboard")
  } else if (grepl("_DARK$", name_parts)) {
    return("dark")
  } else {
    return("default")
  }
}

# Function to extract team name from filename
getTeamName <- function(filename) {
  # Remove .png extension and variant suffixes
  team_name <- gsub("\\.png$", "", filename)
  team_name <- gsub("_SCOREBOARD_DARK$|_SCOREBOARD$|_DARK$", "", team_name)
  return(team_name)
}

# EXECUTE SCRIPT ----------------------------------------------------------
message("Starting to preload NHL logos...")
message("================================================")

# Step 1: Get list of all PNG files
logo_files <- list.files(logos_dir, pattern = "\\.png$", full.names = FALSE)
logo_paths <- file.path(logos_dir, logo_files)

message("Found ", length(logo_files), " logo files")

# Step 2: Initialize storage structure
logos_data <- list()
success_count <- 0
failed_count <- 0

# Step 3: Read each logo file
message("\nReading logo files...")
pb <- txtProgressBar(min = 0, max = length(logo_files), style = 3)

for (i in seq_along(logo_files)) {
  filename <- logo_files[i]
  filepath <- logo_paths[i]
  
  # Extract team name and logo type
  team_name <- getTeamName(filename)
  logo_type <- getLogoType(filename)
  
  # Read the logo
  logo_data <- readLogoFile(filepath, team_name, logo_type)
  
  # Store in nested structure: logos_data[[team_name]][[logo_type]]
  if (is.null(logos_data[[team_name]])) {
    logos_data[[team_name]] <- list()
  }
  
  logos_data[[team_name]][[logo_type]] <- logo_data
  
  if (logo_data$success) {
    success_count <- success_count + 1
  } else {
    failed_count <- failed_count + 1
  }
  
  # Update progress bar
  setTxtProgressBar(pb, i)
}
close(pb)

# Step 4: Create summary metadata
summary_metadata <- list(
  creation_date = Sys.Date(),
  creation_time = Sys.time(),
  total_logos = length(logo_files),
  successful_reads = success_count,
  failed_reads = failed_count,
  teams = names(logos_data),
  logo_types = unique(unlist(lapply(logos_data, names))),
  source_directory = logos_dir
)

# Step 5: Combine everything into final structure
final_data <- list(
  logos = logos_data,
  metadata = summary_metadata
)

# Step 6: Save as RDS file
message("\n\nSaving to RDS file...")
saveRDS(final_data, output_rds)
file_size_mb <- round(file.size(output_rds) / 1024 / 1024, 2)

# Step 7: Display summary
message("\n================================================")
message("PRELOAD SUMMARY")
message("================================================")
message("Successfully read: ", success_count, " logos")
message("Failed to read: ", failed_count, " logos")
message("Number of teams: ", length(logos_data))
message("Logo types found: ", paste(summary_metadata$logo_types, collapse = ", "))
message("RDS file size: ", file_size_mb, " MB")
message("Saved to: ", output_rds)

# Step 8: Show sample of structure
message("\nSample of data structure:")
sample_team <- names(logos_data)[1]
message("  Team: ", sample_team)
message("  Available logos: ", paste(names(logos_data[[sample_team]]), collapse = ", "))

message("\nScript completed!")

# USAGE EXAMPLES ----------------------------------------------------------
# How to use the preloaded logos in your scripts:

# # Load the logos (do this once at the start of your script)
# nhl_logos <- readRDS("01_data/team_color_codes/nhl_logos_preloaded.rds")
# 
# # Access a specific logo
# edm_logo <- nhl_logos$logos[["EDMONTON_OILERS"]][["default"]]$image
# 
# # Get all teams
# all_teams <- names(nhl_logos$logos)
# 
# # Get metadata
# creation_date <- nhl_logos$metadata$creation_date

# END ---------------------------------------------------------------------