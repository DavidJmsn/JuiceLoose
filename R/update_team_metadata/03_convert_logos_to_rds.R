# CONVERT LOGOS TO .RDS FILE ----------------------------------------------

# The purpose of this script is to turn a folder full of team logos and transform it 
# into a single .RDS file that can be used in other scripts.
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

# Load required libraries
required_packages <- c("png", "yaml", "tools")
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
output_rds <- file.path(data_dir, paste0(tolower(league), "_logos_preloaded.rds"))

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

# Function to create team name mapping
createTeamMapping <- function(teams_yaml) {
  mapping <- list()
  
  for (team_abbrev in names(teams_yaml)) {
    team_info <- teams_yaml[[team_abbrev]]
    
    # Create various possible team name formats that might appear in filenames
    display_name <- team_info$displayName
    location <- team_info$location
    name <- team_info$name
    
    # Convert to uppercase and replace spaces/special chars with underscores
    possible_names <- c(
      toupper(gsub("[^A-Za-z0-9]", "_", gsub("\\.", "", display_name))),
      toupper(gsub("[^A-Za-z0-9]", "_", gsub("\\.", "", paste(location, name)))),
      toupper(gsub("[^A-Za-z0-9]", "_", gsub("\\.", "", name))),
      team_abbrev
    )
    
    # Remove duplicates and empty strings
    possible_names <- unique(possible_names[possible_names != ""])
    
    # Map each possible name to the team abbreviation
    for (possible_name in possible_names) {
      mapping[[possible_name]] <- team_abbrev
    }
  }
  
  return(mapping)
}

# Function to get team colors from YAML
getTeamColors <- function(team_name, teams_yaml, team_mapping) {
  # Try to find the team abbreviation using the mapping
  team_abbrev <- team_mapping[[team_name]]
  
  if (is.null(team_abbrev)) {
    warning(paste("Could not find team colors for:", team_name))
    return(list(
      primary_color = NA,
      alternate_color = NA,
      display_name = team_name,
      abbreviation = NA
    ))
  }
  
  team_info <- teams_yaml[[team_abbrev]]
  
  # Handle potential missing color fields
  primary_color <- if (!is.null(team_info$color)) paste0("#", team_info$color) else NA
  alternate_color <- if (!is.null(team_info$alternateColor)) paste0("#", team_info$alternateColor) else NA
  
  return(list(
    primary_color = primary_color,
    alternate_color = alternate_color,
    display_name = team_info$displayName,
    abbreviation = team_info$abbreviation,
    location = team_info$location,
    team_name = team_info$name,
    slug = team_info$slug
  ))
}

# Function to generate league-specific usage examples
generateUsageExamples <- function(league) {
  league_lower <- tolower(league)
  
  # Sample team based on league
  sample_team <- switch(league,
                        NHL = "EDMONTON_OILERS",
                        NBA = "LOS_ANGELES_LAKERS",
                        NFL = "NEW_ENGLAND_PATRIOTS",
                        MLB = "NEW_YORK_YANKEES",
                        "SAMPLE_TEAM"
  )
  
  examples <- paste0(
    "# USAGE EXAMPLES ----------------------------------------------------------\n",
    "# How to use the preloaded logos and colors in your scripts:\n\n",
    "# # Load the data (do this once at the start of your script)\n",
    "# ", league_lower, "_data <- readRDS(\"data/", league, "/team_metadata/", 
    league_lower, "_logos_preloaded.rds\")\n",
    "# \n",
    "# # Access a specific logo\n",
    "# team_name <- \"", sample_team, "\"  # or whatever the team name format is\n",
    "# team_logo <- ", league_lower, "_data$teams[[team_name]]$logos[[\"default\"]]$image\n",
    "# \n",
    "# # Get team colors\n",
    "# team_colors <- ", league_lower, "_data$teams[[team_name]]$colors\n",
    "# primary_color <- team_colors$primary_color      # e.g., \"#fc4c02\"\n",
    "# alternate_color <- team_colors$alternate_color  # e.g., \"#000000\"\n",
    "# display_name <- team_colors$display_name        # e.g., \"", 
    gsub("_", " ", sample_team), "\"\n",
    "# \n",
    "# # Get all teams\n",
    "# all_teams <- names(", league_lower, "_data$teams)\n",
    "# \n",
    "# # Get metadata\n",
    "# creation_date <- ", league_lower, "_data$metadata$creation_date\n",
    "# \n",
    "# # Example: Create a plot with team colors\n",
    "# # plot(..., col = team_colors$primary_color)\n"
  )
  
  return(examples)
}

# EXECUTE SCRIPT ----------------------------------------------------------
message("\nStarting to preload ", league, " logos with team colors...")
message("================================================")

# Step 0: Load team data from YAML
message("Loading team data from YAML...")
teams_yaml <- tryCatch({
  yaml::read_yaml(teams_yaml_path)
}, error = function(e) {
  stop("Failed to load teams YAML: ", e$message)
})
team_mapping <- createTeamMapping(teams_yaml)
message("Loaded data for ", length(teams_yaml), " teams")

# Step 1: Get list of all PNG files
logo_files <- list.files(logos_dir, pattern = "\\.png$", full.names = FALSE)
logo_paths <- file.path(logos_dir, logo_files)

if (length(logo_files) == 0) {
  stop("No logo files found in ", logos_dir)
}

message("Found ", length(logo_files), " logo files")

# Step 2: Initialize storage structure
logos_data <- list()
success_count <- 0
failed_count <- 0
color_matches <- 0
color_misses <- 0

# Step 3: Read each logo file
message("\nReading logo files and matching colors...")
pb <- txtProgressBar(min = 0, max = length(logo_files), style = 3)

for (i in seq_along(logo_files)) {
  filename <- logo_files[i]
  filepath <- logo_paths[i]
  
  # Extract team name and logo type
  team_name <- getTeamName(filename)
  logo_type <- getLogoType(filename)
  
  # Read the logo
  logo_data <- readLogoFile(filepath, team_name, logo_type)
  
  # Get team colors
  team_colors <- getTeamColors(team_name, teams_yaml, team_mapping)
  
  # Store in nested structure: logos_data[[team_name]][[logo_type]]
  if (is.null(logos_data[[team_name]])) {
    logos_data[[team_name]] <- list(
      colors = team_colors,
      logos = list()
    )
    
    # Track color matching success
    if (!is.na(team_colors$abbreviation)) {
      color_matches <- color_matches + 1
    } else {
      color_misses <- color_misses + 1
    }
  }
  
  logos_data[[team_name]]$logos[[logo_type]] <- logo_data
  
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
  league = league,
  creation_date = Sys.Date(),
  creation_time = Sys.time(),
  total_logos = length(logo_files),
  successful_reads = success_count,
  failed_reads = failed_count,
  color_matches = color_matches,
  color_misses = color_misses,
  teams = names(logos_data),
  logo_types = unique(unlist(lapply(logos_data, function(x) names(x$logos)))),
  source_directory = logos_dir,
  yaml_source = teams_yaml_path
)

# Step 5: Combine everything into final structure
final_data <- list(
  teams = logos_data,
  metadata = summary_metadata,
  team_mapping = team_mapping
)

# Step 6: Save as RDS file
message("\n\nSaving to RDS file...")
saveRDS(final_data, output_rds)
file_size_mb <- round(file.size(output_rds) / 1024 / 1024, 2)

# Step 7: Display summary
message("\n================================================")
message("PRELOAD SUMMARY FOR ", league)
message("================================================")
message("Successfully read: ", success_count, " logos")
message("Failed to read: ", failed_count, " logos")
message("Color matches: ", color_matches, " teams")
message("Color misses: ", color_misses, " teams")
message("Number of teams: ", length(logos_data))
message("Logo types found: ", paste(summary_metadata$logo_types, collapse = ", "))
message("RDS file size: ", file_size_mb, " MB")
message("Saved to: ", output_rds)

# Step 8: Show sample of structure
message("\nSample of data structure:")
if (length(logos_data) > 0) {
  sample_team <- names(logos_data)[1]
  message("  Team: ", sample_team)
  message("  Display Name: ", logos_data[[sample_team]]$colors$display_name)
  if (!is.na(logos_data[[sample_team]]$colors$primary_color)) {
    message("  Primary Color: ", logos_data[[sample_team]]$colors$primary_color)
    message("  Alternate Color: ", logos_data[[sample_team]]$colors$alternate_color)
  } else {
    message("  Colors: Not available")
  }
  message("  Available logos: ", paste(names(logos_data[[sample_team]]$logos), collapse = ", "))
}

# Show any teams that didn't get color matches
if (color_misses > 0) {
  message("\nTeams without color matches:")
  for (team_name in names(logos_data)) {
    if (is.na(logos_data[[team_name]]$colors$abbreviation)) {
      message("  - ", team_name)
    }
  }
}

# Write usage examples to a separate file
usage_file <- file.path(data_dir, paste0(tolower(league), "_logos_usage_examples.txt"))
writeLines(generateUsageExamples(league), usage_file)
message("\nUsage examples written to: ", usage_file)

message("\nScript 03 completed successfully for ", league, "!")

# Return success status for master script
if (exists("master_mode") && master_mode) {
  script_result <- list(
    success = TRUE,
    total_logos = length(logo_files),
    successful_reads = success_count,
    failed_reads = failed_count,
    league = league,
    rds_size_mb = file_size_mb
  )
}

# END ---------------------------------------------------------------------