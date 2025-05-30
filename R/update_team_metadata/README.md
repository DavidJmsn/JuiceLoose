# JuiceLoose

## ESPN Team Data Update Scripts

A collection of R scripts to fetch and process team metadata (information and logos) from ESPN for multiple sports leagues.

## Overview

This suite of scripts downloads team information and logos from ESPN's API for NHL, NBA, NFL, and MLB teams. The data is saved in multiple formats for easy use in R projects.

## Features

- **Multi-league support**: NHL, NBA, NFL, MLB
- **Automatic logo download**: All logo variants (default, dark, scoreboard)
- **Team colors extraction**: Primary and alternate colors for each team
- **Multiple output formats**: YAML, CSV, and RDS files
- **Error handling**: Robust error handling and validation
- **Master control script**: Run everything with a single command

## Scripts

### 00_master_update_team_data.R (Master Script)
The main control script that orchestrates the entire process. Run this to update all data.

### 01_update_teams_yaml.R
Fetches team information from ESPN's API and saves it as YAML and CSV files.

### 02_update_logos.R
Downloads team logos in all available variants (default, dark, scoreboard, etc.).

### 03_convert_logos_to_rds.R
Converts downloaded logos into a single RDS file with team colors for easy loading in R.

## Directory Structure

```
data/
├── NHL/
│   └── team_metadata/
│       ├── teams.yml
│       ├── teams.csv
│       ├── logos/
│       │   ├── EDMONTON_OILERS.png
│       │   ├── EDMONTON_OILERS_DARK.png
│       │   └── ...
│       └── nhl_logos_preloaded.rds
├── NBA/
│   └── team_metadata/
│       └── ... (same structure)
├── NFL/
│   └── team_metadata/
│       └── ... (same structure)
└── MLB/
    └── team_metadata/
        └── ... (same structure)
```

## Usage

### Quick Start

1. Place all four scripts in your working directory
2. Run the master script:

```r
source("00_master_update_team_data.R")
```

### Configuration

Edit the configuration section in `00_master_update_team_data.R`:

```r
# Select which leagues to process
PROCESS_LEAGUES <- list(
  NHL = TRUE,   # Set to FALSE to skip
  NBA = TRUE,
  NFL = TRUE,
  MLB = TRUE
)

# Advanced options
OPTIONS <- list(
  league_delay = 2,              # Delay between leagues (seconds)
  skip_if_recent = FALSE,        # Skip if data is recent
  recent_threshold_hours = 24,   # Define "recent" in hours
  continue_on_error = TRUE,      # Continue if one league fails
  verbose = TRUE,                # Detailed output
  create_summary = TRUE          # Create summary report
)
```

### Running Individual Scripts

You can also run scripts individually for a specific league:

```r
# Set the league
league <- "NHL"  # or "NBA", "NFL", "MLB"

# Define league configuration
league_config <- list(
  sport = "hockey",     # basketball, football, baseball
  league_code = "nhl"   # nba, nfl, mlb
)

# Run individual scripts
source("01_update_teams_yaml.R")
source("02_update_logos.R")
source("03_convert_logos_to_rds.R")
```

## Using the Data

### Loading Team Information

```r
# Load YAML data
library(yaml)
nhl_teams <- yaml::read_yaml("data/NHL/team_metadata/teams.yml")

# Access team info
oilers_info <- nhl_teams[["EDM"]]
print(oilers_info$displayName)  # "Edmonton Oilers"
print(oilers_info$color)        # Team color hex code
```

### Using Preloaded Logos

```r
# Load the RDS file
nhl_data <- readRDS("data/NHL/team_metadata/nhl_logos_preloaded.rds")

# Get a team's logo
team_logo <- nhl_data$teams[["EDMONTON_OILERS"]]$logos[["default"]]$image

# Get team colors
team_colors <- nhl_data$teams[["EDMONTON_OILERS"]]$colors
primary_color <- team_colors$primary_color    # "#FC4C02"
alternate_color <- team_colors$alternate_color # "#041E42"

# Plot with team colors and logo
library(png)
plot(1:10, col = primary_color, pch = 19, cex = 2)
rasterImage(team_logo, 2, 2, 4, 4)
```

### Available Logo Types

- `default`: Standard team logo
- `dark`: Dark variant (if available)
- `scoreboard`: Simplified version for scoreboards
- `scoreboard_dark`: Dark scoreboard variant

## Requirements

### R Packages

```r
# Required packages
install.packages(c(
  "jsonlite",    # JSON parsing
  "httr",        # HTTP requests
  "data.table",  # Data manipulation
  "yaml",        # YAML file handling
  "png",         # PNG image reading
  "tools"        # File utilities
))
```

### System Requirements

- R version 3.5.0 or higher
- Internet connection for API access
- Approximately 100-200 MB disk space per league

## Troubleshooting

### Common Issues

1. **Network errors**: Check your internet connection and firewall settings
2. **Missing packages**: Install required packages using the command above
3. **API changes**: ESPN may change their API structure; check for script updates

### Error Messages

- `"Failed to fetch team data"`: Network issue or API unavailable
- `"No teams data found"`: API response structure may have changed
- `"Failed to download logo"`: Individual logo URL may be broken

### Validation

After running, check:
- Team count matches expected (30-32 teams per league)
- Logo files are > 1KB (valid images)
- RDS file loads without errors

## Notes

- **Respect ESPN's servers**: The scripts include delays between requests
- **Color accuracy**: Team colors are provided by ESPN and should be official
- **Logo quality**: Logos are downloaded at the quality provided by ESPN
- **Updates**: Run periodically to get roster changes and updated logos

## License

These scripts are provided as-is for personal and educational use. ESPN's data and logos are property of ESPN and the respective teams.

## Author

David Jamieson  
Email: david.jmsn@icloud.com  
Created: 2025-05-30