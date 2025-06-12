# NHL BETTING ANALYSIS DASHBOARD -----------------------------------------
# Purpose: Interactive Shiny dashboard for NHL betting analysis and visualization
# Author: Professional implementation with robust error handling
# Last Updated: 2025-06-01

# SETUP -------------------------------------------------------------------
setwd(Sys.getenv("HOME"))

# Suppress startup messages
suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(bsicons)
  library(bigrquery)
  library(data.table)
  library(dplyr)
  library(tidyr)
  library(plotly)
  library(DT)
  library(lubridate)
  library(tibble)
  library(ggplot2)
  library(ggimage)
  library(png)
  library(base64enc)
  library(rlang)
})

# CONFIGURATION -----------------------------------------------------------

config <- list(
  # Environment settings
  platform = Sys.getenv("PLATFORM", "local"),
  
  # BigQuery settings
  project_id = Sys.getenv("PROJECT_ID"),
  dataset = Sys.getenv("DATASET"),
  service_account = Sys.getenv("service_account"),
  
  # Data paths
  local_data_path = "data/NHL",
  completed_games_table = "completed_games",
  upcoming_games_table = "upcoming_games",
  
  # Visualization settings
  custom_red = "#e74c3c",
  custom_green = "#18bc9c",
  default_gray = "#999999",
  
  # App settings
  app_title = "Willy Snipe?",
  theme_preset = "lux",
  log_file = "nhl_dashboard.log",
  
  # Logo settings
  logo_height = 30,
  logo_size = 0.1,
  
  # Timeout settings
  query_timeout = 30
)

# Create log directory if needed
log_dir <- NULL
if (config$platform == "local") {
  log_dir <- file.path(getwd(), "data/NHL/logs")
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
  }
  config$log_file <- file.path(log_dir, config$log_file)
}

# LOGGING FUNCTIONS -------------------------------------------------------

#' Log message with timestamp and level
#' @param message Message to log
#' @param level Log level (INFO, WARN, ERROR)
log_message <- function(message, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_entry <- sprintf("[%s] %s: %s", timestamp, level, message)
  
  # Print to console based on level
  if (level == "ERROR") {
    message(log_entry)
  } else if (interactive()) {
    cat(log_entry, "\n")
  }
  
  # Write to log file if local and log directory exists
  if (config$platform == "local" && !is.null(config$log_file) && !is.null(log_dir)) {
    tryCatch({
      if (dir.exists(log_dir)) {
        cat(log_entry, "\n", file = config$log_file, append = TRUE)
      }
    }, error = function(e) {
      # Silent fail if can't write to log
    })
  }
}

# UTILITY FUNCTIONS -------------------------------------------------------

#' Import .data pronoun from rlang
.data <- rlang::.data

#' Negate %in% operator
`%ni%` <- Negate(`%in%`)

#' Calculate betting line from expected value and win probability
#'
#' This version accepts vector inputs for EV and winProb. If EV is length 1 and winProb is a longer vector,
#' EV will be recycled to match length(winProb). If lengths are incompatible, it errors out.
#'
#' @param EV    Expected value(s). Either a single numeric or a numeric vector.
#' @param winProb  Win probability(ies). Must be the same length as EV (after possible recycling), numeric, in (0, 1].
#' @return       A numeric vector of the same length as winProb (and EV), with each element 
#'               equal to (EV + 1) / winProb, or NA if winProb or EV is NA or out of bounds.
calculate_line_from_ev <- function(EV, winProb) {
  # Coerce to numeric
  EV      <- as.numeric(EV)
  winProb <- as.numeric(winProb)
  
  # If EV is length 1 but winProb is longer, recycle EV
  if (length(EV) == 1 && length(winProb) > 1) {
    EV <- rep(EV, length(winProb))
  }
  
  # Now ensure lengths match exactly
  if (length(EV) != length(winProb)) {
    stop("`EV` and `winProb` must be the same length, or `EV` must be length 1 so it can be recycled.")
  }
  
  n <- length(winProb)
  # Initialize output vector of NAs
  result <- rep(NA_real_, n)
  
  # Identify positions where both EV and winProb are valid numbers in (0, 1]
  valid_idx <- !is.na(EV) & !is.na(winProb) & (winProb > 0) & (winProb <= 1)
  
  # Vectorized assignment: only fill in the valid spots
  if (any(valid_idx)) {
    result[valid_idx] <- (EV[valid_idx] + 1) / winProb[valid_idx]
  }
  
  return(result)
}


#' Calculate win probability from line and expected value
#' @param EV Expected value
#' @param line Betting line
#' @return Calculated win probability
calculate_prob_from_line <- function(EV, line) {
  if (any(line <= 0, na.rm = TRUE)) {
    log_message("Invalid line provided to calculate_prob_from_line", "WARN")
    result <- (EV + 1) / line
    result[line <= 0] <- NA
    return(result)
  }
  return((EV + 1) / line)
}

# DATA CONNECTION FUNCTIONS -----------------------------------------------

#' Establish BigQuery connection with authentication
#' @return TRUE if successful, FALSE otherwise
establish_bigquery_connection <- function() {
  tryCatch({
    log_message("Establishing BigQuery connection...")
    
    bigrquery::bq_deauth()
    
    if (is.null(config$service_account) || config$service_account == "") {
      log_message("Service account not found", "ERROR")
      return(FALSE)
    }
    
    token <- gargle::credentials_service_account(
      path = config$service_account,
      scopes = c(
        "https://www.googleapis.com/auth/userinfo.email",
        "https://www.googleapis.com/auth/bigquery"
      )
    )
    
    bigrquery::bq_auth(token = token)
    log_message("BigQuery authentication successful")
    return(TRUE)
    
  }, error = function(e) {
    log_message(sprintf("BigQuery connection failed: %s", e$message), "ERROR")
    return(FALSE)
  })
}

#' Execute BigQuery query with error handling
#' @param table_name Name of the table to query
#' @return Data frame or NULL on error
execute_bigquery_query <- function(table_name) {
  tryCatch({
    query <- sprintf("SELECT * FROM `%s.%s.%s`", 
                     config$project_id, 
                     config$dataset, 
                     table_name)
    
    log_message(sprintf("Executing query for table: %s", table_name))
    
    result <- bq_project_query(config$project_id, query) %>% 
      bq_table_download()
    
    log_message(sprintf("Retrieved %d rows from %s", nrow(result), table_name))
    return(result)
    
  }, error = function(e) {
    log_message(sprintf("Query failed for %s: %s", table_name, e$message), "ERROR")
    return(NULL)
  })
}

#' Load data based on platform configuration
#' @return List with completed_games and upcoming_games data frames
load_platform_data <- function() {
  log_message(sprintf("Loading data for platform: %s", config$platform))
  
  if (config$platform == "posit_connect_cloud") {
    if (!establish_bigquery_connection()) {
      log_message("Failed to establish BigQuery connection", "ERROR")
      return(list(completed_games = NULL, upcoming_games = NULL))
    }
    
    completed_games <- execute_bigquery_query(config$completed_games_table)
    upcoming_games <- execute_bigquery_query(config$upcoming_games_table)
    
  } else if (config$platform == "local") {
    tryCatch({
      completed_games <- readRDS(file.path(config$local_data_path, "rds_files/completed_games.rds"))
      upcoming_games <- readRDS(file.path(config$local_data_path, "rds_files/upcoming_games.rds"))
      
      log_message(sprintf("Loaded local data: %d completed games, %d upcoming games", 
                          nrow(completed_games), nrow(upcoming_games)))
      
    }, error = function(e) {
      log_message(sprintf("Failed to load local data: %s", e$message), "ERROR")
      return(list(completed_games = NULL, upcoming_games = NULL))
    })
    
  } else {
    log_message("Platform environment variable not set correctly", "ERROR")
    return(list(completed_games = NULL, upcoming_games = NULL))
  }
  
  return(list(completed_games = completed_games, upcoming_games = upcoming_games))
}

# LOGO AND COLOR FUNCTIONS ------------------------------------------------

#' Extract team colors from NHL logos object
#' @param nhl_logos Preloaded NHL logos object
#' @return Data frame with team colors
extract_team_colors <- function(nhl_logos) {
  tryCatch({
    colors_list <- list()
    
    for (team_key in names(nhl_logos$teams)) {
      team_data <- nhl_logos$teams[[team_key]]
      
      if (!is.null(team_data$colors$primary_color)) {
        team_name <- gsub("_", " ", team_key)
        color_hex <- gsub("^#", "", team_data$colors$primary_color)
        
        colors_list[[length(colors_list) + 1]] <- data.frame(
          team = team_name,
          color_hex = color_hex,
          priority = 1,
          stringsAsFactors = FALSE
        )
      }
    }
    
    colors_df <- do.call(rbind, colors_list)
    log_message(sprintf("Extracted colors for %d teams", nrow(colors_df)))
    return(colors_df)
    
  }, error = function(e) {
    log_message(sprintf("Failed to extract team colors: %s", e$message), "ERROR")
    return(data.frame(team = character(), color_hex = character(), priority = integer()))
  })
}

#' Convert logo from RDS to base64 HTML img tag
#' @param team_name Name of the team
#' @param nhl_logos Preloaded NHL logos object
#' @param logo_type Type of logo to use
#' @param height Height of the logo in pixels
#' @return HTML img tag or team name if logo not found
create_logo_html_tag <- function(team_name, nhl_logos, logo_type = "default", height = 30) {
  tryCatch({
    team_key <- gsub(" ", "_", toupper(team_name))
    
    if (!team_key %in% names(nhl_logos$teams)) {
      return(team_name)
    }
    
    logo_data <- nhl_logos$teams[[team_key]]$logos[[logo_type]]
    
    if (is.null(logo_data) || !logo_data$success) {
      return(team_name)
    }
    
    temp_file <- tempfile(fileext = ".png")
    writePNG(logo_data$image, temp_file)
    base64_string <- base64encode(temp_file)
    unlink(temp_file)
    
    img_tag <- sprintf(
      '<img src="data:image/png;base64,%s" height="%d" alt="%s" title="%s" style="vertical-align: middle;">',
      base64_string, height, team_name, team_name
    )
    
    return(img_tag)
    
  }, error = function(e) {
    log_message(sprintf("Failed to create logo for %s: %s", team_name, e$message), "WARN")
    return(team_name)
  })
}

# VISUALIZATION FUNCTIONS -------------------------------------------------

#' Create base EV plot with ribbons
#' @param xlims X-axis limits
#' @return ggplot object
create_base_ev_plot <- function(xlims) {
  # Simple approach with ribbons
  x_vals <- seq(0.01, 1, by = 0.01)
  
  # Calculate lines for different EV values
  ev_data <- data.frame(
    x = x_vals,
    ev_neg5 = calculate_line_from_ev(EV = -5, winProb = x_vals),
    ev_0 = calculate_line_from_ev(EV = 0, winProb = x_vals),
    ev_005 = calculate_line_from_ev(EV = 0.05, winProb = x_vals),
    ev_01 = calculate_line_from_ev(EV = 0.10, winProb = x_vals)
  )
  
  # Cap extreme values
  ev_data <- ev_data %>%
    mutate(across(starts_with("ev"), ~pmin(.x, 10, na.rm = TRUE)))
  
  p <- ggplot(ev_data, aes(x = x)) +
    # Red area below EV=0
    geom_ribbon(aes(ymin = 0, ymax = ev_0), fill = "red", alpha = 0.3) +
    # Orange ribbon EV=0 to 0.05
    geom_ribbon(aes(ymin = ev_0, ymax = ev_005), fill = "orange", alpha = 0.2) +
    # Yellow ribbon EV=0.05 to 0.10
    geom_ribbon(aes(ymin = ev_005, ymax = ev_01), fill = "yellow", alpha = 0.2) +
    # Green area above EV=0.10
    geom_ribbon(aes(ymin = ev_01, ymax = 10), fill = "green", alpha = 0.2) +
    # Main EV=0 line
    geom_line(aes(y = ev_0), color = "black", linewidth = 1) +
    xlim(0, 1) +
    ylim(0, 10)
  
  return(p)
}

#' Process team logos for plotting
#' @param df Data frame
#' @param team_col Team column name
#' @param nhl_logos NHL logos object
#' @param logo_type Logo type
#' @param temp_logo_dir Temporary directory
#' @return Data frame with logo paths
process_team_logos <- function(df, team_col, nhl_logos, logo_type, temp_logo_dir) {
  if (is.null(temp_logo_dir)) {
    temp_logo_dir <- file.path(tempdir(), "nhl_logos_temp", format(Sys.time(), "%Y%m%d_%H%M%S"))
  }
  
  if (!dir.exists(temp_logo_dir)) {
    dir.create(temp_logo_dir, recursive = TRUE)
  }
  
  df$team_key <- gsub(" ", "_", toupper(df[[team_col]]))
  df$logo_path <- NA
  
  unique_teams <- unique(df$team_key)
  for (team in unique_teams) {
    if (team %in% names(nhl_logos$teams)) {
      logo_data <- nhl_logos$teams[[team]]$logos[[logo_type]]
      
      if (!is.null(logo_data) && logo_data$success) {
        temp_file <- file.path(temp_logo_dir, paste0(team, "_", logo_type, ".png"))
        writePNG(logo_data$image, temp_file)
        df$logo_path[df$team_key == team] <- temp_file
      }
    }
  }
  
  return(df)
}

#' Add logo points to plot
#' @param p ggplot object
#' @param df Data frame
#' @param win_prob_col Win probability column
#' @param line_col Line column
#' @param team_col Team column
#' @param logo_size Logo size
#' @return Updated ggplot object
add_logo_points <- function(p, df, win_prob_col, line_col, team_col, logo_size) {
  df_with_logos <- df[!is.na(df$logo_path) & file.exists(df$logo_path), ]
  df_without_logos <- df[is.na(df$logo_path) | !file.exists(df$logo_path), ]
  
  if (nrow(df_with_logos) > 0) {
    p <- p + geom_image(data = df_with_logos,
                        mapping = aes(x = .data[[win_prob_col]],
                                      y = .data[[line_col]],
                                      image = logo_path),
                        size = logo_size)
  }
  
  if (nrow(df_without_logos) > 0) {
    p <- p + 
      geom_point(data = df_without_logos,
                 mapping = aes(x = .data[[win_prob_col]],
                               y = .data[[line_col]]),
                 size = 3, color = "black") +
      geom_text(data = df_without_logos,
                mapping = aes(x = .data[[win_prob_col]],
                              y = .data[[line_col]],
                              label = .data[[team_col]]),
                vjust = -1,
                size = 2.5)
  }
  
  return(p)
}

#' Add regular points to plot
#' @param p ggplot object
#' @param df Data frame
#' @param win_prob_col Win probability column
#' @param line_col Line column
#' @param winner_col Winner column
#' @param team_col Team column
#' @param interactive Whether plot is interactive
#' @return Updated ggplot object
add_regular_points <- function(p, df, win_prob_col, line_col, winner_col, team_col, interactive) {
  # Ensure winner column is factor
  df[[winner_col]] <- as.factor(df[[winner_col]])
  
  if (interactive) {
    # For interactive plots, include text for hover
    p + geom_point(data = df,
                   mapping = aes(x = .data[[win_prob_col]],
                                 y = .data[[line_col]],
                                 color = .data[[winner_col]],
                                 shape = .data[[winner_col]],
                                 text = paste("Team:", .data[[team_col]],
                                              "<br>Win Prob:", round(.data[[win_prob_col]], 3),
                                              "<br>Line:", .data[[line_col]],
                                              "<br>Winner:", .data[[winner_col]]))) +
      scale_shape_manual(name = "Winner", values = c("TRUE" = 16, "FALSE" = 4), drop = FALSE) +
      scale_color_manual(name = "Winner", values = c("TRUE" = config$custom_green, "FALSE" = config$custom_red), drop = FALSE)
  } else {
    # For static plots, don't include text aesthetic
    p + geom_point(data = df,
                   mapping = aes(x = .data[[win_prob_col]],
                                 y = .data[[line_col]],
                                 color = .data[[winner_col]],
                                 shape = .data[[winner_col]])) +
      scale_shape_manual(name = "Winner", values = c("TRUE" = 16, "FALSE" = 4), drop = FALSE) +
      scale_color_manual(name = "Winner", values = c("TRUE" = config$custom_green, "FALSE" = config$custom_red), drop = FALSE)
  }
}

#' Style the EV plot
#' @param p ggplot object
#' @param title Plot title
#' @param xlims X-axis limits
#' @param ylims Y-axis limits
#' @param line_values Line values for y-axis breaks
#' @return Styled ggplot object
style_ev_plot <- function(p, title, xlims, ylims, line_values) {
  p + 
    labs(x = 'Win Probability', y = 'Line', title = title) +
    coord_cartesian(xlim = xlims, ylim = ylims) +
    scale_x_continuous(breaks = seq(0, 1, by = 0.10)) +
    scale_y_continuous(breaks = seq(floor(min(line_values, na.rm = TRUE)), 
                                    ceiling(max(line_values, na.rm = TRUE)), 
                                    by = 0.5)) +
    theme_minimal() +
    theme(plot.title = element_text(hjust = 0.5),
          legend.position = "bottom")
}

#' Create expected value scatter plot
#' @param df Data frame with game data
#' @param line_col Column name for betting line
#' @param win_prob_col Column name for win probability
#' @param winner_col Column name for winner indicator
#' @param team_col Column name for team names
#' @param title Plot title
#' @param use_logos Whether to use team logos
#' @param nhl_logos Preloaded NHL logos object
#' @param logo_type Type of logo to use
#' @param logo_size Size of logos
#' @param interactive Whether to create interactive plot
#' @param temp_logo_dir Temporary directory for logo files
#' @return ggplot or plotly object
create_ev_scatter_plot <- function(df, 
                                   line_col = "price",
                                   win_prob_col = "win_percent", 
                                   winner_col = "winner",
                                   team_col = "team",
                                   title = 'Odds vs Win Probability',
                                   use_logos = FALSE,
                                   nhl_logos = NULL,
                                   logo_type = "default",
                                   logo_size = 0.1,
                                   interactive = TRUE,
                                   temp_logo_dir = NULL) {
  
  tryCatch({
    # Input validation
    if (nrow(df) == 0) {
      log_message("Empty data frame provided to create_ev_scatter_plot", "WARN")
      return(NULL)
    }
    
    # Filter out invalid data
    df <- df %>%
      filter(!is.na(.data[[line_col]]) & !is.na(.data[[win_prob_col]]))
    
    if (nrow(df) == 0) {
      log_message("No valid data after filtering NAs", "WARN")
      return(NULL)
    }
    
    # Set axis limits with buffer
    ylims <- c(min(df[[line_col]] - 0.25, na.rm = TRUE),
               max(df[[line_col]] + 0.25, na.rm = TRUE))
    xlims <- c(max(0, min(df[[win_prob_col]] - 0.05, na.rm = TRUE)),
               min(1, max(df[[win_prob_col]] + 0.05, na.rm = TRUE)))
    
    # Process logos if requested
    if (use_logos && !interactive && !is.null(nhl_logos)) {
      df <- process_team_logos(df, team_col, nhl_logos, logo_type, temp_logo_dir)
    }
    
    # Create base plot
    p <- create_base_ev_plot(xlims)
    
    # Add data points
    if (use_logos && !interactive && "logo_path" %in% names(df)) {
      p <- add_logo_points(p, df, win_prob_col, line_col, team_col, logo_size)
    } else {
      p <- add_regular_points(p, df, win_prob_col, line_col, winner_col, team_col, interactive)
    }
    
    # Style the plot
    p <- style_ev_plot(p, title, xlims, ylims, df[[line_col]])
    
    # Remove legend for logo plots
    if (use_logos && !interactive) {
      p <- p + theme(legend.position = "none")
    }
    
    # Convert to interactive if requested
    if (interactive && !use_logos) {
      p <- ggplotly(p, tooltip = "text") %>%
        layout(legend = list(x = 0.65, y = 0.9, bgcolor = 'rgba(0,0,0,0)'))
    }
    
    return(p)
    
  }, error = function(e) {
    log_message(sprintf("Failed to create EV scatter plot: %s", e$message), "ERROR")
    return(NULL)
  })
}

# DATA PROCESSING FUNCTIONS -----------------------------------------------

#' Process upcoming games data
#' @param upcoming_games_db Raw upcoming games data
#' @param team_colors Team colors data frame
#' @return Processed upcoming games data
process_upcoming_games <- function(upcoming_games_db, team_colors) {
  tryCatch({
    if (is.null(upcoming_games_db) || nrow(upcoming_games_db) == 0) {
      log_message("No upcoming games data to process", "WARN")
      return(data.frame())
    }
    
    processed <- merge.data.frame(
      x = upcoming_games_db, 
      y = slice_head(team_colors, n = 1, by = "team"),
      by = "team", 
      all.x = TRUE
    ) %>%
      arrange(game_id, retrieved_time, home_or_away)
    
    log_message(sprintf("Processed %d upcoming games", nrow(processed)))
    return(processed)
    
  }, error = function(e) {
    log_message(sprintf("Failed to process upcoming games: %s", e$message), "ERROR")
    return(data.frame())
  })
}

# DATA LOADING ------------------------------------------------------------

log_message("Starting NHL Betting Dashboard initialization")

# Load data
game_data <- load_platform_data()
completed_games_db <- game_data$completed_games
upcoming_games_db <- game_data$upcoming_games

# Load logos
nhl_logos <- tryCatch({
  readRDS(file.path(config$local_data_path, "team_metadata/nhl_logos_preloaded.rds"))
}, error = function(e) {
  log_message(sprintf("Failed to load NHL logos: %s", e$message), "ERROR")
  NULL
})

# Extract team colors
team_colors <- if (!is.null(nhl_logos)) {
  extract_team_colors(nhl_logos)
} else {
  data.frame(team = character(), color_hex = character(), priority = integer())
}

# Process upcoming games
if (!is.null(upcoming_games_db)) {
  upcoming_games_db <- process_upcoming_games(upcoming_games_db, team_colors)
}

# UI DEFINITION -----------------------------------------------------------

ui <- page_navbar(
  title = config$app_title,
  theme = bs_theme(preset = config$theme_preset, version = 5),
  
  nav_panel(
    title = "Betting Record",
    icon = bs_icon("graph-up-arrow"),
    navset_card_pill(
      title = "Plots",
      full_screen = TRUE,
      nav_panel(
        "Profit/Loss",
        plotlyOutput("profit_loss_plot")
      ),
      nav_panel(
        "E.V. Scatterplot",
        plotlyOutput("ev_plot")
      )
    )
  ),
  
  nav_panel(
    title = "Model Performance",
    icon = bs_icon("speedometer"),
    navset_card_pill(
      title = "Plots",
      full_screen = TRUE,
      nav_panel(
        "Win Percentage",
        plotlyOutput("win_percentage_plot")
      ),
      nav_panel(
        "Implied Win Percentage",
        plotlyOutput("implied_win_percentage_plot")
      )
    )
  ),
  
  nav_panel(
    title = "Upcoming Games",
    icon = bs_icon("calendar-event"), 
    navset_card_pill(
      full_screen = TRUE,
      nav_panel(
        "Line Movement",
        layout_column_wrap(
          width = NULL,
          style = htmltools::css(grid_template_columns = "11fr 1fr"),
          plotlyOutput("line_movement_plot"),
          DTOutput("narrow_dt")
        )
      ),
      nav_panel(
        "Table",
        DTOutput("upcoming_games_table")
      ),
      nav_panel(
        "E.V. Scatterplot",
        plotOutput("upcoming_ev_plot")
      )
    )
  ),
  
  nav_spacer(),
  
  nav_item(
    actionButton("quit_btn", "Quit",
                 icon = icon("power-off"),
                 class = "btn-danger")
  )
)

# SERVER LOGIC ------------------------------------------------------------

server <- function(input, output, session) {
  
  # Reactive: Process upcoming games
  upcoming_df <- reactive({
    tryCatch({
      if (is.null(upcoming_games_db) || nrow(upcoming_games_db) == 0) {
        return(data.frame())
      }
      
      upcoming_games_db %>%
        filter(game_state_odds %ni% c("LIVE", "CRIT", "FINAL")) %>%
        filter(is.na(game_state_results)) %>%
        mutate(
          retrieved_time = as.POSIXct(retrieved_time),
          game_time_clean = gsub("\\s*ET.*", "", game_time),
          game_time_clean = format(
            as.POSIXct(game_time_clean, format = "%I:%M %p"),
            format = "%H:%M"
          ),
          game_date_time = as.POSIXct(
            paste0(game_date, " ", game_time_clean),
            format = "%Y-%m-%d %H:%M"
          ),
          time_to_gametime = as.numeric(difftime(game_date_time, retrieved_time, units = "hours"))
        )
    }, error = function(e) {
      log_message(sprintf("Error processing upcoming games: %s", e$message), "ERROR")
      return(data.frame())
    })
  })
  
  # Reactive: Value bets from completed games
  value_df <- reactive({
    tryCatch({
      if (is.null(completed_games_db) || nrow(completed_games_db) == 0) {
        return(data.frame())
      }
      
      completed_games_db %>%
        filter(game_state_odds %in% c("FUT", "PRE")) %>% 
        slice_max(kelly_criterion, n = 1, with_ties = FALSE, by = c(game_id, team))
    }, error = function(e) {
      log_message(sprintf("Error processing value bets: %s", e$message), "ERROR")
      return(data.frame())
    })
  })
  
  # Reactive: Profit by date
  profit_by_date <- reactive({
    tryCatch({
      val_df <- value_df()
      if (nrow(val_df) == 0) {
        return(data.frame())
      }
      
      val_df %>%
        filter(kelly_criterion > 0) %>%
        group_by(game_date) %>%
        summarise(
          N_BETS = n(),
          N_WINNERS = sum(winner),
          TOTAL_WAGERED = sum(kelly_criterion * 100),
          TOTAL_RETURN = sum(ifelse(winner, kelly_criterion * price * 100, 0)),
          PROFIT = TOTAL_RETURN - TOTAL_WAGERED,
          .groups = "drop"
        )
    }, error = function(e) {
      log_message(sprintf("Error calculating profit by date: %s", e$message), "ERROR")
      return(data.frame())
    })
  })
  
  # Output: Profit/Loss plot
  output$profit_loss_plot <- renderPlotly({
    tryCatch({
      profit_data <- profit_by_date()
      if (nrow(profit_data) == 0) {
        return(plot_ly() %>% layout(title = "No data available"))
      }
      
      profit_data <- profit_data %>%
        mutate(color = ifelse(PROFIT > 0, config$custom_green, config$custom_red))
      
      plot_ly(data = profit_data) %>%
        add_lines(x = ~game_date, y = ~cumsum(PROFIT), name = 'Profit/Loss', 
                  line = list(shape = 'spline', smoothing = 0.5)) %>%
        add_lines(x = ~game_date, y = ~cumsum(TOTAL_WAGERED * 0.0476 * -1), name = 'Juice Tax',
                  line = list(shape = 'spline', smoothing = 0.5, dash = "dash")) %>%
        add_bars(x = ~game_date, y = ~PROFIT,
                 marker = list(color = ~color),
                 opacity = 0.6,
                 name = "Daily P/L",
                 hoverinfo = "text",
                 hovertext = ~paste0('<b>Profit:</b> $', round(PROFIT, 2), "<br>",
                                     '<b>N Bets:</b> ', N_BETS, "<br>",
                                     '<b>N Winners:</b> ', N_WINNERS, "<br>",
                                     '<b>Wagered:</b> $', round(TOTAL_WAGERED, 2), "<br>",
                                     '<b>Returned:</b> $', round(TOTAL_RETURN, 2))) %>%
        layout(
          title = list(text = 'Cumulative Profit/Loss Over Time', y = 0.97),
          xaxis = list(title = 'Date'),
          yaxis = list(title = 'Dollars ($)'),
          showlegend = FALSE
        )
    }, error = function(e) {
      log_message(sprintf("Error rendering profit/loss plot: %s", e$message), "ERROR")
      plot_ly() %>% layout(title = "Error loading plot")
    })
  })
  
  # Output: EV plot
  output$ev_plot <- renderPlotly({
    tryCatch({
      val_df <- value_df()
      if (is.null(val_df) || nrow(val_df) == 0) {
        return(plot_ly() %>% layout(title = "No data available"))
      }
      
      # Ensure numeric columns
      val_df$price <- as.numeric(val_df$price)
      val_df$win_percent <- as.numeric(val_df$win_percent)
      
      # Filter out any remaining invalid data
      val_df <- val_df %>%
        filter(!is.na(price) & !is.na(win_percent) & 
                 is.finite(price) & is.finite(win_percent))
      
      if (nrow(val_df) == 0) {
        return(plot_ly() %>% layout(title = "No valid data available"))
      }
      
      create_ev_scatter_plot(
        df = val_df, 
        line_col = "price", 
        win_prob_col = "win_percent", 
        winner_col = "winner"
      )
    }, error = function(e) {
      log_message(sprintf("Error rendering EV plot: %s", e$message), "ERROR")
      plot_ly() %>% layout(title = "Error loading plot")
    })
  })
  
  # Reactive: Upcoming value games
  upcoming_value_df <- reactive({
    tryCatch({
      up_df <- upcoming_df()
      if (nrow(up_df) == 0) {
        return(data.frame())
      }
      
      up_df %>%
        slice_max(retrieved_time, n = 1, with_ties = FALSE, by = c("game_id", "team"))
    }, error = function(e) {
      log_message(sprintf("Error processing upcoming value games: %s", e$message), "ERROR")
      return(data.frame())
    })
  })
  
  # Output: Upcoming EV plot
  output$upcoming_ev_plot <- renderPlot({
    tryCatch({
      up_val_df <- upcoming_value_df()
      if (is.null(up_val_df) || nrow(up_val_df) == 0) {
        return(NULL)
      }
      
      # Ensure numeric columns
      up_val_df$price <- as.numeric(up_val_df$price)
      up_val_df$win_percent <- as.numeric(up_val_df$win_percent)
      
      # Filter out any invalid data
      up_val_df <- up_val_df %>%
        filter(!is.na(price) & !is.na(win_percent) & 
                 is.finite(price) & is.finite(win_percent))
      
      if (nrow(up_val_df) == 0) {
        return(NULL)
      }
      
      # For upcoming games, winner is NA, so create a dummy column
      up_val_df$winner_display <- "Upcoming"
      
      create_ev_scatter_plot(
        df = up_val_df,
        line_col = "price",
        win_prob_col = "win_percent",
        winner_col = "winner_display",  # Use the dummy column
        team_col = "team",
        use_logos = TRUE,
        nhl_logos = nhl_logos,
        logo_type = "default",
        logo_size = 0.1,
        interactive = FALSE
      )
    }, error = function(e) {
      log_message(sprintf("Error rendering upcoming EV plot: %s", e$message), "ERROR")
      NULL
    })
  })
  
  # Reactive: Win probability bins
  win_prob_bin_df <- reactive({
    tryCatch({
      val_df <- value_df()
      if (nrow(val_df) == 0) {
        return(data.frame())
      }
      
      val_df %>%
        mutate(
          WIN_PROB_BIN = cut(win_percent, breaks = seq(0, 1, by = 0.10), 
                             include.lowest = TRUE, right = FALSE),
          Price_BIN = cut(rank(price), breaks = 10, labels = FALSE)
        )
    }, error = function(e) {
      log_message(sprintf("Error creating win probability bins: %s", e$message), "ERROR")
      return(data.frame())
    })
  })
  
  # Reactive: Win probability vs observed
  win_prob_vs_observed <- reactive({
    tryCatch({
      bin_df <- win_prob_bin_df()
      if (nrow(bin_df) == 0) {
        return(data.frame())
      }
      
      bin_df %>%
        group_by(WIN_PROB_BIN) %>%
        summarise(
          AVERAGE_WIN_PROB = mean(win_percent),
          WINNERS = sum(winner),
          GAMES = n(),
          OBSERVED_WIN_PERCENTAGE = WINNERS / GAMES,
          .groups = "drop"
        )
    }, error = function(e) {
      log_message(sprintf("Error calculating win prob vs observed: %s", e$message), "ERROR")
      return(data.frame())
    })
  })
  
  # Reactive: Line vs observed
  line_vs_observed <- reactive({
    tryCatch({
      bin_df <- win_prob_bin_df()
      if (nrow(bin_df) == 0) {
        return(data.frame())
      }
      
      bin_df %>%
        group_by(Price_BIN) %>%
        summarise(
          AVERAGE_LINE = mean(price),
          EXP_WIN_PROB = calculate_prob_from_line(EV = 0, line = AVERAGE_LINE),
          WINNERS = sum(winner),
          GAMES = n(),
          OBSERVED_WIN_PERCENTAGE = WINNERS / GAMES,
          MIN_LINE = min(price),
          MAX_LINE = max(price),
          .groups = "drop"
        )
    }, error = function(e) {
      log_message(sprintf("Error calculating line vs observed: %s", e$message), "ERROR")
      return(data.frame())
    })
  })
  
  # Output: Win percentage plot
  output$win_percentage_plot <- renderPlotly({
    tryCatch({
      win_data <- win_prob_vs_observed()
      if (nrow(win_data) == 0) {
        return(plot_ly() %>% layout(title = "No data available"))
      }
      
      plot_ly(data = win_data, x = ~WIN_PROB_BIN) %>%
        add_trace(y = ~AVERAGE_WIN_PROB * 100, name = 'Expected', type = "bar",
                  text = ~round(AVERAGE_WIN_PROB * 100, 1), textposition = 'outside',
                  opacity = 0.75, marker = list(color = config$custom_red)) %>%
        add_trace(y = ~OBSERVED_WIN_PERCENTAGE * 100, name = 'Observed', type = "bar",
                  text = ~round(OBSERVED_WIN_PERCENTAGE * 100, 1), textposition = 'outside',
                  opacity = 0.75, marker = list(color = config$custom_green)) %>%
        layout(
          barmode = 'overlay',
          xaxis = list(title = 'Win Probability'),
          yaxis = list(title = 'Win Percentage', range = c(0, 100)),
          legend = list(
            title = list(text = 'Win Percentage'),
            x = 0.1, y = 0.95, bgcolor = 'rgba(0,0,0,0)'
          )
        )
    }, error = function(e) {
      log_message(sprintf("Error rendering win percentage plot: %s", e$message), "ERROR")
      plot_ly() %>% layout(title = "Error loading plot")
    })
  })
  
  # Output: Implied win percentage plot
  output$implied_win_percentage_plot <- renderPlotly({
    tryCatch({
      line_data <- line_vs_observed()
      if (nrow(line_data) == 0) {
        return(plot_ly() %>% layout(title = "No data available"))
      }
      
      plot_ly(data = line_data, y = ~as.factor(round(AVERAGE_LINE, 2))) %>%
        add_trace(x = ~EXP_WIN_PROB * 100, name = 'Expected',
                  type = 'bar', orientation = 'h',
                  hovertext = ~paste0('<b>Exp. Win %:</b> ', round(EXP_WIN_PROB * 100, 1)),
                  textposition = 'outside',
                  opacity = 0.75, marker = list(color = config$custom_red)) %>%
        add_trace(x = ~OBSERVED_WIN_PERCENTAGE * 100, name = 'Observed',
                  type = 'bar', orientation = 'h',
                  hovertext = ~paste0('<b>Obs. Win %:</b> ', round(OBSERVED_WIN_PERCENTAGE * 100, 1)),
                  textposition = 'outside',
                  opacity = 0.75, marker = list(color = config$custom_green)) %>%
        layout(
          barmode = 'overlay',
          yaxis = list(title = 'Line'),
          xaxis = list(title = 'Win Percentage', range = c(0, 100)),
          legend = list(
            title = list(text = 'Win Percentage'),
            x = 0.6, y = 0.95, bgcolor = 'rgba(0,0,0,0)'
          )
        )
    }, error = function(e) {
      log_message(sprintf("Error rendering implied win percentage plot: %s", e$message), "ERROR")
      plot_ly() %>% layout(title = "Error loading plot")
    })
  })
  
  # Output: Game selection table
  output$narrow_dt <- renderDT({
    tryCatch({
      up_df <- upcoming_df()
      if (nrow(up_df) == 0) {
        return(datatable(data.frame(Game = character())))
      }
      
      datatable(
        data.frame(Game = unique(up_df$game)),
        rownames = FALSE,
        selection = "single",
        options = list(
          pageLength = 30,
          dom = "t",
          columnDefs = list(
            list(className = 'dt-center', targets = "_all")
          )
        )
      )
    }, error = function(e) {
      log_message(sprintf("Error rendering game selection table: %s", e$message), "ERROR")
      datatable(data.frame(Game = character()))
    })
  })
  
  # Output: Upcoming games table
  output$upcoming_games_table <- renderDT({
    tryCatch({
      up_df <- upcoming_df()
      if (nrow(up_df) == 0) {
        return(datatable(data.frame(Message = "No upcoming games")))
      }
      
      games_data <- up_df %>%
        slice_max(retrieved_time, n = 1, by = team) %>%
        arrange(game_date, game_time, home_or_away)
      
      games_compact <- games_data %>%
        group_by(game, game_date, game_time) %>%
        summarise(
          display = paste0(
            '<div style="display: flex; align-items: center; justify-content: center;">',
            '<div style="text-align: center; width: 155px;">',
            create_logo_html_tag(first(team), nhl_logos, "default", 40),
            '<br><strong>', first(team), '</strong><br>',
            'Line: ', round(first(price), 2), '<br>',
            'KC: ', round(first(kelly_criterion) * 100, 2), '%<br>',
            'Goalie: ', first(goalie),
            '</div>',
            '<div style="text-align: center; width: 20px;"><strong>VS</strong></div>',
            '<div style="text-align: center; width: 155px;">',
            create_logo_html_tag(last(team), nhl_logos, "default", 40),
            '<br><strong>', last(team), '</strong><br>',
            'Line: ', round(last(price), 2), '<br>',
            'KC: ', round(last(kelly_criterion) * 100, 2), '%<br>',
            'Goalie: ', last(goalie),
            '</div></div>'
          ),
          .groups = "drop"
        )
      
      datatable(
        games_compact[, c("game_date", "display")],
        rownames = FALSE,
        escape = FALSE,
        extensions = "RowGroup",
        options = list(
          pageLength = 30,
          dom = "t",
          rowGroup = list(dataSrc = 0),
          columnDefs = list(
            list(visible = FALSE, targets = 0),
            list(className = 'dt-center', targets = "_all")
          )
        ),
        colnames = c("Date" = "game_date", "Matchup" = "display")
      )
    }, error = function(e) {
      log_message(sprintf("Error rendering upcoming games table: %s", e$message), "ERROR")
      datatable(data.frame(Message = "Error loading games"))
    })
  })
  
  # Reactive: Selected game
  selected_game <- reactive({
    req(input$narrow_dt_row_last_clicked)
    up_df <- upcoming_df()
    if (nrow(up_df) == 0) return(NULL)
    
    unique_games <- unique(up_df$game)
    if (input$narrow_dt_row_last_clicked <= length(unique_games)) {
      return(unique_games[input$narrow_dt_row_last_clicked])
    }
    return(NULL)
  })
  
  # Reactive: Game time data
  game_time_df <- reactive({
    sel_game <- selected_game()
    if (is.null(sel_game)) return(data.frame())
    
    upcoming_df() %>%
      filter(game == sel_game)
  })
  
  # Output: Line movement plot
  output$line_movement_plot <- renderPlotly({
    tryCatch({
      plot_data <- game_time_df()
      if (nrow(plot_data) == 0) {
        return(plot_ly() %>% layout(title = "Select a game to view line movement"))
      }
      
      # Create color mapping
      color_mapping <- plot_data %>%
        select(team, color_hex) %>%
        distinct() %>%
        mutate(
          color_hex = ifelse(is.na(color_hex), gsub("#", "", config$default_gray), color_hex),
          color_hex = paste0("#", gsub("^#", "", color_hex))
        ) %>%
        deframe()
      
      # Create the plot
      p <- plot_ly()
      
      # Get unique teams for iteration
      teams <- unique(plot_data$team)
      
      # Add traces for each team
      for (team in teams) {
        team_data <- plot_data %>% filter(team == !!team)
        team_color <- color_mapping[team]
        
        # Kelly Criterion trace
        p <- p %>%
          add_trace(
            data = team_data,
            x = ~time_to_gametime, y = ~kelly_criterion,
            type = "scatter", mode = "lines+markers",
            color = I(team_color),
            marker = list(symbol = 'circle', size = 10),
            line = list(shape = 'hv', width = 3),
            hoverinfo = "text",
            hovertext = ~paste0('<b>Team:</b> ', team, "<br>",
                                '<b>KC:</b> ', round(kelly_criterion, 2), "<br>",
                                '<b>Line:</b> ', round(price, 2), "<br>",
                                '<b>Win Prob:</b> ', round(win_percent, 2), "<br>",
                                '<b>Hours to PD:</b> ', round(-1 * time_to_gametime, 0)),
            name = paste(team, "KC"),
            visible = TRUE
          )
        
        # Expected Value trace
        p <- p %>%
          add_trace(
            data = team_data,
            x = ~time_to_gametime, y = ~expected_value,
            type = "scatter", mode = "lines+markers",
            color = I(team_color),
            marker = list(symbol = 'x', size = 10),
            line = list(shape = 'hv', width = 3, dash = "dash"),
            hoverinfo = "text",
            hovertext = ~paste0('<b>Team:</b> ', team, "<br>",
                                '<b>EV:</b> ', round(expected_value, 2), "<br>",
                                '<b>Line:</b> ', round(price, 2), "<br>",
                                '<b>Win Prob:</b> ', round(win_percent, 2), "<br>",
                                '<b>Hours to PD:</b> ', round(-1 * time_to_gametime, 0)),
            name = paste(team, "EV"),
            yaxis = "y3",
            visible = TRUE,
            showlegend = FALSE
          )
        
        # Price trace
        p <- p %>%
          add_trace(
            data = team_data,
            x = ~time_to_gametime, y = ~price,
            type = "scatter", mode = "lines+markers",
            color = I(team_color),
            marker = list(symbol = "diamond", size = 10),
            line = list(shape = 'hv', width = 3, dash = "dot"),
            hoverinfo = "text",
            hovertext = ~paste0('<b>Team:</b> ', team, "<br>",
                                '<b>Line:</b> ', round(price, 2), "<br>",
                                '<b>Hours to PD:</b> ', round(-1 * time_to_gametime, 0)),
            name = paste(team, "Line"),
            yaxis = "y2",
            visible = FALSE,
            showlegend = FALSE
          )
        
        # Win Probability trace
        p <- p %>%
          add_trace(
            data = team_data,
            x = ~time_to_gametime, y = ~win_percent,
            type = "scatter", mode = "lines+markers",
            color = I(team_color),
            marker = list(symbol = "circle-open", size = 10),
            line = list(shape = 'hv', width = 3, dash = "dashdot"),
            hoverinfo = "text",
            hovertext = ~paste0('<b>Team:</b> ', team, "<br>",
                                '<b>Win Prob:</b> ', round(win_percent, 2), "<br>",
                                '<b>Hours to PD:</b> ', round(-1 * time_to_gametime, 0)),
            name = paste(team, "Win Prob"),
            yaxis = "y3",
            visible = FALSE,
            showlegend = FALSE
          )
      }
      
      # Calculate number of traces per view
      n_teams <- length(teams)
      n_traces_per_team <- 4
      total_traces <- n_teams * n_traces_per_team
      
      # Create visibility vectors for buttons
      vis_all <- rep(TRUE, total_traces)
      vis_kc <- rep(FALSE, total_traces)
      vis_line <- rep(FALSE, total_traces)
      
      for (i in 0:(n_teams-1)) {
        vis_kc[i*n_traces_per_team + 1] <- TRUE  # KC
        vis_kc[i*n_traces_per_team + 2] <- TRUE  # EV
        vis_line[i*n_traces_per_team + 3] <- TRUE  # Line
        vis_line[i*n_traces_per_team + 4] <- TRUE  # Win%
      }
      
      # Layout with view controls
      p %>%
        layout(
          title = list(text = 'Line Movement Before Gametime', y = 0.97),
          xaxis = list(
            title = 'Hours Before Game',
            range = c(max(plot_data$time_to_gametime, 0) + 3, 0),
            zerolinecolor = "#CCCCCC",
            zerolinewidth = 3
          ),
          yaxis = list(
            title = 'Kelly Criterion',
            range = c(-0.3, 0.3),
            zerolinecolor = "#CCCCCC",
            zerolinewidth = 3
          ),
          yaxis2 = list(
            title = '',
            overlaying = "y",
            side = "left",
            showline = FALSE,
            showticklabels = FALSE,
            showgrid = FALSE
          ),
          yaxis3 = list(
            title = 'Expected Value',
            range = c(-0.3, 0.3),
            overlaying = "y",
            side = "right",
            showline = TRUE,
            showticklabels = TRUE,
            zerolinecolor = "#CCCCCC",
            zerolinewidth = 3,
            showgrid = TRUE
          ),
          legend = list(
            title = list(text = "Team"),
            x = 0.02, y = 0.02
          ),
          margin = list(r = 50),
          updatemenus = list(
            list(
              active = 1,
              type = "buttons",
              direction = "down",
              x = 0.1,
              y = 1,
              buttons = list(
                list(
                  label = "All",
                  method = "update",
                  args = list(
                    list(visible = vis_all),
                    list(
                      yaxis = list(
                        showline = TRUE,
                        showticklabels = TRUE,
                        title = 'Kelly Criterion',
                        range = c(-0.3, 0.3),
                        zerolinecolor = "#CCCCCC",
                        zerolinewidth = 3
                      ),
                      yaxis2 = list(
                        showline = TRUE,
                        showticklabels = TRUE,
                        title = 'Line',
                        overlaying = "y",
                        side = "left"
                      ),
                      yaxis3 = list(
                        showline = TRUE,
                        showticklabels = TRUE,
                        title = 'Win Probability',
                        range = c(0, 1),
                        overlaying = "y",
                        side = "right"
                      )
                    )
                  )
                ),
                list(
                  label = "Kelly Crit.",
                  method = "update",
                  args = list(
                    list(visible = vis_kc),
                    list(
                      yaxis = list(
                        title = 'Kelly Criterion',
                        range = c(-0.3, 0.3),
                        zerolinecolor = "#CCCCCC",
                        zerolinewidth = 3
                      ),
                      yaxis2 = list(
                        title = '',
                        overlaying = "y",
                        side = "left",
                        showline = FALSE,
                        showticklabels = FALSE,
                        showgrid = FALSE
                      ),
                      yaxis3 = list(
                        title = 'Expected Value',
                        range = c(-0.3, 0.3),
                        overlaying = "y",
                        side = "right",
                        showline = TRUE,
                        showticklabels = TRUE,
                        zerolinecolor = "#CCCCCC",
                        zerolinewidth = 3,
                        showgrid = TRUE
                      )
                    )
                  )
                ),
                list(
                  label = "Line & Win %",
                  method = "update",
                  args = list(
                    list(visible = vis_line),
                    list(
                      yaxis = list(
                        showline = FALSE,
                        showticklabels = FALSE,
                        title = '',
                        showgrid = FALSE
                      ),
                      yaxis2 = list(
                        showline = TRUE,
                        showticklabels = TRUE,
                        title = 'Line',
                        overlaying = "y",
                        side = "left"
                      ),
                      yaxis3 = list(
                        showline = TRUE,
                        showticklabels = TRUE,
                        title = 'Win Probability',
                        range = c(0, 1),
                        overlaying = "y",
                        side = "right"
                      )
                    )
                  )
                )
              )
            )
          )
        )
      
    }, error = function(e) {
      log_message(sprintf("Error rendering line movement plot: %s", e$message), "ERROR")
      plot_ly() %>% layout(title = "Error loading plot")
    })
  })
  
  # Observer: Quit button
  observeEvent(input$quit_btn, {
    log_message("User clicked quit button, stopping app")
    stopApp()
  })
  
  # Session end cleanup
  session$onSessionEnded(function() {
    log_message("Session ended")
  })
}

# RUN APPLICATION ---------------------------------------------------------

log_message("NHL Betting Dashboard initialization complete")
log_message(sprintf("Data loaded: %s completed games, %s upcoming games", 
                    ifelse(is.null(completed_games_db), "0", nrow(completed_games_db)),
                    ifelse(is.null(upcoming_games_db), "0", nrow(upcoming_games_db))))

shinyApp(ui = ui, server = server)

# END ---------------------------------------------------------------------