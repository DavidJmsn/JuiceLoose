# NHL BETTING ANALYSIS DASHBOARD ------------------------------------------
# Purpose: Optimized Shiny dashboard using data.table for performance
# Author: Professional implementation with efficient data handling
# Last Updated: 2025-12-19

# SETUP -------------------------------------------------------------------

suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(bsicons)
  library(data.table)
  library(plotly)
  library(DT)
  library(ggplot2)
  library(lubridate)
  library(png)
  library(base64enc)
  library(bigrquery)
  library(gargle)
  library(ggimage)
  library(here)
})

# CONFIGURATION -----------------------------------------------------------

# # Handle service account for cloud deployment
# service_account_path <- Sys.getenv("SERVICE_ACCOUNT")
# 
# # Check if SERVICE_ACCOUNT_JSON (base64 encoded) is provided for cloud deployment
# if (Sys.getenv("PLATFORM", "local") == "posit_connect_cloud") {
#   service_json_content <- Sys.getenv("SERVICE_ACCOUNT_JSON")
#   if (nzchar(service_json_content)) {
#     # Decode base64 and write to temp file
#     temp_json <- tempfile(fileext = ".json")
#     writeLines(rawToChar(base64enc::base64decode(service_json_content)), temp_json)
#     service_account_path <- temp_json
#   }
# }

app_config <- list(
  # Platform detection
  platform = Sys.getenv("PLATFORM", "local"),
  
  # BigQuery settings
  project_id = Sys.getenv("PROJECT_ID"),
  dataset = Sys.getenv("DATASET"),
  service_account = Sys.getenv("SERVICE_ACCOUNT"),
  
  # Data paths using here package for robust path handling
  rds_path = here("data", "NHL", "rds_files"),
  logo_path = here("data", "NHL", "team_metadata", "nhl_logos_preloaded.rds"),
  
  # UI settings
  app_title = "Willy Snipe?",
  theme = "lux",
  
  # Colors
  color_green = "#18bc9c",
  color_red = "#e74c3c",
  color_gray = "#999999"
)

# LOGGING FUNCTIONS -------------------------------------------------------

#' Log message with timestamp and level
#' @param msg Message to log
#' @param level Log level (INFO, WARN, ERROR, SUCCESS, DEBUG)
log_message <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_entry <- sprintf("[%s] %s: %s", timestamp, level, msg)
  
  # Console output with color coding
  if (interactive() || level %in% c("ERROR", "WARN")) {
    if (level == "ERROR") {
      message(log_entry)
    } else if (level == "SUCCESS") {
      cat("\033[32m", log_entry, "\033[0m\n", sep = "")
    } else if (level == "WARN") {
      cat("\033[33m", log_entry, "\033[0m\n", sep = "")
    } else {
      cat(log_entry, "\n")
    }
  }
}

# DATA LOADING FUNCTIONS --------------------------------------------------

#' Load data based on platform
#' @return List with completed and upcoming games
load_app_data <- function() {
  log_message("Loading NHL data for Shiny app", "INFO")
  
  if (app_config$platform == "posit_connect_cloud") {
    # Load from BigQuery
    return(load_bigquery_data())
  } else {
    # Load from local RDS files
    return(load_local_data())
  }
}

#' Load data from local RDS files
#' @return List with data.tables
load_local_data <- function() {
  tryCatch({
    completed <- readRDS(file.path(app_config$rds_path, "completed_games.rds"))
    upcoming <- readRDS(file.path(app_config$rds_path, "upcoming_games.rds"))
    
    # Ensure data.table
    setDT(completed)
    setDT(upcoming)
    
    log_message(sprintf("Loaded %d completed, %d upcoming games from RDS",
                        nrow(completed), nrow(upcoming)), "INFO")
    
    return(list(completed = completed, upcoming = upcoming))
    
  }, error = function(e) {
    log_message(sprintf("Failed to load RDS files: %s", e$message), "ERROR")
    return(list(completed = data.table(), upcoming = data.table()))
  })
}

#' Load data from BigQuery
#' @return List with data.tables
load_bigquery_data <- function() {
  tryCatch({
    log_message("Establishing BigQuery connection...", "INFO")
    
    # Authenticate
    bigrquery::bq_deauth()
    
    if (is.null(app_config$service_account) || app_config$service_account == "") {
      log_message("Service account not found", "ERROR")
      return(list(completed = data.table(), upcoming = data.table()))
    }
    
    token <- gargle::credentials_service_account(
      path = app_config$service_account,
      scopes = c("https://www.googleapis.com/auth/bigquery")
    )
    
    bigrquery::bq_auth(token = token)
    
    # Query tables
    completed_query <- sprintf("SELECT * FROM `%s.%s.%s`",
                               app_config$project_id,
                               app_config$dataset,
                               "completed_games")
    
    upcoming_query <- sprintf("SELECT * FROM `%s.%s.%s`",
                              app_config$project_id,
                              app_config$dataset,
                              "upcoming_games")
    
    completed <- setDT(bq_project_query(app_config$project_id, completed_query) %>%
                         bq_table_download())
    
    upcoming <- setDT(bq_project_query(app_config$project_id, upcoming_query) %>%
                        bq_table_download())
    
    log_message(sprintf("Loaded %d completed, %d upcoming games from BigQuery",
                        nrow(completed), nrow(upcoming)), "SUCCESS")
    
    return(list(completed = completed, upcoming = upcoming))
    
  }, error = function(e) {
    log_message(sprintf("Failed to load from BigQuery: %s", e$message), "ERROR")
    return(list(completed = data.table(), upcoming = data.table()))
  })
}

#' Load team logos
#' @return List with logo data or NULL
load_team_logos <- function() {
  if (!file.exists(app_config$logo_path)) {
    log_message("Logo file not found", "WARN")
    return(NULL)
  }
  
  tryCatch({
    readRDS(app_config$logo_path)
  }, error = function(e) {
    log_message(sprintf("Failed to load logos: %s", e$message), "WARN")
    NULL
  })
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
        color_hex <- team_data$colors$primary_color
        
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

# CALCULATION FUNCTIONS ---------------------------------------------------

#' Calculate line from EV and win probability
#' @param ev Expected value
#' @param prob Win probability
#' @return Decimal line
calc_line_from_ev <- function(ev, prob) {
  ifelse(prob > 0 & prob <= 1, (ev + 1) / prob, NA_real_)
}

#' Generate EV background layers
#'
#' Creates a set of `geom_ribbon` and `geom_line` layers giving the
#' red–green gradient background and the 0 EV reference line used in the
#' scatterplots.
#' @param x_range numeric vector of length 2 specifying the range of win
#'   probabilities to cover
#' @return list of ggplot layers
ev_background_layers <- function(x_range = c(0, 1)) {
  x_vals <- seq(x_range[1], x_range[2], length.out = 200)
  bg <- data.frame(
    x = x_vals,
    neg = calc_line_from_ev(-5, x_vals),
    ev0 = calc_line_from_ev(0, x_vals),
    ev05 = calc_line_from_ev(0.05, x_vals),
    ev10 = calc_line_from_ev(0.10, x_vals),
    high = calc_line_from_ev(100, x_vals)
  )

  list(
    geom_ribbon(data = bg, aes(x = x, ymin = 0, ymax = neg),
                fill = "red", alpha = 0.4, inherit.aes = FALSE),
    geom_ribbon(data = bg, aes(x = x, ymin = neg, ymax = ev0),
                fill = "red", alpha = 0.3, inherit.aes = FALSE),
    geom_ribbon(data = bg, aes(x = x, ymin = ev0, ymax = ev05),
                fill = "orange", alpha = 0.2, inherit.aes = FALSE),
    geom_ribbon(data = bg, aes(x = x, ymin = ev05, ymax = ev10),
                fill = "yellow", alpha = 0.2, inherit.aes = FALSE),
    geom_ribbon(data = bg, aes(x = x, ymin = ev10, ymax = high),
                fill = "green", alpha = 0.2, inherit.aes = FALSE),
    geom_line(data = bg, aes(x = x, y = ev0),
              color = "black", size = 0.6, inherit.aes = FALSE)
  )
}

# PLOTTING FUNCTIONS ------------------------------------------------------

#' Create EV scatter plot
#' @param dt Data.table with game data
#' @param interactive Whether to create interactive plot
#' @return ggplot or plotly object
create_ev_plot <- function(dt, interactive = TRUE) {
  if (nrow(dt) == 0) return(NULL)
  
  # Ensure numeric and proper column names
  dt[, `:=`(
    price = as.numeric(price),
    win_percent = as.numeric(win_probability)  # Map to expected column name
  )]
  
  # Filter valid data
  plot_dt <- dt[!is.na(price) & !is.na(win_percent) & 
                  price > 0 & win_percent > 0 & win_percent <= 1]
  
  if (nrow(plot_dt) == 0) return(NULL)
  
  # Determine range for background
  x_range <- c(min(dt$win_percent - 0.05), max(dt$win_percent + 0.05))

  # Create base plot with EV background
  p <- ggplot(plot_dt, aes(x = win_percent, y = price)) +
    ev_background_layers(x_range)+ 
    coord_cartesian(ylim = c(min(dt$price) - 0.2, max(dt$price) + 0.2)) +
    # Points
    geom_point(aes(color = factor(winner), shape = factor(winner)), size = 3) +
    scale_color_manual(values = c("TRUE" = app_config$color_green,
                                  "FALSE" = app_config$color_red),
                       na.value = app_config$color_gray) +
    scale_shape_manual(values = c("TRUE" = 16, "FALSE" = 4),
                       na.value = 1) +
    labs(x = "Win Probability", y = "Decimal Line", 
         title = "Expected Value Analysis") +
    theme_minimal() +
    theme(legend.position = "bottom")
  
  if (interactive) {
    ggplotly(p, tooltip = c("team", "price", "win_percent"))
  } else {
    p
  }
}

#' Create profit/loss plot
#' @param dt Data.table with completed games
#' @return plotly object
create_profit_plot <- function(dt) {
  if (nrow(dt) == 0) return(plot_ly())
  
  # Calculate daily profit
  daily_profit <- dt[kelly_criterion > 0, .(
    n_bets = .N,
    n_winners = sum(winner, na.rm = TRUE),
    wagered = sum(kelly_criterion * 100),
    returned = sum(ifelse(winner, kelly_criterion * price * 100, 0)),
    profit = sum(ifelse(winner, kelly_criterion * price * 100, 0)) - sum(kelly_criterion * 100)
  ), by = game_date_est]
  
  setorder(daily_profit, game_date_est)
  daily_profit[, cumulative_profit := cumsum(profit)]
  
  # Create plot
  plot_ly(daily_profit) %>%
    add_lines(x = ~game_date_est, y = ~cumulative_profit, 
              name = "Cumulative P/L",
              line = list(color = app_config$color_green, shape = 'spline', smoothing = 0.5)) %>%
    add_lines(x = ~game_date_est, y = ~cumsum(wagered * 0.0476 * -1), 
              name = 'Juice Tax',
              line = list(shape = 'spline', smoothing = 0.5, dash = "dash")) %>%
    add_bars(x = ~game_date_est, y = ~profit,
             name = "Daily P/L",
             marker = list(color = ~ifelse(profit > 0, 
                                           app_config$color_green, 
                                           app_config$color_red),
                           opacity = 0.6)) %>%
    layout(title = list(text = 'Cumulative Profit/Loss Over Time', y = 0.97),
           xaxis = list(title = "Date"),
           yaxis = list(title = "Dollars ($)"),
           showlegend = FALSE)
}

# DATA LOADING ------------------------------------------------------------

log_message("Starting NHL Betting Dashboard initialization")

# Load data
game_data <- load_app_data()
completed_games <- game_data$completed
upcoming_games <- game_data$upcoming

# Create game column for compatibility
if (!is.null(completed_games) && nrow(completed_games) > 0) {
  completed_games[, game := paste(away, "at", home)]
}

if (!is.null(upcoming_games) && nrow(upcoming_games) > 0) {
  upcoming_games[, game := paste(away, "at", home)]
}

# Load logos
nhl_logos <- load_team_logos()

# Extract team colors
team_colors <- if (!is.null(nhl_logos)) {
  extract_team_colors(nhl_logos)
} else {
  data.frame(team = character(), color_hex = character(), priority = integer())
}

# UI DEFINITION -----------------------------------------------------------

ui <- page_navbar(
  title = app_config$app_title,
  theme = bs_theme(preset = app_config$theme),
  
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
  
  # Reactive: Value bets
  value_bets <- reactive({
    if (is.null(completed_games) || nrow(completed_games) == 0) return(data.table())
    
    # Get best kelly criterion bet per game/team
    completed_games[game_state %in% c("FUT", "PRE"), .SD[which.max(kelly_criterion)], by = .(game_id, team)]
  })
  
  # Reactive: Profit by date
  profit_by_date <- reactive({
    val_bets <- value_bets()
    if (nrow(val_bets) == 0) return(data.table())
    
    val_bets[kelly_criterion > 0, .(
      N_BETS = .N,
      N_WINNERS = sum(winner, na.rm = TRUE),
      TOTAL_WAGERED = sum(kelly_criterion * 100),
      TOTAL_RETURN = sum(ifelse(winner, kelly_criterion * price * 100, 0)),
      PROFIT = sum(ifelse(winner, kelly_criterion * price * 100, 0)) - sum(kelly_criterion * 100)
    ), by = game_date_est]
  })
  
  # Output: Profit/Loss plot
  output$profit_loss_plot <- renderPlotly({
    create_profit_plot(value_bets())
  })
  
  # Output: EV plot
  output$ev_plot <- renderPlotly({
    create_ev_plot(value_bets(), interactive = TRUE)
  })
  
  # Reactive: Win probability bins
  win_prob_bin_df <- reactive({
    val_bets <- value_bets()
    if (nrow(val_bets) == 0) return(data.table())
    
    val_bets[, `:=`(
      WIN_PROB_BIN = cut(win_probability, breaks = seq(0, 1, by = 0.10), 
                         include.lowest = TRUE, right = FALSE),
      Price_BIN = cut(rank(price), breaks = 10, labels = FALSE)
    )]
  })
  
  # Output: Win percentage plot
  output$win_percentage_plot <- renderPlotly({
    bin_df <- win_prob_bin_df()
    if (nrow(bin_df) == 0) {
      return(plot_ly() %>% layout(title = "No data available"))
    }
    
    win_data <- bin_df[, .(
      AVERAGE_WIN_PROB = mean(win_probability),
      WINNERS = sum(winner, na.rm = TRUE),
      GAMES = .N,
      OBSERVED_WIN_PERCENTAGE = sum(winner, na.rm = TRUE) / .N
    ), by = WIN_PROB_BIN]
    
    plot_ly(win_data, x = ~WIN_PROB_BIN) %>%
      add_trace(y = ~AVERAGE_WIN_PROB * 100, name = 'Expected', type = "bar",
                text = ~round(AVERAGE_WIN_PROB * 100, 1), textposition = 'outside',
                opacity = 0.75, marker = list(color = app_config$color_red)) %>%
      add_trace(y = ~OBSERVED_WIN_PERCENTAGE * 100, name = 'Observed', type = "bar",
                text = ~round(OBSERVED_WIN_PERCENTAGE * 100, 1), textposition = 'outside',
                opacity = 0.75, marker = list(color = app_config$color_green)) %>%
      layout(
        barmode = 'overlay',
        xaxis = list(title = 'Win Probability'),
        yaxis = list(title = 'Win Percentage', range = c(0, 100)),
        legend = list(
          title = list(text = 'Win Percentage'),
          x = 0.1, y = 0.95, bgcolor = 'rgba(0,0,0,0)'
        )
      )
  })
  
  # Output: Implied win percentage plot
  output$implied_win_percentage_plot <- renderPlotly({
    bin_df <- win_prob_bin_df()
    if (nrow(bin_df) == 0) {
      return(plot_ly() %>% layout(title = "No data available"))
    }
    
    line_data <- bin_df[, .(
      AVERAGE_LINE = mean(price),
      EXP_WIN_PROB = 1 / mean(price),  # Implied probability from line
      WINNERS = sum(winner, na.rm = TRUE),
      GAMES = .N,
      OBSERVED_WIN_PERCENTAGE = sum(winner, na.rm = TRUE) / .N
    ), by = Price_BIN]
    
    plot_ly(line_data, y = ~as.factor(round(AVERAGE_LINE, 2))) %>%
      add_trace(x = ~EXP_WIN_PROB * 100, name = 'Expected',
                type = 'bar', orientation = 'h',
                hovertext = ~paste0('<b>Exp. Win %:</b> ', round(EXP_WIN_PROB * 100, 1)),
                textposition = 'outside',
                opacity = 0.75, marker = list(color = app_config$color_red)) %>%
      add_trace(x = ~OBSERVED_WIN_PERCENTAGE * 100, name = 'Observed',
                type = 'bar', orientation = 'h',
                hovertext = ~paste0('<b>Obs. Win %:</b> ', round(OBSERVED_WIN_PERCENTAGE * 100, 1)),
                textposition = 'outside',
                opacity = 0.75, marker = list(color = app_config$color_green)) %>%
      layout(
        barmode = 'overlay',
        yaxis = list(title = 'Line'),
        xaxis = list(title = 'Win Percentage', range = c(0, 100)),
        legend = list(
          title = list(text = 'Win Percentage'),
          x = 0.6, y = 0.95, bgcolor = 'rgba(0,0,0,0)'
        )
      )
  })
  
  # Output: Game selection table
  output$narrow_dt <- renderDT({
    if (is.null(upcoming_games) || nrow(upcoming_games) == 0) {
      return(datatable(data.frame(Game = character())))
    }
    
    datatable(
      data.frame(Game = unique(upcoming_games$game)),
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
  })
  
  # Reactive: Selected game
  selected_game <- reactive({
    req(input$narrow_dt_row_last_clicked)
    if (is.null(upcoming_games) || nrow(upcoming_games) == 0) return(NULL)
    
    unique_games <- unique(upcoming_games$game)
    if (input$narrow_dt_row_last_clicked <= length(unique_games)) {
      return(unique_games[input$narrow_dt_row_last_clicked])
    }
    return(NULL)
  })
  
  # Output: Line movement plot
  output$line_movement_plot <- renderPlotly({
    sel_game <- selected_game()
    if (is.null(sel_game)) {
      return(plot_ly() %>% layout(title = "Select a game to view line movement"))
    }

    # Filter for selected game
    game_data <- upcoming_games[game == sel_game]

    if (nrow(game_data) == 0) {
      return(plot_ly() %>% layout(title = "No data for selected game"))
    }

    # Add team colors
    game_data <- merge(game_data, team_colors, by = "team", all.x = TRUE)
    game_data[is.na(color_hex), color_hex := app_config$color_gray]

    # Calculate time relative to game start
    game_data[, `:=`(
      retrieval_time = as.POSIXct(retrieval_time),
      game_time_est = as.POSIXct(game_time_est),
      hours_to_game = as.numeric(difftime(retrieval_time, game_time_est, units = "hours"))
    )]
    setorder(game_data, team, hours_to_game)

    p <- plot_ly()
    for (tm in unique(game_data$team)) {
      tm_data <- game_data[team == tm]
      col <- tm_data$color_hex[1]
      p <- p %>%
        add_trace(
          data = tm_data,
          x = ~hours_to_game, y = ~kelly_criterion,
          type = "scatter", mode = "lines+markers",
          line = list(color = col, shape = "hv"),
          marker = list(color = col),
          hoverlabel = list(bgcolor = col),
          hoverinfo = "text",
          hovertext = ~paste0('<b>Team:</b> ', team,
                              '<br><b>KC:</b> ', round(kelly_criterion, 2),
                              '<br><b>Line:</b> ', round(price, 2),
                              '<br><b>Win Prob:</b> ', round(win_probability, 2),
                              '<br><b>Hours to game:</b> ', round(-hours_to_game, 1)),
          name = paste(tm, "KC"),
          yaxis = "y3",
          visible = TRUE,
          showlegend = TRUE
        ) %>%
        add_trace(
          data = tm_data,
          x = ~hours_to_game, y = ~price,
          type = "scatter", mode = "lines+markers",
          line = list(color = col, dash = "dot", shape = "hv"),
          marker = list(color = col),
          hoverlabel = list(bgcolor = col),
          hoverinfo = "text",
          hovertext = ~paste0('<b>Team:</b> ', team,
                              '<br><b>Line:</b> ', round(price, 2),
                              '<br><b>Hours to game:</b> ', round(-hours_to_game, 1)),
          name = paste(tm, "Line"),
          yaxis = "y",
          visible = FALSE,
          showlegend = FALSE
        ) %>%
        add_trace(
          data = tm_data,
          x = ~hours_to_game, y = ~win_probability,
          type = "scatter", mode = "lines+markers",
          line = list(color = col, dash = "dashdot", shape = "hv"),
          marker = list(color = col),
          hoverlabel = list(bgcolor = col),
          hoverinfo = "text",
          hovertext = ~paste0('<b>Team:</b> ', team,
                              '<br><b>Win Prob:</b> ', round(win_probability, 2),
                              '<br><b>Hours to game:</b> ', round(-hours_to_game, 1)),
          name = paste(tm, "Win Prob"),
          yaxis = "y2",
          visible = FALSE,
          showlegend = FALSE
        )
    }

    n_teams <- length(unique(game_data$team))
    n_traces_per_team <- 3
    total_traces <- n_teams * n_traces_per_team
    vis_kc_only <- rep(FALSE, total_traces)
    vis_kc_price <- rep(FALSE, total_traces)
    vis_kc_winprob <- rep(FALSE, total_traces)
    for (i in 0:(n_teams - 1)) {
      base <- i * n_traces_per_team
      vis_kc_only[base + 1] <- TRUE
      vis_kc_price[base + c(1,2)] <- TRUE
      vis_kc_winprob[base + c(1,3)] <- TRUE
    }

    # x_rng <- range(game_data$hours_to_game)
    x_rng <- c(min(game_data$hours_to_game) - 5, 0)
    y_rng <- range(c(game_data$kelly_criterion, -0.2, 0.2))

    # Create background gradient for Kelly Criterion
    # kc_range <- range(c(game_data$kelly_criterion, 0, 0.1))
    kc_range <- range(c(-0.5, 0, 0.5))
    shapes <- list()
    if (kc_range[1] < 0) {
      shapes <- c(shapes, list(list(
        type = "rect", xref = "x", yref = "y3", layer = "below",
        x0 = min(x_rng), x1 = 0,
        y0 = kc_range[1], y1 = min(0, kc_range[2]),
        fillcolor = "rgba(255,0,0,0.15)", line = list(width = 0)
      )))
    }
    if (kc_range[2] > 0) {
      shapes <- c(shapes, list(list(
        type = "rect", xref = "x", yref = "y3", layer = "below",
        x0 = min(x_rng), x1 = 0,
        y0 = max(0, kc_range[1]), y1 = min(0.05, kc_range[2]),
        fillcolor = "rgba(255,165,0,0.15)", line = list(width = 0)
      )))
    }
    if (kc_range[2] > 0.05) {
      shapes <- c(shapes, list(list(
        type = "rect", xref = "x", yref = "y3", layer = "below",
        x0 = min(x_rng), x1 = 0,
        y0 = max(0.05, kc_range[1]), y1 = min(0.1, kc_range[2]),
        fillcolor = "rgba(255,255,0,0.15)", line = list(width = 0)
      )))
    }
    if (kc_range[2] > 0.1) {
      shapes <- c(shapes, list(list(
        type = "rect", xref = "x", yref = "y3", layer = "below",
        x0 = min(x_rng), x1 = 0,
        y0 = max(0.1, kc_range[1]), y1 = kc_range[2],
        fillcolor = "rgba(0,128,0,0.15)", line = list(width = 0)
      )))
    }

    p %>%
      layout(
        title = list(text = sel_game, y = 0.97),
        xaxis = list(
          title = "Hours Before Game",
          range = c(min(x_rng), 0),
          zerolinecolor = "#CCCCCC",
          zerolinewidth = 3
        ),
        yaxis = list(title = "Line", side = "left"),
        yaxis2 = list(title = "Win Probability", overlaying = "y", side = "left", position = 0.05, range = c(0, 1)),
        yaxis3 = list(title = "Kelly Criterion", overlaying = "y", side = "right", range = c(y_rng[1]-0.05, y_rng[2]+0.05)),
        legend = list(title = list(text = "Team"), x = 0.02, y = 0.02),
        margin = list(r = 50),
        shapes = shapes,
        updatemenus = list(
          list(
            active = 0,
            type = "buttons",
            direction = "down",
            x = 0.1,
            y = 1.1,
            buttons = list(
              list(
                label = "Kelly Only",
                method = "restyle",
                args = list("visible", vis_kc_only)
              ),
              list(
                label = "+ Price",
                method = "restyle",
                args = list("visible", vis_kc_price)
              ),
              list(
                label = "+ Win Probability",
                method = "restyle",
                args = list("visible", vis_kc_winprob)
              )
            )
          )
        )
      )
  })
  
  # Output: Upcoming games table
  output$upcoming_games_table <- renderDT({
    if (is.null(upcoming_games) || nrow(upcoming_games) == 0) {
      return(datatable(data.frame(Message = "No upcoming games")))
    }
    
    # Get latest observation per team
    games_data <- upcoming_games[, .SD[which.max(retrieval_time)], by = team]
    setorder(games_data, game_date_est, game, home_or_away)
    
    # Create compact display
    games_compact <- games_data[, .(
      display = paste0(
        '<div style="display: flex; align-items: center; justify-content: center;">',
        '<div style="text-align: center; width: 155px;">',
        create_logo_html_tag(.SD[home_or_away == "away", team], nhl_logos, "default", 40),
        '<br><strong>', .SD[home_or_away == "away", team], '</strong><br>',
        'Line: ', round(.SD[home_or_away == "away", price], 2), '<br>',
        'KC: ', round(.SD[home_or_away == "away", kelly_criterion] * 100, 2), '%<br>',
        'Goalie: ', .SD[home_or_away == "away", goalie],
        '</div>',
        '<div style="text-align: center; width: 20px;"><strong>VS</strong></div>',
        '<div style="text-align: center; width: 155px;">',
        create_logo_html_tag(.SD[home_or_away == "home", team], nhl_logos, "default", 40),
        '<br><strong>', .SD[home_or_away == "home", team], '</strong><br>',
        'Line: ', round(.SD[home_or_away == "home", price], 2), '<br>',
        'KC: ', round(.SD[home_or_away == "home", kelly_criterion] * 100, 2), '%<br>',
        'Goalie: ', .SD[home_or_away == "home", goalie],
        '</div></div>'
      )
    ), by = .(game_date_est, game)]
    
    datatable(
      games_compact[, c("game_date_est", "display")],
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
      colnames = c("Date" = "game_date_est", "Matchup" = "display")
    )
  })
  
  # Output: Upcoming EV plot
  output$upcoming_ev_plot <- renderPlot({
    if (is.null(upcoming_games) || nrow(upcoming_games) == 0) {
      return(NULL)
    }
    
    # Get latest observation per team
    up_val_df <- upcoming_games[, .SD[which.max(retrieval_time)], by = team]
    
    # Ensure numeric columns
    up_val_df[, `:=`(
      price = as.numeric(price),
      win_percent = as.numeric(win_probability)
    )]
    
    # Filter valid data
    up_val_df <- up_val_df[!is.na(price) & !is.na(win_percent) & 
                             price > 0 & win_percent > 0 & win_percent <= 1]
    
    if (nrow(up_val_df) == 0) return(NULL)
    
    # Create EV plot with team logos
    temp_logo_dir <- tempdir()
    
    # Process logos
    for (i in 1:nrow(up_val_df)) {
      team_key <- gsub(" ", "_", toupper(up_val_df$team[i]))
      if (team_key %in% names(nhl_logos$teams)) {
        logo_data <- nhl_logos$teams[[team_key]]$logos[["default"]]
        if (!is.null(logo_data) && logo_data$success) {
          logo_file <- file.path(temp_logo_dir, paste0(team_key, ".png"))
          writePNG(logo_data$image, logo_file)
          up_val_df[i, logo_path := logo_file]
        }
      }
    }
    
    # Create base plot with EV background
    p <- ggplot(up_val_df, aes(x = win_percent, y = price)) +
      ev_background_layers(c(min(up_val_df$win_percent) - 0.05, max(up_val_df$win_percent) + 0.05)) + 
      coord_cartesian(ylim = c(min(up_val_df$price) - 0.2, max(up_val_df$price) + 0.2))
    
    # Add logos or points
    if ("logo_path" %in% names(up_val_df)) {
      p <- p + ggimage::geom_image(aes(image = logo_path), size = 0.08)
    } else {
      p <- p + geom_point(size = 3, color = app_config$color_gray)
    }
    
    p + labs(x = "Win Probability", y = "Decimal Line", 
             title = "Upcoming Games Expected Value") +
      theme_minimal()
  })
  
  # Observer: Quit button
  observeEvent(input$quit_btn, {
    log_message("User clicked quit button, stopping app")
    stopApp()
  })
}

# RUN APPLICATION ---------------------------------------------------------

log_message(sprintf("Loaded %d completed, %d upcoming games",
                    ifelse(is.null(completed_games), 0, nrow(completed_games)),
                    ifelse(is.null(upcoming_games), 0, nrow(upcoming_games))))

shinyApp(ui = ui, server = server)
