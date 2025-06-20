rm(list = ls())
gc()

setwd("/Users/david/PersonalDevelopment/EVC")
source('scripts/ev_dashboard_functions.R')

needed <- c('R6','data.table','tidyverse','plotly', 'bslib', 'shiny')
missing <- needed[!(needed %in% utils::installed.packages()[, "Package"])]
if (length(missing)) {
  if (interactive()) {
    answer <- askYesNo(paste("Package(s)", paste(missing, collapse = " "),
                             "needed. Install from CRAN?"))
    if (answer)
      install.packages(missing, repos = "https://cloud.r-project.org/")
  } else {
    stop("Install missing packages: ", paste(missing, collapse = ", "))
  }
}

library(R6)
library(data.table)
library(tidyverse)
library(plotly)
library(DT)
library(shiny)
library(bslib)

sports <- c('NBA','NFL','NHL','ERE','MLB','EPL')

ui <- page_navbar(
  theme = bs_theme(preset = 'darkly'),
  title = 'EV Results Dashboard',
  
  # Navigation
  nav_panel(title = 'Profit/Loss',
            card(
              full_screen = TRUE,
              plotlyOutput('profit_loss')
              )
            ),
  nav_panel(title = 'Odds Breakdown',
            card(
              full_screen = TRUE,
              plotlyOutput('odds_breakdown_plot')
            ),
            card(
              full_screen = TRUE,
              plotlyOutput('odds_breakdown_bets_only_plot')
            )
            ),
  nav_panel(title = 'Line & Probability Analysis',
            card(
              full_screen = TRUE,
              plotlyOutput('win_percentage_plot')
            ),
            card(
              full_screen = TRUE,
              plotlyOutput('line_plot')
            )
            ),
  nav_panel(title = 'Line Movement',
            card(
              full_screen = TRUE,
              plotlyOutput('line_movement_plot')
            )
            ),
  nav_panel(title = 'Results Table',
            card(
              DT::dataTableOutput('results_table')
            )
            ),
  nav_panel(title = 'Upcoming Games',
            card(
              DT::dataTableOutput('upcoming_games_table')
            )),
  nav_spacer(),
  nav_menu(
    title = 'Links', 
    align = 'right',
    nav_item(tags$a('Github', href = 'https://github.com/DavidJmsn/EVC_DJ'))
  ),
  
  # Sidebar
  sidebar = sidebar(
    card(
      selectInput(
        'sport',
        'Sport', 
        choices = sports 
      ),
      sliderInput(
        'min_kc', 
        'Min KC', 
        min = 0, max = 0.25, step = 0.025,
        value = 0
      )
    ),
    card(
      card_header('Return Rate'),
      textOutput('return_rate')
    ),
    card(
      card_header('Win Rate'),
      textOutput('win_rate')
    ),
    card(
      card_header('Juice Adj. Win Rate'),
      textOutput('juice_adj_win_rate')
    )
  )
)

server <- function(input, output, session) {
  setwd("/Users/david/PersonalDevelopment/EVC")
  
  # Data
  dt_history <- reactive({
    dt_history <- fread(paste0(input$sport, '/history/', 'history.csv'))
    dt_history[, LINE_MOVEMENT_TIME := as.double(difftime(`Last Update`, `Start Time`, units = 'hours')),
               by = .(GAME_ID, TEAM)]
    return(setorder(dt_history, -GAME_DATE, GAME_ID))
  })
  
  dt_max_kc <- reactive({dt_history()[KC > input$min_kc & LINE_MOVEMENT_TIME <= 0, .SD[which.max(KC)], by = .(GAME_ID, TEAM)]})
  
  vec_bet_both_ways <- reactive({dt_max_kc()[duplicated(GAME_ID), GAME_ID]})
  
  dt_bet_both_ways <- reactive({dt_max_kc()[GAME_ID %in% vec_bet_both_ways]})
  
  dt_no_bet <- reactive({dt_history()[GAME_ID %ni% dt_max_kc$GAME_ID]})
  
  dt_profit_by_date <- reactive({dt_profit_by_date <- dt_max_kc()[!is.na(WINNER) & KC > input$min_kc, 
                                                                  .(TOTAL_WAGERED = sum(KC*100), 
                                                                    TOTAL_RETURN = sum(ifelse(WINNER, (KC*Price*100), 0)),
                                                                    PROFIT = sum(ifelse(WINNER, (KC*Price*100), 0)) - sum(KC*100)), 
                                                                  by = GAME_DATE]
  return(setorder(dt_profit_by_date, GAME_DATE))
  })
  
  # Sidebar statistics
  return_percentage <- reactive({round(return_rate(sum(dt_profit_by_date()$TOTAL_RETURN),sum(dt_profit_by_date()$TOTAL_WAGERED)), 2)})
  
  effective_win_percentage <- reactive({
    round(effective_win_rate(sum(dt_profit_by_date()$TOTAL_RETURN),sum(dt_profit_by_date()$TOTAL_WAGERED), juice_adjusted = FALSE), 2)
    })
  
  effective_win_percentage_juice_adjusted <- reactive({
    round(effective_win_rate(sum(dt_profit_by_date()$TOTAL_RETURN),sum(dt_profit_by_date()$TOTAL_WAGERED), juice_adjusted = TRUE), 2)
    })
  
  output$return_rate <- renderText(return_percentage())
  output$win_rate <- renderText(effective_win_percentage())
  output$juice_adj_win_rate <- renderText(effective_win_percentage_juice_adjusted())
  
  
# Profit loss plot
  profit_loss_fig <- reactive({
    plot_ly(data = dt_profit_by_date()) |>
      add_lines(x = ~GAME_DATE, y = ~cumsum(PROFIT), name = 'Profit/Loss', 
                line = list(shape = 'spline', smoothing = 0.5)) |> 
      add_lines(x = ~ GAME_DATE, y = ~cumsum(TOTAL_WAGERED*0.0476*-1), name = 'Juice Tax',
                line = list(shape = 'spline', smoothing = 0.5)) |> 
      add_bars(x = ~GAME_DATE, y = ~PROFIT, 
               color = ~ifelse(PROFIT > 0, 'Daily Profit', 'Daily Loss')) |>
      layout(
        title = list(text = 'Cumulative Profit Loss Over Time',
                     y = 0.97),
        xaxis = list(title = 'Date'),
        yaxis = list(title = 'Dollars ($)')
      )
  })
  
  output$profit_loss <- renderPlotly(profit_loss_fig())
  
  # Odds breakdown plots
  odds_plot <- reactive({
    plot_ev(games_dt = dt_history()[!is.na(WINNER) & LINE_MOVEMENT_TIME <= 0, .SD[which.max(KC)], by = .(GAME_ID, TEAM)],
            title = 'Odds vs Win Probability - All Games')
  })
  
  bets_plot <- reactive({
    plot_ev(games_dt = dt_max_kc()[!is.na(WINNER)],
            title = 'Odds vs Win Probability - Bets (Positive EV)')
  })
  
  output$odds_breakdown_plot <- renderPlotly(odds_plot())
  
  output$odds_breakdown_bets_only_plot <- renderPlotly(bets_plot())
  
  # Win percentage table
  dt_win_prob_vs_observed <- reactive({
    dt_max_kc()[, `:=` (WIN_PROB_BIN = cut(WIN_PROB, breaks = seq(0, 100, by = 10), include.lowest = TRUE, right = FALSE),
                        Price_BIN = cut(rank(Price), breaks = 10, labels = FALSE))]
    dt_win_prob_vs_observed <- data.table::copy(dt_max_kc()[!is.na(WINNER), .(AVERAGE_WIN_PROB = mean(WIN_PROB),
                                                                              WINNERS = sum(WINNER), 
                                                                              GAMES = .N, 
                                                                              OBSERVED_WIN_PERCENTAGE = sum(WINNER)/.N), 
                                                            by = .(WIN_PROBABILITY = WIN_PROB_BIN)])
    return(setorder(dt_win_prob_vs_observed, WIN_PROBABILITY))
  })
  
  # Line percentage table 
  dt_line_vs_observed <- reactive({
    dt_line_vs_observed <- data.table::copy(dt_max_kc()[!is.na(WINNER), .(AVERAGE_LINE = mean(Price),
                                                                          EXP_WIN_PROB = get_prob(line = mean(Price)),
                                                                          WINNERS = sum(WINNER), 
                                                                          GAMES = .N, 
                                                                          OBSERVED_WIN_PERCENTAGE = sum(WINNER)/.N,
                                                                          MIN_LINE = min(Price), 
                                                                          MAX_LINE = max(Price)), 
                                                        by = .(LINE = Price_BIN)])
    
    return(setorder(dt_line_vs_observed, LINE))
  })
  
  output$win_percentage_plot <- renderPlotly(plot_ly(data = dt_win_prob_vs_observed(), x = ~WIN_PROBABILITY) |>
                                               add_trace(y = ~AVERAGE_WIN_PROB, name = 'Expected',
                                                         mode = 'bars', text = ~round(AVERAGE_WIN_PROB, 1), textposition = 'outside',
                                                         opacity = 0.3) |>
                                               add_trace(y = ~OBSERVED_WIN_PERCENTAGE*100, name = 'Observed',
                                                         mode = 'bars',
                                                         text = ~round(OBSERVED_WIN_PERCENTAGE*100, 1), textposition = 'outside',
                                                         opacity = 0.75) |>
                                               layout(barmode = 'overlay',
                                                      xaxis = list(title = 'Win Probability'),
                                                      yaxis = list(title = 'Win Percentage', range = c(0,100)),
                                                      legend= list(title = list(text = 'Win Percentage'))))
  
  output$line_plot <- renderPlotly(plot_ly(data = dt_line_vs_observed(), y = ~as.factor(round(AVERAGE_LINE,2))) |>
                                     add_trace(x = ~EXP_WIN_PROB, name = 'Expected Win Percentage',
                                               mode = 'bars', orientation = 'h',
                                               hovertext = ~paste0('<b>Exp. Win %: </b>', round(EXP_WIN_PROB, 1)), textposition = 'outside',
                                               opacity = 0.75) |>
                                     add_trace(x = ~OBSERVED_WIN_PERCENTAGE*100, name = 'Observed Win Percentage',
                                               mode = 'bars', orientation = 'h',
                                               hovertext = ~paste0('<b>Obs. Win %: </b>', round(OBSERVED_WIN_PERCENTAGE*100, 1)), textposition = 'outside',
                                               opacity = 0.75) |>
                                     layout(barmode = 'overlay',
                                            yaxis = list(title = 'Line'),
                                            xaxis = list(title = 'Win Percentage', range = c(0,100)),
                                            legend= list(title = list(text = 'Win Percentage'))))
  
  # Line movement plots
  line_movement_fig <- reactive({
    dt_history()[KC > 0] |>
      group_by(as.factor(interaction(GAME_ID, TEAM))) |>
      plot_ly(x = ~LINE_MOVEMENT_TIME, y = ~KC, color = ~as.factor(WINNER)) |>
      add_lines() |>
      layout(
        title = list(text = 'Line Movement Before Gametime',
                     y = 0.97),
        xaxis = list(title = 'Hours Before Gametime'),
        yaxis = list(title = 'Kelly Criterion'),
        legend = list(
          title = list(text = "Winner"),
          x = 1,  # Adjust the x position of the legend
          y = 0.5   # Adjust the y position of the legend
        )
      )
  })
  
  output$line_movement_plot <- renderPlotly(line_movement_fig())
  
  # Results table
  output$results_table <- DT::renderDT(DT::datatable(dt_history(), 
                                                     rownames = FALSE))
  
   # Upcoming games table
  dt_upcoming_games <- reactive({
    dt_history()[is.na(WINNER), .(GAME, GAME_DATE, TEAM, WIN_PROB, Price, EV, KC, MIN_PRICE, Book, LINE_MOVEMENT_TIME)]
    })
  
  output$upcoming_games_table <- DT::renderDT(DT::datatable(dt_upcoming_games(), 
                                                            rownames = FALSE
  ) |> 
    formatRound(columns = sapply(dt_upcoming_games(), is.numeric), digits = 3) )
  
}

shinyApp(ui, server)