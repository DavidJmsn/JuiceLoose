

# SHINY APP TO VIZUALIZE RESULTS FROM NFL PREDICTIONS ---------------------

# Author: David Jamieson
# Date: 2024


# CLEAN ENVIRONMENT -------------------------------------------------------

rm(list = ls())
gc()


# SET WORKING DIRECTORY & SOURCE FILES ------------------------------------

setwd("/Users/david/PersonalDevelopment/EVC")
source('scripts/functions.R')


# LOAD LIBRARIES ----------------------------------------------------------

needed <- c('R6','data.table','tidyverse','plotly', 'bslib', 'shiny', 'DT')
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

for(l in needed){
  suppressPackageStartupMessages(library(l, character.only = T))
}

# Dashboard specifically for the NFL
sports <- c('NFL')


# UI ----------------------------------------------------------------------

ui <- page_navbar(
  theme = bs_theme(preset = 'darkly', version = 5),
  title = 'EV Results Dashboard',
  tags$head(
    tags$style(HTML("
      .full-height {
        height: 90vh; /* 90% of viewport height */
        display: flex;
        flex-direction: column;
      }
      .card {
        flex-grow: 1; /* Ensure the cards fill remaining height */
      }
    "))
  ),
  
  # Navigation
  nav_panel(title = 'Profit/Loss',
            card(
              full_screen = TRUE,
              plotlyOutput('profit_loss')
            )
  ),
  nav_panel(title = 'Spread Profit/Loss',
            card(
              full_screen = TRUE,
              plotlyOutput('spread_profit_loss')
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
            fluidRow(
              column(10,
                     div(class = "full-height",
                         card(
                           full_screen = TRUE,
                           plotlyOutput('line_movement_plot')
                         )
                     )
              ),
              column(2,
                     div(class = "full-height",
                         card(
                           DT::dataTableOutput('narrow_games_table')
                         )
                     )
              )
            )
            
  ),
  nav_panel(title = 'Results Table',
            card(
              DT::dataTableOutput('results_table')
            )
  ),
  nav_panel(title = 'Max Value Table',
            card(
              DT::dataTableOutput('max_kc_table')
            )
  ),
  nav_panel(title = 'Upcoming Games',
            card(
              DT::dataTableOutput('upcoming_games_table')
            )),
  nav_spacer(),
  nav_item(
    actionButton("quit_btn", "Quit",
                 icon = icon("power-off"))
  ),
  nav_menu(
    title = '', 
    align = 'right',
    nav_item(tags$a('Github', href = 'https://github.com/DavidJmsn/EVC_DJ'), icon = icon("github")),
    icon = icon("share-alt")
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
      card_header('Moneyline'),
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
    ),
    card(
      card_header('Spread'),
      card(
        card_header('Return Rate'),
        textOutput('spread_return_rate')
      ),
      card(
        card_header('Win Rate'),
        textOutput('spread_win_rate')
      ),
      card(
        card_header('Juice Adj. Win Rate'),
        textOutput('spread_juice_adj_win_rate')
      )
    )
  )
)


# SERVER ------------------------------------------------------------------

server <- function(input, output, session) {
  setwd("/Users/david/PersonalDevelopment/EVC")
  
  config <- reactive({switch(EXPR = sport, 
                             'NFL' = yaml::yaml.load_file("NFL/Dictionary/espnTeams.yml"))})
  
  # Data
  dt_history <- reactive({
    dt_history <- fread(paste0(input$sport, '/history/', 'h2h_data_record.csv'))
    dt_history[, GAME_DATE := as.Date(`Start Time`)]
    dt_history[, WIN_PROB := `WIN PROB`]
    dt_history[, LINE_MOVEMENT_TIME := as.double(difftime(`Last Update`, `Start Time`, units = 'hours')),
               by = .(GAME_ID, Team)]
    dt_history[, WEEK := as.integer(get_nfl_week(current_date = (`GAME_DATE`-1))), by = .I]
    dt_history[, COLOUR := paste0("#", ifelse(config()[[AB]][["color"]] == "000000", 
                                              yes = config()[[AB]][["alternateColor"]],
                                              no = config()[[AB]][["color"]])), by = .I]
    return(setorder(dt_history, -`GAME_DATE`, GAME_ID))
  })
  
  dt_spread <- reactive({
    dt_spread <- fread(paste0(input$sport, '/history/', 'spread_data_record.csv'))
    dt_spread[, GAME_DATE := as.Date(`Start Time`)]
    dt_spread[, LINE_MOVEMENT_TIME := as.double(difftime(`Last Update`, `Start Time`, units = 'hours')),
               by = .(GAME_ID, Team)]
    dt_spread[, WEEK := as.integer(get_nfl_week(current_date = (`GAME_DATE`-1))), by = .I]
    dt_spread[, COLOUR := paste0("#",ifelse(config()[[AB]][["color"]] == "000000", 
                                            yes = config()[[AB]][["alternateColor"]],
                                            no = config()[[AB]][["color"]])), by = .I]
    return(setorder(dt_spread, -`GAME_DATE`, GAME_ID))
  })
  
  dt_max_kc <- reactive({dt_history()[KC > input$min_kc & LINE_MOVEMENT_TIME <= 0,
                                      .SD[which.max(KC)], by = .(GAME_ID, Team)]})
  
  dt_max_spread <- reactive({dt_spread()[PTS_PERCENT_ADJ > input$min_kc & LINE_MOVEMENT_TIME <= 0,
                                         .SD[which.max(PTS_PERCENT_ADJ)], by = .(GAME_ID, Team)]})
  
  vec_bet_both_ways <- reactive({dt_max_kc()[duplicated(GAME_ID), GAME_ID]})
  
  dt_bet_both_ways <- reactive({dt_max_kc()[GAME_ID %in% vec_bet_both_ways]})
  
  dt_no_bet <- reactive({dt_history()[GAME_ID %ni% dt_max_kc$GAME_ID]})
  
  dt_profit_by_date <- reactive({dt_profit_by_date <- dt_max_kc()[!is.na(WINNER) & KC > input$min_kc, 
                                                                  .(N_BETS = .N,
                                                                    N_WINNERS = sum(WINNER),
                                                                    TOTAL_WAGERED = sum(KC*100), 
                                                                    TOTAL_RETURN = sum(ifelse(WINNER, (KC*Price*100), 0)),
                                                                    PROFIT = sum(ifelse(WINNER, (KC*Price*100), 0)) - sum(KC*100)), 
                                                                  by = WEEK]
  return(setorder(dt_profit_by_date, WEEK))
  })
  
  dt_spread_profit_by_date <- reactive({dt_spread_profit_by_date <- dt_max_spread()[SCORE + POINT_DIFF > 0 & PTS_PERCENT_ADJ > input$min_kc, 
                                                                                    .(N_BETS = .N,
                                                                                      N_WINNERS = sum(SPREAD_WINNER),
                                                                                      TOTAL_WAGERED = sum(PTS_PERCENT_ADJ*100), 
                                                                                      TOTAL_RETURN = sum(ifelse(SPREAD_WINNER, (PTS_PERCENT_ADJ*Price*100), 0)),
                                                                                      PROFIT = sum(ifelse(SPREAD_WINNER, (PTS_PERCENT_ADJ*Price*100), 0)) - sum(PTS_PERCENT_ADJ*100)), 
                                                                                    by = WEEK]
  return(setorder(dt_spread_profit_by_date, WEEK))
  })
  
  # Sidebar statistics
  return_percentage <- reactive({round(return_rate(sum(dt_profit_by_date()$TOTAL_RETURN),sum(dt_profit_by_date()$TOTAL_WAGERED)), 2)})
  
  effective_win_percentage <- reactive({
    round(effective_win_rate(sum(dt_profit_by_date()$TOTAL_RETURN),sum(dt_profit_by_date()$TOTAL_WAGERED), juice_adjusted = FALSE), 2)
  })
  
  effective_win_percentage_juice_adjusted <- reactive({
    round(effective_win_rate(sum(dt_profit_by_date()$TOTAL_RETURN),sum(dt_profit_by_date()$TOTAL_WAGERED), juice_adjusted = TRUE), 2)
  })
  
  # Sidebar statistics
  spread_return_percentage <- reactive({round(return_rate(sum(dt_spread_profit_by_date()$TOTAL_RETURN),sum(dt_spread_profit_by_date()$TOTAL_WAGERED)), 2)})
  
  spread_effective_win_percentage <- reactive({
    round(effective_win_rate(sum(dt_spread_profit_by_date()$TOTAL_RETURN),sum(dt_spread_profit_by_date()$TOTAL_WAGERED), juice_adjusted = FALSE), 2)
  })
  
  spread_effective_win_percentage_juice_adjusted <- reactive({
    round(effective_win_rate(sum(dt_spread_profit_by_date()$TOTAL_RETURN),sum(dt_spread_profit_by_date()$TOTAL_WAGERED), juice_adjusted = TRUE), 2)
  })
  
  output$return_rate <- renderText(return_percentage())
  output$win_rate <- renderText(effective_win_percentage())
  output$juice_adj_win_rate <- renderText(effective_win_percentage_juice_adjusted())
  
  output$spread_return_rate <- renderText(spread_return_percentage())
  output$spread_win_rate <- renderText(spread_effective_win_percentage())
  output$spread_juice_adj_win_rate <- renderText(spread_effective_win_percentage_juice_adjusted())
  
  
  # Profit loss plot
  profit_loss_fig <- reactive({
    plot_ly(data = dt_profit_by_date()) |>
      add_lines(x = ~WEEK, y = ~cumsum(PROFIT), name = 'Profit/Loss', 
                line = list(shape = 'spline', smoothing = 0.5)) |> 
      add_lines(x = ~ WEEK, y = ~cumsum(TOTAL_WAGERED*0.0476*-1), name = 'Juice Tax',
                line = list(shape = 'spline', smoothing = 0.5)) |> 
      add_bars(x = ~WEEK, y = ~PROFIT, 
               color = ~ifelse(PROFIT > 0, 'Daily Profit', 'Daily Loss'),
               hoverinfo = "text",
               hovertext = ~paste0('<b>Profit:    </b>', round(PROFIT,2),"<br>",
                                   '<b>N Bets:    </b>', N_BETS,"<br>",
                                   '<b>N Winners: </b>', N_WINNERS,"<br>",
                                   '<b>Wagered: </b>', round(TOTAL_WAGERED, 2),"<br>",
                                   '<b>Returned:   </b>', round(TOTAL_RETURN, 2))
               ) |>
      layout(
        title = list(text = 'Cumulative Profit Loss Over Time',
                     y = 0.97),
        xaxis = list(title = 'Date'),
        yaxis = list(title = 'Dollars ($)')
      )
  })
  
  output$profit_loss <- renderPlotly(profit_loss_fig())
  
  # Profit loss plot
  spread_profit_loss_fig <- reactive({
    plot_ly(data = dt_spread_profit_by_date()) |>
      add_lines(x = ~WEEK, y = ~cumsum(PROFIT), name = 'Profit/Loss', 
                line = list(shape = 'spline', smoothing = 0.5)) |> 
      add_lines(x = ~ WEEK, y = ~cumsum(TOTAL_WAGERED*0.0476*-1), name = 'Juice Tax',
                line = list(shape = 'spline', smoothing = 0.5)) |> 
      add_bars(x = ~WEEK, y = ~PROFIT, 
               color = ~ifelse(PROFIT > 0, 'Daily Profit', 'Daily Loss'),
               hoverinfo = "text",
               hovertext = ~paste0('<b>Profit:    </b>', round(PROFIT,2),"<br>",
                                   '<b>N Bets:    </b>', N_BETS,"<br>",
                                   '<b>N Winners: </b>', N_WINNERS,"<br>",
                                   '<b>Wagered: </b>', round(TOTAL_WAGERED, 2),"<br>",
                                   '<b>Returned:   </b>', round(TOTAL_RETURN, 2))
               ) |>
      layout(
        title = list(text = 'Cumulative Profit Loss Over Time',
                     y = 0.97),
        xaxis = list(title = 'Date'),
        yaxis = list(title = 'Dollars ($)')
      )
  })
  
  output$spread_profit_loss <- renderPlotly(spread_profit_loss_fig())
  
  # # Max SPREAD table
  # output$spread_table <- DT::renderDT(DT::datatable(dt_max_spread(),
  #                                                   rownames = FALSE))
  
  # Odds breakdown plots
  odds_plot <- reactive({
    plot_ev(games_dt = dt_history()[!is.na(WINNER) & LINE_MOVEMENT_TIME <= 0, .SD[which.max(KC)], by = .(GAME_ID, Team)],
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
  
  # # Line movement plots
  # line_movement_fig <- reactive({
  #   dt_history()[KC > 0] |>
  #     group_by(as.factor(interaction(GAME_ID, Team))) |>
  #     plot_ly(x = ~LINE_MOVEMENT_TIME, y = ~KC, color = ~as.factor(WINNER)) |>
  #     add_lines() |>
  #     layout(
  #       title = list(text = 'Line Movement Before Gametime',
  #                    y = 0.97),
  #       xaxis = list(title = 'Hours Before Gametime'),
  #       yaxis = list(title = 'Kelly Criterion'),
  #       legend = list(
  #         title = list(text = "Winner"),
  #         x = 1,  # Adjust the x position of the legend
  #         y = 0.5   # Adjust the y position of the legend
  #       )
  #     )
  # })

  # Narrow games table 
  output$narrow_games_table <- DT::renderDT(DT::datatable(dt_history()[is.na(WINNER),
                                                                       .(Game = unique(GAME))], 
                                                          rownames = FALSE, selection = "single",
                                                          options = list(pageLength = 30,
                                                                         dom = "t")))
  
  selected_game <- reactive({dt_history()[is.na(WINNER),
                                          .(Game = unique(GAME))
                                          ][input$narrow_games_table_row_last_clicked]$Game})
  
  # Line movement plots
  line_movement_fig <- reactive({
    # dt_history()[is.na(WINNER)] |>
    dt_history()[is.na(WINNER) & GAME %in% selected_game()][order(Team)] |>
      group_by(as.factor(interaction(GAME_ID, Team))) |>
      plot_ly() |>
      add_trace(x = ~LINE_MOVEMENT_TIME/24, y = ~KC, split = ~AB,
                type = "scatter", mode = "lines+markers", # colors = ~COLOUR,
                marker = list(color = ~COLOUR, colors = ~COLOUR),
                line = list(shape = 'hv', width = 3,
                            color = ~COLOUR, colors = ~COLOUR),
                hoverinfo = "text",
                hovertext = ~paste0('<b>Team: </b>', Team,"<br>",
                                    '<b>KC:   </b>', round(KC, 2),"<br>",
                                    '<b>Line: </b>', round(Price, 2),"<br>",
                                    '<b>Win Prob.:   </b>', round(WIN_PROB, 2),"<br>",
                                    '<b>DYS TO KO: </b>', round(-1*LINE_MOVEMENT_TIME/24, 0)),
                textposition = 'outside', opacity = 0.75, name = ~paste(AB, "KC"), 
                visible = TRUE) |>
      add_trace(x = ~LINE_MOVEMENT_TIME/24, y = ~EV, split = ~AB,
                type = "scatter", mode = "lines+markers", # colors = ~COLOUR,
                marker = list(color = ~COLOUR, colors = ~COLOUR),
                line = list(shape = 'hv', width = 3,
                            color = ~COLOUR, colors = ~COLOUR),
                hoverinfo = "text",
                hovertext = ~paste0('<b>Team: </b>', Team,"<br>",
                                    '<b>EV:   </b>', round(EV, 2),"<br>",
                                    '<b>Line: </b>', round(Price, 2),"<br>",
                                    '<b>Win Prob.:   </b>', round(WIN_PROB, 2),"<br>",
                                    '<b>DYS TO KO: </b>', round(-1*LINE_MOVEMENT_TIME/24, 0)),
                textposition = 'outside', opacity = 0.75, yaxis = "y3", name = ~paste(AB, "EV"), 
                visible = TRUE) |>
      add_trace(x = ~LINE_MOVEMENT_TIME/24, y = ~Price, split = ~Team,
                type = "scatter", mode = "lines+markers",
                marker = list(color = ~COLOUR, colors = ~COLOUR),
                line = list(shape = 'hv', width = 3,
                            color = ~COLOUR, colors = ~COLOUR),
                hoverinfo = "text",
                hovertext = ~paste0('<b>Team: </b>', Team,"<br>",
                                    '<b>Line: </b>', round(Price, 2),"<br>",
                                    '<b>DYS TO KO: </b>', round(-1*LINE_MOVEMENT_TIME/24, 0)),
                textposition = 'outside', opacity = 0.75, yaxis = "y2", name = ~paste(AB, "Line"),
                visible = FALSE) |>
      add_trace(x = ~LINE_MOVEMENT_TIME/24, y = ~WIN_PROB,
                type = "scatter", mode = "lines+markers", split = ~Team,
                marker = list(color = ~COLOUR, colors = ~COLOUR),
                line = list(shape = 'hv', width = 3,
                            color = ~COLOUR, colors = ~COLOUR),
                hoverinfo = "text",
                hovertext = ~paste0('<b>Team: </b>', Team,"<br>",
                                    '<b>Win Prob.:   </b>', round(WIN_PROB, 2),"<br>",
                                    '<b>DYS TO KO: </b>', round(-1*LINE_MOVEMENT_TIME/24, 0)),
                textposition = 'outside', opacity = 0.75, yaxis = "y3", name = ~paste(AB, "Win Prob."),
                visible = FALSE) |>
      # add_lines(line = list(shape = 'hv', colors = ~paste0("#", COLOUR)),
      #           hovertext = ~paste0('<b>Team: </b>', Team,"<br>",
      #                               '<b>KC:   </b>', round(KC, 2),"<br>",
      #                               '<b>DYS TO KO: </b>', round(-1*LINE_MOVEMENT_TIME/24, 0)), textposition = 'outside',
      #           opacity = 0.75) |>
      # add_lines(line = list(shape = 'hv', colors = ~paste0("#", COLOUR)),
      #           hovertext = ~paste0('<b>Team: </b>', Team,"<br>",
      #                               '<b>KC:   </b>', round(KC, 2),"<br>",
      #                               '<b>DYS TO KO: </b>', round(-1*LINE_MOVEMENT_TIME/24, 0)), textposition = 'outside',
      #           opacity = 0.75) |>
      layout(
        title = list(text = 'Line Movement Before Gametime',
                     y = 0.97),
        xaxis = list(title = 'Days Before Game',
                     range = c(min(dt_history()[is.na(WINNER)]$LINE_MOVEMENT_TIME/24), 0),
                     zerolinecolor = "#CCCCCC",
                     zerolinewidth = 3),
        yaxis = list(title = 'Kelly Criterion',
                     range = c(-0.3, 0.3),
                     zerolinecolor = "#CCCCCC",
                     zerolinewidth = 3),
        yaxis2 = list(title = '',
                      overlaying = "y",
                      side = "left",
                      showline=F,showticklabels=F,
                      zerolinecolor = "#CCCCCC",
                      zerolinewidth = 0,
                      showgrid = F),
                      # range = c(-0.3, 0.3)),
        yaxis3 = list(title = 'Expected Value',
                      range = c(-20, 20),
                      overlaying = "y",
                      side = "right",
                      showline=T,showticklabels=T,
                      zerolinecolor = "#CCCCCC",
                      zerolinewidth = 3,
                      showgrid = T),
        legend = list(
          title = list(text = "Team"),
          x = 0,  # Adjust the x position of the legend
          y = 0   # Adjust the y position of the legend
        ),
        margin = list(
          # t = 50,
          # b = 50,
          # l = 50,
          r = 50
        ),
        updatemenus = list(
          list(
            active = 1,
            type = "buttons",
            buttons = list(
              list(
                label = "All",
                method = "update", 
                args = list(
                  list(visible = rep(TRUE, 8)),
                  list(yaxis = list(showline=T,showticklabels=T,
                                    title = 'Kelly Criterion',
                                    range = c(-0.3, 0.3),
                                    zerolinecolor = "#CCCCCC",
                                    zerolinewidth = 3),
                       yaxis2 = list(showline=T,showticklabels=T,
                                     title = 'Line',
                                     overlaying = "y",
                                     side = "left"),
                       yaxis3 = list(showline=T,showticklabels=T,
                                     title = 'Win Probability',
                                     range = c(-0.3, 100),
                                     overlaying = "y",
                                     side = "right")
                  )
                )
              ),
              list(
                label = "Kelly Crit.",
                method = "update",
                args = list(
                  list(visible = c(rep(TRUE, 4), rep(FALSE, 4))),
                  list(yaxis = list(title = 'Kelly Criterion',
                                    range = c(-0.3, 0.3),
                                    zerolinecolor = "#CCCCCC",
                                    zerolinewidth = 3),
                       yaxis2 = list(title = '',
                                     overlaying = "y",
                                     side = "left",
                                     showline=F,showticklabels=F,
                                     zerolinecolor = "#FFF",
                                     zerolinewidth = 0,
                                     showgrid = F),
                       # range = c(-0.3, 0.3)),
                       yaxis3 = list(title = 'Expected Value',
                                     range = c(-20, 20),
                                     overlaying = "y",
                                     side = "right",
                                     showline=T,showticklabels=T,
                                     zerolinecolor = "#CCCCCC",
                                     zerolinewidth = 3,
                                     showgrid = T)
                  )
                )
              ),
              list(label = "Line & Win %",
                   method = "update",
                   args = list(
                     list(visible = c(rep(FALSE, 4), rep(TRUE, 4))),
                     list(yaxis = list(showline=F,showticklabels=F,title = '',
                                       showgrid = F),
                          yaxis2 = list(showline=T,showticklabels=T,
                                        title = 'Line',
                                        overlaying = "y",
                                        side = "left"),
                          yaxis3 = list(showline=T,showticklabels=T,
                                        title = 'Win Probability',
                                        range = c(-0.3, 100),
                                        overlaying = "y",
                                        side = "right")
                     )
                   )
              )
            ),
            direction = "down",
            x = 0.1,
            y = 1
          )
        )
      )
  })
  
  output$line_movement_plot <- renderPlotly(line_movement_fig())
  
  # Results table
  output$results_table <- DT::renderDT(DT::datatable(dt_history()[!is.na(WINNER),.(GAME, WEEK, Team, WINNER,
                                                                                   BET = ifelse(KC > 0, yes = TRUE, no = FALSE),
                                                                                   PROFIT = round(sum(ifelse(WINNER, (KC*Price*100), 0)) - sum(KC*100), 2),
                                                                                   WIN_PROB, Price, EV, KC, Book),
                                                                  by = .I], 
                                                     rownames = FALSE))
  
  # Max KC table
  output$max_kc_table <- DT::renderDT(DT::datatable(dt_max_kc()[!is.na(WINNER),.(GAME, WEEK, Team, WINNER, 
                                                                                 BET = ifelse(KC > 0, yes = TRUE, no = FALSE),
                                                                                 PROFIT = round(sum(ifelse(WINNER, (KC*Price*100), 0)) - sum(KC*100), 2),
                                                                                 WIN_PROB, Price, EV, KC, Book),
                                                                by = .I], 
                                                     rownames = FALSE))
  
  # Upcoming games table
  dt_upcoming_games <- reactive({
    dt_history()[is.na(WINNER), .(GAME, WEEK, Team, WIN_PROB, Price, EV, KC, MIN_PRICE, Book, LINE_MOVEMENT_TIME)]
  })
  
  output$upcoming_games_table <- DT::renderDT(DT::datatable(dt_upcoming_games(), 
                                                            rownames = FALSE
  ) |> 
    formatRound(columns = sapply(dt_upcoming_games(), is.numeric), digits = 3) )
  
  # Observe when the quit button is pressed
  observeEvent(input$quit_btn, {
    stopApp()  # Stop the app when the button is clicked
  })
  
}

shinyApp(ui, server)