
# FUNCTIONS FOR EV RESULTS SHINY DASHBOARD --------------------------------

get_line <- function(EV, winProb){
  line = ((EV+100)/winProb)
  return(line)
}

get_prob <- function(EV = 0, line){
  prob <- ((EV+100)/line)
  return(prob)
}

return_rate <- function(total_returned, total_wagered){
  return_rate = total_returned/total_wagered
  return(return_rate)
}

effective_win_rate <- function(total_returned, total_wagered, juice_adjusted = FALSE){
  return_rate = total_returned/total_wagered
  if(juice_adjusted){
    return_rate = return_rate/0.9545455
  }
  win_rate = return_rate*100/2
  return(win_rate)
}

plot_ev <- function(games_dt = dt_history, title = 'Odds vs Win Probability'){
  ylims = c(games_dt[,min(Price)]-0.25,
            games_dt[,max(Price)]+0.25)
  xlims = c(games_dt[,min(WIN_PROB)]-5,
            games_dt[,max(WIN_PROB)]+5)
  
  z <- data.table(x=c(0,105))
  p <- ggplot(data = z, mapping = aes(x=x)) +
    # stat_function(data = data.frame(x=c(0, 100)), mapping = aes(x),fun=function(x) get_line(EV = 0, winProb = x)) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x)) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x),
                  geom = 'ribbon',
                  mapping = aes(ymin=after_stat((get_line(EV = -5, winProb = x))),
                                ymax=after_stat((get_line(EV = 0, winProb = x)))),
                  fill = 'red',
                  alpha = 0.3) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x),
                  geom = 'ribbon',
                  mapping = aes(ymin=after_stat((get_line(EV = 0, winProb = x))),
                                ymax=after_stat((get_line(EV = 5, winProb = x)))),
                  fill = 'orange',
                  alpha = 0.2) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x),
                  geom = 'ribbon',
                  mapping = aes(ymin=after_stat((get_line(EV = 5, winProb = x))),
                                ymax=after_stat((get_line(EV = 10, winProb = x)))),
                  fill = 'yellow',
                  alpha = 0.2) +
    stat_function(fun=function(x) get_line(EV = 0, winProb = x),
                  geom = 'ribbon',
                  mapping = aes(ymin=after_stat((get_line(EV = 10, winProb = x))),
                                ymax=after_stat((get_line(EV = 10000, winProb = x)))),
                  fill = 'green',
                  alpha = 0.2) +
    stat_function(fun=function(x) get_line(EV = -5, winProb = x),
                  geom = 'area',
                  fill = 'red',
                  alpha = 0.4)
  
  p <- p + geom_point(data = games_dt,
                      mapping = aes(x = WIN_PROB,
                                    y = Price,
                                    color = WINNER
                                    # shape = Team
                      )) +
    labs(x = 'Win Probability', y = 'Line',
         title = title
    ) +
    coord_cartesian(xlim = xlims,
                    ylim = ylims) +
    scale_color_discrete(name = "WINNER") +
    scale_x_continuous(breaks = seq(0, 100, by = 10)) +
    scale_y_continuous(breaks = seq(floor(min(games_dt$Price)), ceiling(max(games_dt$Price)), by = 0.5)) +
    # scale_shape_discrete(name = "Team") +
    theme(plot.title = element_text(hjust = 0.5))
  
  return(ggplotly(p))
}
