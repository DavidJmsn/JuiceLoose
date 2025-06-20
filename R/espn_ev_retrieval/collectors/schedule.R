# ESPN SCHEDULE COLLECTOR --------------------------------------------------
# Purpose: collect schedule and win probabilities for various sports
# Author: OpenAI ChatGPT
# Last Updated: 2025-06-20

suppressPackageStartupMessages({
  library(data.table)
  library(here)
})

source(here("R", "espn_ev_retrieval", "utils", "common.R"))
source(here("R", "espn_ev_retrieval", "utils", "api_client.R"))

# Basic sport mapping ------------------------------------------------------

sport_map <- list(
  NBA = list(sport = "basketball", league = "nba"),
  NFL = list(sport = "football", league = "nfl"),
  MLB = list(sport = "baseball", league = "mlb")
)

# Helper to build events endpoint
build_events_endpoint <- function(date, sport_info) {
  date_str <- format(as.Date(date), "%Y%m%d")
  sprintf("/%s/leagues/%s/events?dates=%s&lang=en&region=us",
          sport_info$sport, sport_info$league, date_str)
}

# Parse events JSON into data.table
parse_espn_events <- function(events, sport) {
  if (is.null(events) || length(events$items) == 0) return(data.table())

  games <- list()
  for (ev in events$items) {
    ev_data <- api_get(ev$`$ref`, "espn")
    if (is.null(ev_data)) next

    comp <- ev_data$competitions[[1]]
    if (is.null(comp$competitors) || length(comp$competitors) < 2) next

    away <- comp$competitors[[1]]
    home <- comp$competitors[[2]]
    if (home$homeAway == "away") {
      tmp <- home; home <- away; away <- tmp
    }

    game_dt <- data.table(
      game_id = ev_data$id,
      sport = sport,
      game_date = as.Date(ev_data$date),
      game_time = format(as.POSIXct(ev_data$date, tz = "UTC"), "%H:%M", tz = "America/New_York"),
      home_team = standardize_team_name(home$team$displayName),
      away_team = standardize_team_name(away$team$displayName),
      home_win_prob = NA_real_,
      away_win_prob = NA_real_
    )

    if (!is.null(ev_data$predictor$homeTeam$gameProjection)) {
      game_dt$home_win_prob <- ev_data$predictor$homeTeam$gameProjection/100
    }
    if (!is.null(ev_data$predictor$awayTeam$gameProjection)) {
      game_dt$away_win_prob <- ev_data$predictor$awayTeam$gameProjection/100
    }

    # sport specific extras (placeholder fields)
    if (!is.null(home$probables)) {
      game_dt$home_starter <- home$probables[[1]]$athlete$displayName
    }
    if (!is.null(away$probables)) {
      game_dt$away_starter <- away$probables[[1]]$athlete$displayName
    }

    games[[length(games)+1]] <- game_dt
  }

  rbindlist(games, fill = TRUE)
}

# Main function ------------------------------------------------------------

collect_espn_schedule <- function(dates, sport = "NBA") {
  sport <- toupper(sport)
  if (!sport %in% names(sport_map)) {
    stop("Unsupported sport: ", sport)
  }

  dates <- parse_date_args(dates)
  sport_info <- sport_map[[sport]]

  out <- list()
  for (d in dates) {
    endpoint <- build_events_endpoint(d, sport_info)
    events <- api_get(endpoint, "espn")
    if (is.null(events)) next
    out[[length(out)+1]] <- parse_espn_events(events, sport)
  }

  if (length(out) == 0) return(data.table())
  res <- rbindlist(out, fill = TRUE)
  setorder(res, game_date, game_time)
  res
}

