# ESPN EV RETRIEVAL MASTER SCRIPT ----------------------------------------
# Purpose: orchestrate collection of ESPN schedule, odds and compute EV
# Author: OpenAI ChatGPT
# Last Updated: 2025-06-20

suppressPackageStartupMessages({
  library(data.table)
  library(here)
})

source(here("R","espn_ev_retrieval","utils","common.R"))
source(here("R","espn_ev_retrieval","utils","api_client.R"))
source(here("R","espn_ev_retrieval","collectors","schedule.R"))
source(here("R","espn_ev_retrieval","collectors","odds.R"))

retrieve_espn_expected_values <- function(dates = NULL, sport = "NBA", save_file = TRUE) {
  init_logging("espn_ev")
  check_delay_arg()
  ensure_directories()

  dates <- parse_date_args(dates)
  log_message(sprintf("=== ESPN EV Retrieval for %s (%d dates) ===", sport, length(dates)), "INFO")

  sched <- with_retry(collect_espn_schedule, dates = dates, sport = sport, context = "Schedule")
  if (is.null(sched) || nrow(sched) == 0) {
    log_message("No schedule data retrieved", "ERROR")
    return(invisible(data.table()))
  }

  odds <- with_retry(collect_espn_odds, dates = dates, sport = sport, context = "Odds")
  if (is.null(odds) || nrow(odds) == 0) {
    log_message("No odds data retrieved", "ERROR")
    return(invisible(data.table()))
  }

  win_dt <- rbind(
    sched[!is.na(home_win_prob), .(game_id, game_date, game_time,
                                   team = home_team, opponent = away_team,
                                   win_probability = home_win_prob,
                                   home_or_away = "home",
                                   home_starter, away_starter)],
    sched[!is.na(away_win_prob), .(game_id, game_date, game_time,
                                   team = away_team, opponent = home_team,
                                   win_probability = away_win_prob,
                                   home_or_away = "away",
                                   home_starter, away_starter)]
  )

  merged <- merge(win_dt, odds,
                  by.x = c("game_date", "game_time", "team"),
                  by.y = c("game_date", "game_time", "name"),
                  all.x = TRUE)

  merged[, `:=`(
    expected_value = calculate_ev(price, win_probability),
    kelly_criterion = calculate_kelly(price, win_probability),
    retrieval_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "America/New_York")
  )]

  if (save_file) {
    out_file <- file.path(espn_config$data_dir, "expected_value",
                          sprintf("%s_%s_ev.csv",
                                  format(Sys.time(), "%Y%m%d_%H%M%S"), tolower(sport)))
    fwrite(merged, out_file)
    log_message(sprintf("Saved %d rows to %s", nrow(merged), out_file), "SUCCESS")
  }

  log_message("ESPN EV retrieval completed", "SUCCESS")
  invisible(merged)
}

if (!interactive()) {
  retrieve_espn_expected_values()
}

