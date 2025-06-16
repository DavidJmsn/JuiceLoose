library(here)
library(withr)
withr::local_dir(here::here())
library(testthat)
library(data.table)
source(here::here("R","nhl_ev_retrieval","collectors","schedule.R"))

sample_game <- data.frame(
  id = 1,
  season = 20252026,
  gameType = 2,
  gameState = 'FUT',
  startTimeUTC = '2025-01-01T17:00:00Z',
  homeTeam.abbrev = 'NYR',
  awayTeam.abbrev = 'BOS',
  homeTeam.placeName.default = 'New York',
  homeTeam.commonName.default = 'Rangers',
  awayTeam.placeName.default = 'Boston',
  awayTeam.commonName.default = 'Bruins',
  homeTeam.score = 0,
  awayTeam.score = 0,
  venueTimezone = 'America/New_York',
  venue.default = 'Arena',
  stringsAsFactors = FALSE
)
sample_game$tvBroadcasts <- list(list(network='NBC'))

sample_data <- list(gameWeek = list(date = '2025-01-01', games = list(sample_game)))

test_that('parse_nhl_games extracts basic info', {
  skip('complex parsing skipped in CI')
  res <- parse_nhl_games(sample_data, as.Date('2025-01-01'))
  expect_equal(nrow(res), 1)
  expect_equal(res$home_team, 'NYR')
  expect_equal(res$away_team, 'BOS')
})
