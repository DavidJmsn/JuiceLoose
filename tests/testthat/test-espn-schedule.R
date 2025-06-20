library(here)
library(withr)
withr::local_dir(here::here())
library(testthat)
library(jsonlite)
source(here("R","espn_ev_retrieval","collectors","schedule.R"))

# load sample data
events <- fromJSON("tests/testthat/testdata/espn_events.json", simplifyDataFrame = TRUE)
event_data <- fromJSON("tests/testthat/testdata/espn_event.json", simplifyDataFrame = TRUE)

mock_api <- function(endpoint, api_type="espn", query=list(), use_cache=TRUE) {
  event_data
}

with_mock(api_get = mock_api, {
  res <- parse_espn_events(events, "NBA")
  test_that('parse_espn_events extracts teams and probabilities', {
    expect_equal(nrow(res), 1)
    expect_equal(res$home_team, 'NEW YORK KNICKS')
    expect_equal(res$away_team, 'BOSTON CELTICS')
    expect_equal(res$home_win_prob, 0.60)
    expect_equal(res$away_win_prob, 0.40)
  })
})
