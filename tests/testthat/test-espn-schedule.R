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
    expect_equal(res$home_team, 'KNICKS')
    expect_equal(res$away_team, 'CELTICS')
    expect_equal(res$home_win_prob, 0.60)
    expect_equal(res$away_win_prob, 0.40)
  })
})

# competitors wrapped in an extra list
wrapped_event <- event_data
wrapped_event$competitions$competitors[[1]] <- list(as.data.frame(wrapped_event$competitions$competitors[[1]]))

mock_api2 <- function(endpoint, api_type="espn", query=list(), use_cache=TRUE) {
  wrapped_event
}

with_mock(api_get = mock_api2, {
  res <- parse_espn_events(events, "NBA")
  test_that('parse_espn_events handles wrapped competitors', {
    expect_equal(nrow(res), 1)
    expect_equal(res$home_team, 'KNICKS')
    expect_equal(res$away_team, 'CELTICS')
  })
})

# teams referenced only by $ref
events_ref <- fromJSON("tests/testthat/testdata/espn_events_teamref.json", simplifyDataFrame = TRUE)
event_ref_data <- fromJSON("tests/testthat/testdata/espn_event_teamref.json", simplifyDataFrame = TRUE)

mock_api3 <- function(endpoint, api_type="espn", query=list(), use_cache=TRUE) {
  if (endpoint == "espn_event_teamref.json") {
    event_ref_data
  } else if (endpoint == "home_team.json") {
    list(displayName = "New York Knicks")
  } else if (endpoint == "away_team.json") {
    list(displayName = "Boston Celtics")
  } else {
    event_data
  }
}

with_mock(api_get = mock_api3, {
  res <- parse_espn_events(events_ref, "NBA")
  test_that('parse_espn_events resolves team refs', {
    expect_equal(nrow(res), 1)
    expect_equal(res$home_team, 'KNICKS')
    expect_equal(res$away_team, 'CELTICS')
  })
})
