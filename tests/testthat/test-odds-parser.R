library(here)
library(withr)
withr::local_dir(here::here())
library(testthat)
library(jsonlite)
library(data.table)
source(here::here("R","nhl_ev_retrieval","collectors","odds.R"))

raw_data <- fromJSON("tests/testthat/testdata/odds_sample.json", simplifyDataFrame = TRUE)

test_that('parse_odds_efficient flattens odds', {
  res <- parse_odds_efficient(raw_data)
  expect_gt(nrow(res), 0)
  expect_true('price' %in% names(res))
  expect_equal(unique(res$market), 'h2h')
})
