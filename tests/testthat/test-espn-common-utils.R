library(here)
library(withr)
withr::local_dir(here::here())
library(testthat)
source(here("R","espn_ev_retrieval","utils","common.R"))

test_that('espn standardize_team_name works', {
  expect_equal(standardize_team_name(c('New York Knicks.', 'L.A. Lakers')), c('NEW YORK KNICKS','LA LAKERS'))
})

test_that('espn ev helpers work', {
  expect_equal(calculate_ev(2,0.5), 0)
  expect_true(is.na(calculate_ev(-1,0.5)))
  expect_equal(round(calculate_kelly(2,0.5),3),0)
})
