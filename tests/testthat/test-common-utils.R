library(here)
library(withr)
withr::local_dir(here::here())
library(testthat)
library(withr)
library(data.table)
source(here::here("R","nhl_ev_retrieval","utils","common.R"))


test_that('standardize_team_name works', {
  expect_equal(standardize_team_name(c('Utah Flames.', 'New York Rangers')), c('FLAMES', 'NEW YORK RANGERS'))
  expect_equal(standardize_team_name(NULL), character(0))
})


test_that('parse_date_args handles defaults and errors', {
  withr::with_options(list(warn = 2), {
    dates <- parse_date_args(c('2025-01-01', '2025-01-02'))
    expect_equal(dates, as.Date(c('2025-01-01','2025-01-02')))
  })
  expect_error(parse_date_args('bad-date'))
})


test_that('calculate_ev and kelly return expected values', {
  expect_equal(calculate_ev(2, 0.5), 0)
  expect_true(is.na(calculate_ev(-1, 0.5)))
  expect_equal(round(calculate_kelly(2,0.5),3), 0)
  expect_true(is.na(calculate_kelly(1,0.5)))
})


test_that('validate_columns and data_quality log appropriately', {
  dt <- data.table(a=1:3,b=c(1,NA,3))
  expect_true(validate_columns(dt,c('a','b')))
  expect_false(validate_columns(dt,c('a','b','c')))
  expect_true(validate_data_quality(dt))
})
