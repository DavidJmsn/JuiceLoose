library(here)
library(withr)
withr::local_dir(here::here())
library(testthat)
library(data.table)


test_that('calc_line_from_ev works', {
  skip_if_not_installed('bsicons')
  source(here::here('app.R'), local = TRUE)
  expect_equal(calc_line_from_ev(0.05, 0.5), 2.1)
  expect_true(is.na(calc_line_from_ev(0.05, 0)))
})


test_that('extract_team_colors returns data.frame', {
  skip_if_not_installed('bsicons')
  source(here::here('app.R'), local = TRUE)
  logos <- list(teams = list(TEST_TEAM = list(colors = list(primary_color = '#FF0000'))))
  df <- extract_team_colors(logos)
  expect_equal(df$team, 'TEST TEAM')
  expect_equal(df$color_hex, '#FF0000')
})
