library(here)
library(withr)
withr::local_dir(here::here())
library(testthat)

skip_if_not_installed('anytime')
source(here::here('R','nhl_ev_retrieval','collectors','goalies.R'), local = TRUE)
html_text <- readLines(here::here("tests","testthat","testdata","goalies_sample.html"))

test_that('extract_goalie_data parses goalies', {
  dt <- extract_goalie_data(paste(html_text, collapse='\n'))
  expect_equal(nrow(dt), 2)
  expect_true(all(c('BOSTON BRUINS','NEW YORK RANGERS') %in% dt$team))
})
