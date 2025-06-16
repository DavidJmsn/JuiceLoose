library(here)
library(withr)
withr::local_dir(here::here("R","nhl_ev_retrieval"))
library(testthat)
library(webmockr)
httr_mock()
source(here::here("R","nhl_ev_retrieval","utils","api_client.R"))

webmockr::enable()

test_that('api_get uses caching', {
  stub_request('get', 'https://api-web.nhle.com/test') %>%
    to_return(body = '{"ok":true}', status = 200)

  client <- create_nhl_api_client(use_cache = TRUE)
  res1 <- client$get('/test')
  res2 <- client$get('/test')

  expect_equal(res1$ok, TRUE)
  expect_equal(res2$ok, TRUE)
  expect_equal(length(request_registry()$request_signatures$hash), 1)
})

webmockr::disable()
