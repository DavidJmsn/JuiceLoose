# JuiceLoose

JuiceLoose provides scripts to collect betting data from the NHL API and ESPN,
along with a Shiny dashboard for exploring expected value metrics. Data can be
stored locally or pushed to Google BigQuery for use on Posit Connect.

## Requirements
- R 4.0 or later
- Google BigQuery credentials
- Environment variables:
  - `PROJECT_ID` – GCP project containing the BigQuery dataset
  - `DATASET` – target BigQuery dataset name
  - `SERVICE_ACCOUNT` – path to a service account JSON file
  - `SERVICE_ACCOUNT_JSON` – base64 encoded credentials for cloud deployments (optional)
  - `ODDS_API_KEY` – API key for the-odds-api.com
  - `PLATFORM` – set to `posit_connect_cloud` when deploying on Posit Connect

## Usage
1. Install R package dependencies listed in `manifest.json`.
2. Retrieve data using the scripts in `R/nhl_ev_retrieval` and
   `R/espn_ev_retrieval`. These scripts fetch schedule and odds data from ESPN and compute expected value metrics.
3. Launch the dashboard with `Rscript app.R` or run the file inside an
   interactive R session.

Additional scripts for downloading team metadata are documented in
`R/update_team_metadata/README.md`.

### ESPN schedule

`collect_espn_schedule()` now handles competitor entries where the team
object only includes a `$ref` link. The collector fetches the referenced
team resource so that names are parsed correctly in the results.

## API quota usage

`APIClient$get` logs the remaining quota information returned by the NHL API.
If the response headers include `X-Requests-Remaining`, `X-Requests-Used`, or
`X-Requests-Last`, the values are printed at the INFO level. These headers are
also attached to the parsed response as `attr(res, "response_headers")` so that
callers can implement their own rate limiting or monitoring.

