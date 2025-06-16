# JuiceLoose

JuiceLoose provides scripts to collect NHL betting data and a Shiny dashboard
for exploring expected value metrics. Data can be stored locally or pushed to
Google BigQuery for use on Posit Connect.

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
2. Retrieve data using the scripts in `R/nhl_ev_retrieval`.
3. Launch the dashboard with `Rscript app.R` or run the file inside an
   interactive R session.

Additional scripts for downloading team metadata are documented in
`R/update_team_metadata/README.md`.

