---
editor_options: 
  markdown: 
    wrap: 72
---

Overview

This repository contains a master R script (master.R) that orchestrates
the retrieval and processing of NHL game, odds, win‐probability, and
starting goalie data, then computes expected values and Kelly Criterion
metrics for head‐to‐head (h2h) betting. It’s intended to run on a daily
(or ad‐hoc) basis, processing “today” and “tomorrow” by default or using
user‐supplied dates in YYYY‐MM‐DD format.

```         
Note: This isn’t a polished “plug‐and‐play” tool. You need to review environment variables, confirm that dependent scripts exist and function, and understand how each component behaves under failure. If you blindly run it without verifying prerequisites, you’ll quickly discover where things break.
```

Table of Contents

```         
Features

Prerequisites

Installation & Setup

Configuration

Usage

Logging

Directory Structure

Function Reference

Error Handling & Pitfalls

Future Considerations

License & Contact
```

Features

```         
Master Orchestration

    Calls four separate R scripts (schedule, win‐probability, odds, starting goalie) with retry logic.

    Logs each step’s success or failure and can halt on vitally missing data.

Data Retrieval with Retry

    Retries up to max_retries times (default: 3) with retry_delay (default: 5 seconds) before giving up on vital scripts.

    Non‐vital retrieval (starting goalie) falls back to dummy data if unavailable.

Expected Value & Kelly Criterion Calculation

    Converts decimal odds and win‐probability percentages to expected value (EV) and Kelly Criterion (K).

    Computes implied lines and implied win‐percentages for reference.

Intermediate & Final Reporting

    Saves intermediate text reports (schedule, win‐prob, odds, goalies) if save_intermediate = TRUE.

    Generates a final summary report listing total games, positive‐EV bets, and key metrics.

File Output

    Writes a timestamped CSV of final data to output_dir.

    Creates logs of run details and timestamped summary files.

Configurable Behavior

    Users can override default “today & tomorrow” dates via command‐line arguments or by passing manual_dates to the main function.

    Optionally email final CSV if email_reports = TRUE (placeholder logic; must implement mailing utility).
```

Prerequisites

```         
R (>= 4.0)

Required R packages (all loaded at startup with suppressPackageStartupMessages({ … })):

    dplyr

    tibble

    stringr

    lubridate

    readr

    tidyr

    Be skeptical: Just because these are listed doesn’t guarantee they’re installed or the correct versions. You’d best run install.packages() for each or use a lockfile mechanism (e.g., renv) to ensure reproducibility.

Operating System

    Any OS that can run R and shell commands. The script uses Sys.getenv("HOME") to construct paths, so it presumes a Unix‐like environment.

Environment Variables

    HOME: expected to be set (R typically inherits it).

    Make sure your home directory actually contains the subdirectories and scripts referenced (see “Directory Structure” below).

Dependent Scripts (see Section 4: Configuration)

    01_NHL_retrieve_schedule.R

    02_NHL_win_probability_retrieval.R

    03_NHL_odds_retrieval.R

    04_starting_goalie_retrieval.R

Each of these scripts must define a single retrieval function, respectively:

    retrieve_nhl_schedule(dates) → data.frame with columns at least home, away, game_date, start_time_local

    retrieve_win_probabilities(dates) → data.frame with columns home, away, team, win_probability (e.g., “47.6%”)

    retrieve_nhl_odds(dates) → data.frame with columns market, home_team, away_team, name, price, point, book

    retrieve_starting_goalies(dates) → data.frame with columns team, away, home, goalie, status, status_updated

    Warning: If any of these scripts change interfaces (column names, return types), the master script will fail. Validate each script’s output format before integrating.
```

Installation & Setup

```         
Clone or Download this repository to your machine.

Verify directory layout (the defaults assume a certain folder structure; adjust if yours differ—see next section).

Install required R packages:
```

R -e
"install.packages(c('dplyr','tibble','stringr','lubridate','readr','tidyr'))"

Ensure dependent scripts exist at the paths used by master_config (or
modify master_config to point to their actual locations).

Make the master script executable (optional):

```         
chmod +x master.R

(Optional) Configure email in email_report() by replacing the placeholder logic with a working command or library (e.g., mailx, sendmailR, or Rmail).
```

Configuration

Inside the master script, there is a master_config list. Adjust values
as needed:

master_config \<- list( \# Paths to dependent R scripts (adjust if your
scripts reside elsewhere) schedule_script =
file.path(Sys.getenv("HOME"), "R/update_ev/01_NHL_retrieve_schedule.R"),
win_prob_script = file.path(Sys.getenv("HOME"),
"R/update_ev/02_NHL_win_probability_retrieval.R"), odds_script =
file.path(Sys.getenv("HOME"), "R/update_ev/03_NHL_odds_retrieval.R"),
goalies_script = file.path(Sys.getenv("HOME"),
"R/update_ev/04_starting_goalie_retrieval.R"),

\# Directories for outputs & logs output_dir =
file.path(Sys.getenv("HOME"), "data/NHL/expected_value"), log_dir =
file.path(Sys.getenv("HOME"), "logs"),

\# Behavior toggles save_intermediate = TRUE, \# If FALSE, skips saving
intermediate reports email_reports = FALSE, \# If TRUE, invokes
`email_report()` after completion email_address =
"[user\@email.com](mailto:user@email.com){.email}",

\# Retry policy max_retries = 3, \# Number of attempts per script
retry_delay = 5 \# Seconds between attempts )

```         
schedule_script, win_prob_script, odds_script, goalies_script:

    Must point to actual script files. If you reorganize directories, update these paths.

    If any of the “vital” scripts (schedule, win‐probability, odds) cannot be sourced after retries, the master script exits with an error.

output_dir, log_dir:

    Will be created if they don’t already exist (dir.create(..., recursive = TRUE)).

    Make sure the user running the script has write permissions.

save_intermediate:

    When TRUE, the script writes a timestamped .txt report for each data category (schedule, win_probabilities, odds, goalies).

    If your datasets are large or you don’t want intermediate logs, set this to FALSE.

email_reports & email_address:

    email_reports = TRUE triggers a call to email_report(output_file) at the end. This function is a placeholder; you must implement the actual sending logic.

    If left FALSE, the script simply logs that emailing wasn’t configured.

max_retries & retry_delay:

    Tune based on empirical experience/latency of APIs or data sources. Retries occur only for sourcing the R scripts, not for web/API calls inside those scripts.

Caveat: If you change the name or signature of any retrieval function in dependent scripts, the source_with_retry() logic will not properly assign the retrieved functions into the global environment, leading to null objects or errors.
```

Directory Structure

Below is an example structure. If yours differs, update master_config
accordingly.

home/ ├── R/ │ └── update_ev/ │ ├── 01_NHL_retrieve_schedule.R │ ├──
02_NHL_win_probability_retrieval.R │ ├── 03_NHL_odds_retrieval.R │ └──
04_starting_goalie_retrieval.R ├── data/ │ └── NHL/ │ └──
expected_value/ \# Final CSVs get saved here ├── logs/ \# Master script
logs & summary reports └── master.R \# This master script (README
target)

```         
R/update_ev/01_… – Four retriever scripts.

data/NHL/expected_value/ – CSV outputs are written here as <TIMESTAMP>_h2h_expected_values.csv.

logs/ –

    One log file per run (e.g., nhl_master_20250531_140512.log).

    Intermediate .txt reports (e.g., nhl_schedule_20250531.txt, nhl_win_probabilities_20250531.txt, etc.).

    Final summary .txt (e.g., nhl_ev_summary_20250531.txt).

Reality check: If any directory is mounted read‐only, you’ll see “permission denied” errors. Confirm write privileges before running.
```

Usage

```         
Open a terminal and navigate to the directory containing master.R.

Run without arguments (processes today and tomorrow):
```

Rscript master.R

(Equivalent to calling retrieve_nhl_expected_values() with no
manual_dates.)

Run with specific dates (overrides “today & tomorrow”):

Rscript master.R 2025-06-01 2025-06-02

```         
You can supply one or multiple dates in the format YYYY-MM-DD.

The script logs which dates it parsed.
```

Sourcing within R (for ad‐hoc testing):

```         
source("master.R")
# This won’t execute automatically because of the `if (!interactive() && identical(...))` guard.
# Instead, call:
retrieve_nhl_expected_values(manual_dates = c("2025-06-01", "2025-06-02"))

Inspect logs & outputs:

    After execution, look in logs/ for:

        A new nhl_master_<TIMESTAMP>.log file (run‐level details).

        If save_intermediate = TRUE, files like nhl_schedule_<YYYYMMDD>.txt, nhl_win_probabilities_<YYYYMMDD>.txt, nhl_odds_<YYYYMMDD>.txt, and either nhl_goalies_<YYYYMMDD>.txt or a dummy‐data notice.

        A final nhl_ev_summary_<YYYYMMDD>.txt that reports positive EV bets, counts, and top results.

    In data/NHL/expected_value/, you’ll find one CSV per run, named <YYYYMMDD_HHMMSS>_h2h_expected_values.csv.
```

Logging

```         
Primary Log File:
Each run starts with init_log(), which:

    Creates a timestamped log file in log_dir:

logs/nhl_master_20250531_140512.log

Prints a header (script name, start time) to that file.
```

log_message(message, level)

```         
Prepends a timestamp (YYYY-MM-DD HH:MM:SS) and level (INFO, WARN, ERROR, SUCCESS).

Prints to console with color‐coding:

    ERROR → default console error formatting (will show in R’s standard error channel).

    WARN → yellow text.

    SUCCESS → green text.

    INFO → plain text.

Appends all messages to the LOG_FILE.
```

Intermediate Reports When save_intermediate = TRUE, each retrieval
function (schedule, win probabilities, odds, goalies) calls
save_intermediate_report(data, report_type, dates).

```         
Saves a human‐readable .txt with:

    Header, date, and “Dates processed” list.

    Summary stats: total rows, breakdown by date, first 10 rows of sample.

NOTE: If data is NULL or empty, no report is written.
```

Final Summary generate_final_report(final_data, output_file) writes to:

```         
logs/nhl_ev_summary_<YYYYMMDD>.txt

Contents include:

    “Total games” (unique home vs. away)

    “Total betting opportunities” (rows in final_data)

    Date range (min(game_date) to max(game_date))

    Section on “Positive Expected Value Bets” (count, average EV, max EV, and top 10 by EV).

    Path to the saved CSV.

Be aware: None of these sinks check for disk space. If you run out of disk, logs may be partial or blank.
```

Function Reference

Below is a high‐level summary of the key helper functions defined within
master.R. Use this to understand how pieces fit together and where you
might customize behavior. 1. init_log()

```         
Purpose:

    Creates a new log file with a header (script name, timestamp).

Returns:

    File path of the log file (assigned to global LOG_FILE).
```

2.  log_message(message, level = "INFO")

    Params:

    ```         
     message (string): Text to log.

     level ("INFO" | "WARN" | "ERROR" | "SUCCESS"): Affects color and log severity.
    ```

    Behavior:

    ```         
     Prepends [YYYY-MM-DD HH:MM:SS] LEVEL: message.

     Prints to console (colorized if not ERROR).

     Appends to LOG_FILE.
    ```

3.  parse_command_args()

    Purpose:

    ```         
     Reads command‐line arguments via commandArgs(trailingOnly = TRUE).

     Attempts to coerce them to Date objects.
    ```

    Returns:

    ```         
     Vector of valid Date objects.

     If parsing fails or no arguments given, returns NULL.
    ```

    Side Effects:

    ```         
     Logs an ERROR if invalid dates are provided, then stops execution.
    ```

    Caveat: If the user accidentally types “2025/06/01” or “Jun 1 2025”,
    it fails. Only YYYY‐MM‐DD works.

4.  get_processing_dates(manual_dates = NULL)

    Params:

    ```         
     manual_dates (character vector or NULL): If provided, coerced to Date and takes precedence.
    ```

    Behavior & Priority:

    ```         
     If manual_dates is not NULL, uses those dates.

     Else, calls parse_command_args() to check for CLI args.

     If still NULL, defaults to c(Sys.Date(), Sys.Date() + 1) (“today” and “tomorrow”).
    ```

    Returns:

    ```         
     Sorted, unique vector of Date objects.
    ```

    Logs:

    ```         
     Which dates were ultimately chosen.
    ```

5.  Expected Value & Related Functions

    calcExpectedValue(line, winProb)

    ```         
     Returns (line * winProb) - 1.
    ```

    get_prob(EV, line)

    ```         
     Returns (EV + 1) / line.
    ```

    get_line(EV, winProb)

    ```         
     Returns (EV + 1) / winProb.
    ```

    decimal_to_fraction(decimal_odds)

    ```         
     Equivalent to decimal_odds - 1.
    ```

    calc_kelly_crit(price, winP)

    ```         
     If b = decimal_to_fraction(price), p = winP, q = 1 - p, returns p - (q / b).
    ```

    Reminder: All of these assume that price is in true decimal odds
    (e.g., 2.50, not +150 or fractional strings). Double‐check inputs
    from remote sources.

6.  source_with_retry(script_path, script_name, vital = TRUE)

    Params:

    ```         
     script_path (string): Full path to the R script to source.

     script_name (string): Human‐readable label (“schedule”, “win probabilities”, “odds”, or “starting goalies”).

     vital (logical): If TRUE, failure to source aborts the main script; if FALSE, logs a warning and continues.
    ```

    Behavior:

    ```         
     Attempts up to master_config$max_retries times.

     Each attempt: source(script_path, local = script_env).

     On success, extracts the correct retrieval function from script_env (using script_name to decide which function to assign globally).

     On failure, logs an ERROR, waits master_config$retry_delay seconds (unless out of attempts), then retries.

     If still failing after all retries:

         If vital == TRUE, calls stop(…) which halts execution.

         If vital == FALSE, logs a WARN and returns FALSE.
    ```

    Returns:

    ```         
     TRUE if sourced successfully, FALSE otherwise (and depending on vital, may never return).
    ```

    Warning: If a non‐vital script fails (e.g., starting goalies), the
    code falls back to create_dummy_goalie_data(). This can lead to
    “Unknown” goalie status—be prepared for NA values in downstream
    analysis.

7.  create_dummy_goalie_data(games)

    Params:

    ```         
     games (data.frame): The schedule data (must contain home, away, game_date, start_time_local).
    ```

    Returns:

    ```         
     A data frame with one row per team per game, where goalie = NA, status = "Unknown", and other fields are populated.
    ```

    Side Effects:

    ```         
     Logs a WARN indicating dummy data was generated.
    ```

8.  save_intermediate_report(data, report_type, dates)

    Params:

    ```         
     data (data.frame): The data to report on.

     report_type (string): One of "schedule", "win_probabilities", "odds", "goalies".

     dates (vector of Date): Dates being processed.
    ```

    Behavior:

    ```         
     If master_config$save_intermediate == FALSE or data is NULL or has zero rows, does nothing.

     Otherwise, writes a .txt file named nhl_<report_type>_<YYYYMMDD>.txt in log_dir.

     Contents include:

         Header with “NHL <REPORT_TYPE> Report – <Month DD, YYYY>”

         “Dates processed: <YYYY-MM-DD, YYYY-MM-DD,…>”

         Summary stats: total rows, breakdown by date (if column date exists), sample (first 10 rows).

     Logs a SUCCESS message stating the report was saved.
    ```

9.  generate_final_report(data, output_file)

    Params:

    ```         
     data (data.frame): The final merged data containing EVs, etc.

     output_file (string): Path to the CSV that was just written.
    ```

    Behavior:

    ```         
     Writes logs/nhl_ev_summary_<YYYYMMDD>.txt with:

         Title: “NHL EXPECTED VALUE SUMMARY REPORT”

         “Generated: <Timestamp>”

         “SUMMARY STATISTICS”

             Total games (unique home‐away combos).

             Total betting opportunities (# rows).

             Date range.

         If any positive EV (expected_value > 0):

             Count.

             Average EV (as percentage).

             Max EV (as percentage).

             A table of top 10 by EV (columns: team, price, win_percent, expected_value, kelly_criterion, book).

         If no positive EV found, simply notes “No positive expected value bets found.”

         “Output saved to: <output_file>”

     Appends a log entry noting the summary’s creation.
    ```

10. email_report(output_file)

    Params:

    ```         
    output_file (string): Path to CSV to email.
    ```

    Behavior:

    ```         
    Currently a placeholder. Logs a WARN: “Email functionality not implemented.”

    Example in comments suggests using mail -s 'NHL Expected Values Report' <address> < <file>.
    ```

    To Do:

    ```         
    Replace this with real email code (e.g., call an SMTP via sendmailR, or wrap a shell command that attaches the CSV).
    ```

Error Handling & Pitfalls

```         
Missing or Misnamed Scripts

    If any of the four retriever scripts is not found at the specified path, source_with_retry() will fail. If the script is “vital” (first three), the entire run aborts.

    Fix: Double‐check paths under master_config.

Invalid or Empty Data Frames

    Even if scripts source successfully, if they return NULL or an empty data.frame, the master script may:

        Warn and exit (e.g., no schedule → stop).

        Fall back to dummy data (goalies).

    Tip: Inside each dependent script, implement sanity checks to ensure returned data has at least the required columns.

Incorrect Column Names

    The master script expects specific column names:

        For schedule: home, away, game_date, start_time_local.

        For win probabilities: home, away, team, win_probability (as a “XX%” string).

        For odds: market, home_team, away_team, name, price, point, book.

        For goalies: team, away, home, goalie, status, status_updated.

    Any deviation (e.g., gameTime instead of start_time_local) leads to merge‐failures producing NA columns or outright errors.

    Action: Inspect column names immediately after retrieving; consider adding a small validation function.

Inconsistent Team Naming Conventions

    The master script forces team names to uppercase and removes periods (gsub("\\.", "", name)).

    If external sources use abbreviations, alternate spellings, or trailing whitespace, merges may not match.

    Suggestion: Standardize team names via a lookup table (e.g., map “NYR” and “NY Rangers” both to “NEWYORKRANGERS”).

API/Website Downtime

    The underlying retrieval scripts likely pull from APIs or web pages. If these endpoints change or go down, you’ll see repeated errors in log.

    Forward‐Thinking: Add unit tests (e.g., mock API responses), or at least schedule a dry‐run check to verify data pipelines before commit time.

Time Zone & Date Handling

    The master script sets attr(final_data$current_time, "tzone") <- "America/New_York". But earlier, it uses Sys.time() and Sys.Date() without explicit time zone.

    On a server in a different time zone, Sys.Date() might be off by one. Only the current_time column ends up with the "America/New_York" TZ attribute.

    Consideration: If you plan to run this in UTC or another TZ, you may get weird date boundaries. Be explicit: with_tz(now(), "America/New_York") or always work in UTC.

Disk Space & Log Growth

    If run daily with save_intermediate = TRUE, you’ll accumulate one intermediate report per day per data category plus a summarizing log file. Over months, that’s dozens of files.

    Mitigation: Implement a log‐rotation policy or periodically archive older logs.

Emailing Reports

    Since emailing is only a stub, the default behavior is to log a WARN and move on. If you expect emails but don’t implement, you’ll mistake “WARN: Email functionality not implemented” for “success.”

    Fix: Remove or refine if (master_config$email_reports) email_report() once mailing is live.
```

Future Considerations

```         
Packaging & Dependency Management

    Convert this into an R package (e.g., NHLExpectedValue) with proper namespace, DESCRIPTION file, and tests. Manage dependencies via renv or packrat.

    Pros: Reproducibility, version control, easy deployment.

    Cons: Initial overhead.

Continuous Integration / Scheduling

    Rather than manually running via cron or ad‐hoc, integrate with a CI/CD pipeline (e.g., GitHub Actions) or a dedicated scheduler (e.g., Jenkins, Airflow).

    CI job could:

        Check out the repository.

        Install dependencies.

        Run Rscript master.R.

        Archive artifacts (CSV, logs).

        Notify stakeholders if errors occur.

Robust Email & Notification

    Replace placeholder email_report() with a robust email subsystem (e.g., blastula, gmailr) or integrate Slack/Webhook notifications upon success or failure.

    Example: “🚨 NHL EV Script Failure: Schedule retrieval failed on 2025­05­31 at 14:01:05.”

Improved Team Name Standardization

    Instead of simple toupper() + gsub, use a canonical mapping table to handle edge cases (New: “Tampa Bay Lightning” vs. “TB Lightning”).

    Possibly pull “team IDs” from a trusted API instead of relying on string matches.

Data Caching & Incremental Updates

    Right now, every run re‐downloads all data for the specified dates. For multi‐day backfills or repeated runs, consider caching previous results and only fetching new or updated records.

    Could save an RDS or parquet file of last‐run data and compare changes.

Parallelization

    If retrieving schedule, win probabilities, and odds are each network‐bound, you could run certain retrieval steps in parallel (e.g., using future or a multi‐threaded approach) to speed up.

    Be careful: rate‐limits may apply.

Comprehensive Unit Tests

    Mock the four retrieval scripts to return synthetic data frames with known outputs (e.g., one game, one probability, one odds line, one goalie). Validate that merges and expected‐value calculations are correct.

    Add testthat tests to check:

        Column alignment.

        EV formula correctness.

        Proper fallback to dummy goalie data.

Transition to Tidyverse Pipelines / Modular Design

    The current script crams everything into one monolithic file. Break it into smaller modules/functions and move them into a proper package or a set of source files in R/. E.g., separate:

    R/logging.R

    R/ev_calculations.R

    R/data_processing.R

    R/retrieve_master.R → the main orchestrator.

This would improve maintainability.
```

License & Contact

```         
Author: David Jamieson

    E‐mail: david.jmsn@icloud.com

Last Updated: 2025‐05‐31

Reality check: If you’re reading this after 2025, check whether upstream APIs or data sources have changed. This code is frozen as of May 2025 and may break if NHL endpoints evolve. Treat it as a reference, not a permanent solution.
```
