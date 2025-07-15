# Rockd Usage Stats

The project is meant to parse Traefik logs and post to rockd api, in order to store location statistics for rockd

## Features
- Reads tab-separated log files (`.tsv`)
- Parses and extracts specific fields (date, time, IP, latitude, longitude)
- Batches data in chunks (e.g. 1000 rows)
- Sends data batches to an API via POST requests
- Loads configuration (like API URL and keys) from `.env` file

## Requirements

- Python 3.7+
- Packages listed in `requirements.txt`

## Installation

1. Clone the repo:
   ```bash
   git clone https://github.com/davidsklar99/rockd-usage-stats.git
   cd rockd-usage-stats
