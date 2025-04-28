# SC Organization Importer

## Description

This Python script (`org_importer.py`) is designed to import and update information about Star Citizen organizations (guilds) from the official Roberts Space Industries (RSI) website. It stores the collected data in an SQLite database (`sc_organizations.db`) and records the history of changes in another database (`corporation_history.db`).

## Features

*   Retrieves detailed information about Star Citizen organizations.
*   Stores data in a local SQLite database.
*   Tracks the history of organization changes (name, member count, etc.).
*   Manages logs in the `sc_org_importer.log` file.

## Installation

1.  **Clone the repository (if applicable) or download the files.**
2.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

Run the import script using the provided shell script:

```bash
./start_importer.sh
```

Or directly with Python:

```bash
python org_importer.py
```

The script will start browsing organizations and updating the database.

## Important Files

*   `org_importer.py`: The main import script.
*   `requirements.txt`: List of required Python dependencies.
*   `start_importer.sh`: Shell script to easily launch the importer.
*   `sc_organizations.db`: SQLite database containing current organization information.
*   `corporation_history.db`: SQLite database containing the history of organization changes.
*   `sc_org_importer.log`: Log file recording the script's activity.

