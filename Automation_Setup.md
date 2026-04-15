# Automation Setup

This project includes two runner scripts:

- [run_hooks.bat](run_hooks.bat) for Windows
- [run_hooks.sh](run_hooks.sh) for macOS/Linux

Both scripts run [daily_valuation.py](daily_valuation.py), preferring the local virtual environment when available.

## 1. One-Time Preparation

### Windows
1. Confirm the project path is correct.
2. Test manually in Command Prompt:
   - `run_hooks.bat`

### macOS/Linux
1. Make the script executable:
   - `chmod +x run_hooks.sh`
2. Test manually:
   - `./run_hooks.sh`

## 2. Windows Task Scheduler (Weekdays)

1. Open Task Scheduler.
2. Click Create Task.
3. On General tab:
   - Name: Portfolio Daily Valuation
   - Select Run whether user is logged on or not (optional).
4. On Triggers tab:
   - New...
   - Begin the task: On a schedule
   - Settings: Weekly
   - Check Mon, Tue, Wed, Thu, Fri
   - Set your desired time
5. On Actions tab:
   - New...
   - Action: Start a program
   - Program/script: `cmd.exe`
   - Add arguments: `/c "C:\path\to\your\project\run_hooks.bat"`
6. Save and run once with Run to verify.

## 3. cron (macOS/Linux, Weekdays)

1. Open crontab:
   - `crontab -e`
2. Add a weekday schedule line. Example: run at 18:00 every weekday:
   - `0 18 * * 1-5 /absolute/path/to/project/run_hooks.sh >> /absolute/path/to/project/automation.log 2>&1`
3. Save and exit.
4. Verify cron entry:
   - `crontab -l`

## 4. Pick Your Time Carefully

Choose a schedule that aligns with your use case (for example, after market close in your preferred timezone).

## 5. Troubleshooting

- If the task runs but no output appears, check the log file (cron) or Task Scheduler history (Windows).
- If Python import errors occur, ensure your virtual environment has required packages installed.
- If paths contain spaces, keep paths quoted in scheduler commands.
