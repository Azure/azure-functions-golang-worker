# Timer Trigger Sample

An Azure Function that runs on a schedule using a CRON expression. The timer info (schedule status, next/last run times, past-due flag) is deserialized into a typed `TimerInfo` struct.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```

## Setup

```bash
cd samples/timerTrigger
go mod init myapp
go get github.com/azure/azure-functions-golang-worker
go mod tidy
```

## Run

```bash
func start
```

`func start` automatically builds the Go project before launching. To skip the build step (e.g., if you've already built manually), use:

```bash
func start --no-build
```

## CRON Expression

The default schedule is `0 */5 * * * *` (every 5 minutes). Azure Functions uses **6-field NCrontab** expressions:

```
{second} {minute} {hour} {day} {month} {day-of-week}
```

Examples:
- `0 */5 * * * *` — every 5 minutes
- `0 0 * * * *` — every hour
- `0 0 0 * * *` — every day at midnight
- `0 30 9 * * 1-5` — 9:30 AM on weekdays

## How It Works

When the timer fires, the Azure Functions host invokes `TimerHandler` with a `TimerInfo` struct containing:
- **Schedule** — whether the schedule adjusts for DST
- **ScheduleStatus** — last run time, next run time, and last updated time
- **IsPastDue** — `true` if the current invocation is later than scheduled
