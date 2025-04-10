
import dagster as dg
from datetime import timedelta

daily_refresh_job = dg.define_asset_job(
    "daily_refresh",
    selection=[
        "event_requests_stg",
        "event_details_stg",
        "event_requests",
        "player_dim",
        "event_dim",
        "course_layout_dim",
        "event_score_fact",
        "requests_by_status_per_day",
        "event_attendance",
        "most_active_players"
    ],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent":1})
)

@dg.schedule(
    job=daily_refresh_job,
    execution_timezone="America/New_York",
    cron_schedule="0 6 * * *" # run everyday at 6am
)
def daily_schedule(context: dg.ScheduleEvaluationContext):
    previous_day = context.scheduled_execution_time.date() - timedelta(days=1)
    str_date = previous_day.strftime("%Y-%m-%d")
    return dg.RunRequest(
        run_key=str_date,
        partition_key=str_date
    )