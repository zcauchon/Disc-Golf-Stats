import dagster as dg

daily_partitions = dg.DailyPartitionsDefinition(start_date="2020-01-01", timezone="America/New_York")

