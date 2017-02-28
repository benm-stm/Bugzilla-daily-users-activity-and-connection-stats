# Bugzilla-daily-users-activity-and-connection-stats

Extract users connection and activity statistics from the database (by croning the script or by giving the date range) and save the result in a json file

#how to crone
30 23 * * * cd /home/bgzuser/Bugzilla_stats && ./stats_to_csv_or_to_json.pl cron >> logs/stats_log.log 2>&1
