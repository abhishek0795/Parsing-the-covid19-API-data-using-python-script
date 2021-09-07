# Parsing-the-covid19-API-data-using-python-script

## Steps to perform different tasks in this project.

1. Find out the list of all casses in each country along with the top 3 most affected countries based on number of deaths in the listed country.
2. To get the list of corona confirmed cases in India during April 2020 and May 2020 and save each of the above data result into CSV file.
3. I created a s3 bucket named 'coda-covid-data-s3-abcd' in aws and add a lifecycle rule on the bucket to expire objects older than 90 days.
4. To add cross-region replication to above bucket with another bucket named 'coda-covid-data-backup-s3'.
5. Finally, copy each of the result into CSV file to s3 bucket using python script with timestamp.
