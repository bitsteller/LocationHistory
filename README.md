# LocationHistory

Contains two components:
- Javascript code to download Google Location History (kml) for multiple days
- Python importer that loads kml data from Google Location History into a Postgres database


## downloadLocationHistory.js

In order to download kml files for multiple days containing the location history of a user, log into the users account on Google Maps. Copy the Javascript code in the file "downloadLocationHistory.js" into the console in your browser and modify the start and end date variables. Execute the code. Fetch your files in the download folder.


## Postgres importer

To import trips into a postgres DB do the following:

1. Create a subdirectory "kml". Inside this folder you create a folder for each user and place his/her kml files inside the folder.
2. Set the DB credentials in config.py
3. Execute upload_trips.py to run the import


Optionally merge_trips.py can be used to join trips ("activities") that occured very close together in time.

