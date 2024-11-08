from railbe.raildb import RailDB



# Set up database
db = RailDB(dbname = "BelgianRail.db")

# Add station data
db.update_stations()

# Add liveboard data
db.update_liveboard()

# Close connection
db.close()
