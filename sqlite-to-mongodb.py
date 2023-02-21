# Steps:
# - Loop through sqlite "file" table in stratas
# - For each strata:
#   - Get list of database entries from sqlite "file"
#   - Loop through file entries
#       - Check if file already exists in remote MongoDB
#       - For each file entry get "repo" information
#       - For each file entry get list of commits from "comit"
#       - Contruct JSON Object for one file
#       - Upload file entry to MongoDB remote
#   - Store strata information in file to continue script in case of error

# We need:
# Global current JSON Object
# Global Loop over Strata
# Functions to handle the data retrievel from sqlite
# Functions to print output
# Functions to handle request to MongoDB
# Interrupt handler