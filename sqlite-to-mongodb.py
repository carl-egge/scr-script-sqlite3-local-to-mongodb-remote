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

#  DIFFERENT STRATEGY:
# Go through the repo table one by one and then select files

#-------------------------------------------------------------------------------
# MIT License
#
# Copyright (c) 2019 Carl Egge
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#-------------------------------------------------------------------------------

############################  SQLITE TO MONGODB  #############################

# This script is developed as a helper when working with the "github-file-scaper"
# and the "Smart Contract Repository". The file scaper produces a local sqlite3
# database containing smart contracts and their meta data in three tables (file,
# repo and comit). This script will transfer the data into a json-like format and
# upload it to the remote MongoDB database of the "Smart Contract Repository"

import os, sys, argparse, shutil, time, signal
import sqlite3, csv
import requests

# First we parse the user arguments

# fix for argparse: ensure terminal width is determined correctly
os.environ['COLUMNS'] = str(shutil.get_terminal_size().columns)

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''Transfer local sqlite data into remote mongodb database.''')

parser.add_argument('--database-path', metavar='FILE', default='results.db', 
    help='path to local sqlite3 database (default: results.db)')

parser.add_argument('--statistics', metavar='FILE', default='sampling.csv', 
    help='upload statistics file (default: sampling.csv)')

parser.add_argument('--remote-connection', metavar='STRING', default='mongodb://localhost:27017/', 
    help='connection string for target mongodb database (default: mongodb://localhost:27017/)')

args = parser.parse_args()

if not os.path.isfile(args.database_path):
    sys.exit('database file was not found')

#-------------------------------------------------------------------------------

# We want to avoid to store the entire database content in a python variable therefore
# we loop through the content of the "file" table and only select a few entries at the
# time. We then handle these entries and when they are uploaded we continue to go through
# the table.

# Globalley we store the current JSON Object that is constructed before it is uploaded to
# the remote database
current = {}
finished = 0

# We keep track of the execution time and amount of requests of the script.

start = time.time()
post_requests = 0


#-------------------------------------------------------------------------------

# During the transfering we want to display information on all the strata uploaded so 
# far and a status message.

status_msg = ''

def print_summary():
    print()
    print('Repositories handled: %d / %d ' % (0, 1))
    print('Files handled: %d / %d ' % (0, 1))
    print('Commits handled: %d / %d ' % (0, 1))
    print('Uploaded Documents: %d' % (finished))
    print()
    print(status_msg)

def clear_footer():
    sys.stdout.write(f'\033[9F\r\033[J')

# For convenience, we also have function for just updating the status message.
# It returns the old message so it can be restored later if desired.

def update_status(msg):
    global status_msg
    old_msg = status_msg
    status_msg = msg
    sys.stdout.write('\033[F\r\033[J')
    print(status_msg)
    return old_msg

#-------------------------------------------------------------------------------

# We also need to establish a connection to the remote database.

# TODO: GET MONGODB CONNECTION

# We define a convienent function to upload data to the remote database

def post(url, params={}):
    global post_requests
    try:
        res = requests.get(url, params)
    except requests.ConnectionError:
        print("\nERROR :: There seems to be a problem with your internet connection.")
        return signal_handler(0,0)
    post_requests += 1
    
    if res.status_code != 200:
        res.raise_for_status()
        return handle_error_code(res)
    else:
        return res

def handle_error_code(res):
    t = res.headers.get('X-RateLimit-Reset')
    if t is not None:
        t = max(0, int(int(t) - time.time()))
    else: 
        t = int(res.headers.get('Retry-After', 60))
    err_msg = f'Exceeded rate limit. Retrying after {t} seconds...'
    if not args.github_token:
        err_msg += ' Try running the script with a GitHub TOKEN.'
    old_msg = update_status(err_msg)
    time.sleep(t)
    update_status(old_msg)
    return post(res.url)

#-------------------------------------------------------------------------------

# This is a good place to try open the connection to the local database.
# 'commit' is a reserved keyword in sqlite, therefore the tablename is 'comit'.

try:
    db = sqlite3.connect(args.database_path)
except sqlite3.Error as e:
    error_string = 'sqlite DB connection failed', str(e)
    sys.exit(error_string)

dbcursor = db.cursor()


#-------------------------------------------------------------------------------

# Now we can finally get into it! 

status_msg = 'Initialize Program'
print_summary()

# Before starting the iterative search process, let's see if we have a sampling
# statistics file that we could use to continue a previous search. If so, let's
# get our data structures and UI up-to-date; otherwise, create a new statistics
# file.

if os.path.isfile(args.statistics):
    update_status('Continuing previous upload...')
    with open(args.statistics, 'r') as f:
        fr = csv.reader(f)
        next(fr) # skip header
        for row in fr:
            strat_first = int(row[0])
            total_sam_comit += sam_comit
            clear_footer()
            print_summary()
else:
    with open(args.statistics, 'w') as f:
        f.write('stratum_first,stratum_last,population_file,sample_repo,sample_file,sample_comit\n')


statsfile = open(args.statistics, 'a', newline='')
stats = csv.writer(statsfile)

#-------------------------------------------------------------------------------

# Let's also quickly define a signal handler to cleanly deal with Ctrl-C. If the
# user quits the program and cancels the search, we want to allow him to later
# continue more-or-less where he left of. So we need to properly close the
# database and statistic file.

def signal_handler(sig,frame):
    db.commit()
    db.close()
    statsfile.flush()
    statsfile.close()
    print("\nThe program took " + time.strftime("%H:%M:%S", 
        time.gmtime((time.time())-start)) + " to execute (Hours:Minutes:Seconds).")
    print("The program has sent " + str(post_requests) + "requests to remote.\n\n")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


#-------------------------------------------------------------------------------

# TODO: MAIN LOOP




update_status('Done.')
print("The program took " + time.strftime("%H:%M:%S", time.gmtime((time.time())-start)) + 
    " to execute (Hours:Minutes:Seconds).")
print("The program has sent " + str(post_requests) + "requests to remote.\n\n")