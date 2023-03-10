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
# and the "Smart Contract Repository". The file-scaper produces a local sqlite3
# database containing smart contracts and their meta data in three tables (file,
# repo and comit). This script will transfer the data into a json-like format and
# upload it to the remote MongoDB database of the "Smart Contract Repository"

import os, sys, argparse, shutil, time, signal
import sqlite3, csv
import requests, pymongo
import json, re

# First we parse the user arguments

# fix for argparse: ensure terminal width is determined correctly
os.environ['COLUMNS'] = str(shutil.get_terminal_size().columns)

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''Transfer local sqlite data into remote mongodb database.''')

parser.add_argument('--database-path', metavar='FILE', default='results.db', 
    help='path to local sqlite3 database (default: results.db)')

# TODO: Implement statistics file??
# (store repo_id or file (sha?) in statistics file for interrupted runs)
# parser.add_argument('--statistics', metavar='FILE', default='sampling.csv', 
#     help='upload statistics file (default: sampling.csv)')

parser.add_argument('--remote-connection', metavar='STRING', default='mongodb://localhost:27017/', 
    help='connection string for target mongodb database (default: mongodb://localhost:27017/)')

# The following two arguments are only needed if we want to double check licenses
parser.add_argument('--check-repo-license', dest='checkLicenses', action='store_true', 
    help='Use the GitHub API to double check for each repository if it has a license file. (default: False)')

parser.add_argument('--github-token', metavar='TOKEN', 
    default=os.environ.get('GITHUB_TOKEN'), 
    help='''personal access token for GitHub 
    (by default, the environment variable GITHUB_TOKEN is used)''')

args = parser.parse_args()

if not os.path.isfile(args.database_path):
    sys.exit('database file was not found')

#-------------------------------------------------------------------------------

# Globally we store the current JSON Object that is constructed before it is uploaded to
# the remote database.
document = {}

# We also keep track of the database elements that have been handled so far.
repos_handled = 0
repos_total = 0
files_handled = 0
files_total = 0
commits_handled = 0
commits_total = 0
duplicate_files = 0
finished = 0

# We keep track of the execution time and amount of requests of the script.
start = time.time()
http_requests = 0

# Let's also inform the user that the script is running.
print(' > Starting the Script')

#-------------------------------------------------------------------------------

# During the transfering we want to display information on all the strata uploaded so 
# far and a status message.

status_msg = ''

def print_summary():
    print()
    print(' > Repositories handled: %d / %d ' % (repos_handled, repos_total))
    print(' > Files handled: %d / %d ' % (files_handled, files_total))
    print(' > Commits handled: %d / %d ' % (commits_handled, commits_total))
    print(' > Uploaded Documents: %d' % (finished))
    print(' > Duplicate Files: %d' % (duplicate_files))
    print()
    print(status_msg)

def clear_footer():
    sys.stdout.write(f'\033[8F\r\033[J')

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

# This is a good place to try open the connection to the databases. First we 
# connect to the local sqlite3 file that contains the smart contracts and afterwards
# we connect to the remote MongoDB database. If an error occurs we exit the script.
# Hint: 'commit' is a reserved keyword in sqlite, therefore the tablename is 'comit'.

try:
    print(' > Trying to connect to Sqlite3 database: "%s"' % args.database_path)
    db = sqlite3.connect(args.database_path)
except sqlite3.Error as e:
    error_string = 'sqlite DB connection failed', str(e)
    sys.exit(error_string)

dbcursor = db.cursor()

sys.stdout.write('\033[F\r\033[J')
print(' > Successfully connected to Sqlite3 database: "%s"' % args.database_path)

try:
    print(' > Trying to connect to MongoDB database: "%s"' % args.remote_connection)
    # client = MongoClient('mongodb://<username>:<password>@<host>:<port>/')
    mongo_client = pymongo.MongoClient(args.remote_connection)
    mongo_client.server_info() # check if connection is successful
except pymongo.errors.ServerSelectionTimeoutError as e:
    error_string = 'MongoDB connection failed', str(e)
    sys.exit(error_string)

mongo_db = mongo_client.main_db
mongo_collection = mongo_db.contracts

sys.stdout.write('\033[F\r\033[J')
print(' > Successfully connected to MongoDB database: "%s"' % args.remote_connection)

#-------------------------------------------------------------------------------

# Before starting the loop over the database, let's see if we have a statistics 
# file that we could use to continue a previous script run. If so, let's
# get our data structures and UI up-to-date; otherwise, create a new statistics
# file.

# if os.path.isfile(args.statistics):
#     # update_status('Continuing previous upload...')
#     with open(args.statistics, 'r') as f:
#         fr = csv.reader(f)
#         next(fr) # skip header
#         for row in fr:
#             strat_first = int(row[0])
#             # total_sam_comit += sam_comit
#             clear_footer()
#             print_summary()
# else:
#     with open(args.statistics, 'w') as f:
#         f.write('stratum_first,stratum_last,population_file,sample_repo,sample_file,sample_comit\n')


# statsfile = open(args.statistics, 'a', newline='')
# stats = csv.writer(statsfile)

#-------------------------------------------------------------------------------

# Let's also quickly define a signal handler to cleanly deal with Ctrl-C. If the
# user quits the program we want to be able continue more-or-less where we left of.
# So we need to properly close the database connections and statistic file.

def signal_handler(sig,frame):
    db.commit()
    db.close()
    mongo_client.close()
    print("\n > Script terminated by user.")
    print(" > The program took " + time.strftime("%H:%M:%S", 
        time.gmtime((time.time())-start)) + " to execute (Hours:Minutes:Seconds).")
    print(" > The program has sent " + str(http_requests) + " requests.\n")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


#-------------------------------------------------------------------------------

# In order to handle Get request to the GitHub API we define a small helper function
# and also a helping function that can handle request errors.

def get(url, params={}):
    global http_requests
    # throttle github api
    time.sleep(0.72)
    try:
        res = requests.get(url, params, headers={'Authorization': f'token {args.github_token}'})
    except requests.ConnectionError:
        print("\nERROR :: There seems to be a problem with your internet connection.")
        return signal_handler(0,0)
    http_requests += 1
    
    if res.status_code == 403:
        return handle_rate_limit_error(res)
    elif res.status_code != 200:
        res.raise_for_status()
    else:
        return res

def handle_rate_limit_error(res):
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
    return get(res.url)

#-------------------------------------------------------------------------------

# Helper function to check if the license of the repository is open source.

def check_license(row):
    licenses = ['apache-2.0', 'agpl-3.0', 'bsd-2-clause', 'bsd-3-clause', 'bsl-1.0',
            'cc0-1.0', 'epl-2.0', 'gpl-2.0', 'gpl-3.0', 'lgpl-2.1', 'mit',
            'mpl-2.0', 'unlicense']
    # Send GET request to GitHub API to get the license
    res = get('https://api.github.com/repos/' + row[2] + '/license')
    if res.json()['license']['key'] in licenses:
        return True
    else:
        return False

# Helper function to get the compiler version from the content

def get_compiler_version(content):
    check_version = re.search(r'pragma solidity [<>^]?=?\s*([\d.]+)', content)
    if (check_version):
        return check_version.group(1)
    else:
        return None
    
    # ALTERNATIVE:
    # compiler_version = ''
    # for line in content.splitlines():
    #     if line.startswith('pragma solidity'):
    #         compiler_version = line.split(' ')[2]
    #         break
    # return compiler_version[:-1]

#-------------------------------------------------------------------------------

# Now we can finally get into it! 

status_msg = 'Initialize Program'

# Before we start we count the rows in the three tables and update the UI in order
# to give the user an overview on how much data we are dealing with.

dbcursor.execute("SELECT COUNT(1) FROM repo")
repos_total = dbcursor.fetchone()[0]
dbcursor.execute("SELECT COUNT(1) FROM file")
files_total = dbcursor.fetchone()[0]
dbcursor.execute("SELECT COUNT(1) FROM comit")
commits_total = dbcursor.fetchone()[0]

print_summary()
time.sleep(0.5)

# In order to get the necessary data from the sqlite3 database and construct
# the data object that we want to send to the remote database, we need to loop over
# the repositories, files and commits. We start by getting a cursor on all 
# the repositories in the sqlite3 file.

repocursor = db.cursor()
repocursor.execute("SELECT * FROM repo")

# We iterate over the rows using the cursor directly and not fetchall() to avoid loading
# all rows into the ram at once. This is especially important if the database is large.
# The cursor is consumed during this iteration, so we can't use it again later.

# TODO: It is probably safer to only get specific columns from the database instead of 
# using the start (*) operator. Then we can call the columns by name instead of by index.
# This would also make the code more readable. It can be done for the repo and file table.

for row in repocursor:
    # repo_id is row[0]
    # repo name is row[1]
    # ...

    # For each repository we first check the license again to assure that it is 
    # open source.
    if args.checkLicenses and not check_license(row):
        print(' > License missing for: %s' % row[2])
        continue

    # Each row represents a repository. Hence we select all files from this repository
    # from the files table and store them in a list. We can now iterate over the files,
    # get the corrosponding commits from the comit table and construct the data object
    # that should be uploaded to the mongodb database.

    dbcursor.execute("SELECT * FROM file WHERE repo_id = ?", (row[0],))
    files = dbcursor.fetchall()

    for file in files:

        update_status('Processing file: "%s" from "%s"' % (file[2], row[2]))

        document = {
            # "_id": { "$oid": "63f64e1cd56ad6d1d7c1a887" },
            "name": file[1],
            "path": file[2],
            "sha": file[3],
            "language": "Solidity",
            "license": "",
            "repo": {
                "repo_id": row[0],
                "full_name": row[2],
                "description": row[3],
                "url": row[4],
                "owner_id": row[6]
            },
            "versions": []
        }
        
        dbcursor.execute('''SELECT sha, message, size, created, content, parents
            FROM comit WHERE file_id = ? ORDER BY created''', (file[0],))

        # We loop over the commits and add them to the data object.
        
        vid = 0
        for sha, message, size, created, content, parents in dbcursor.fetchall():
            document['versions'].append({
                "version_id": vid,
                "sha": sha,
                "message": message,
                "size": size,
                "created": created,
                "compiler_version": get_compiler_version(content),
                "content": content,
                "parents": parents
            })
            vid += 1
            commits_handled += 1

        files_handled += 1

        # Before we upload the data to the remote database we need to check if the file is already
        # in the database to avoid duplicates.
        # Usually the file sha uniquely identifies the file. However, if forks are included in 
        # the database, the same file sha can exist for different files. Therefore uniqueness
        # can only be guaranteed if the repo_id and the file sha are combined.
        
        duplicate = mongo_collection.find_one({"repo.repo_id": row[0], "sha": file[3]})
        if duplicate:
            duplicate_files += 1
            update_status('File "%s" already exists in MongoDB' % file[1])
            continue

        # As a final step we use pymongo to insert the data object into the remote database.
        # We check if the insertion was successful and if not we print an error message.
        # Afterwards we update the UI to show the user that a file has been processed.

        inserted = mongo_collection.insert_one(document).inserted_id
        if not inserted:
            update_status('ERROR :: Inserting "%s" into MongoDB failed' % file[1])

        finished += 1
        document = {}
        clear_footer()
        print_summary()

    repos_handled += 1
    clear_footer()
    print_summary()

# In the end we close the connections the database and the statistics file.
db.commit()
db.close()
mongo_client.close()

update_status('Done.')
print("\n > The program took " + time.strftime("%H:%M:%S", time.gmtime((time.time())-start)) + 
    " to execute (Hours:Minutes:Seconds).")
print(" > The program has sent " + str(http_requests) + " requests.\n")