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

parser.add_argument('--remote-connection', metavar='STRING', default='mongodb://localhost:27017/', 
    help='connection string for target mongodb database (default: mongodb://localhost:27017/)')

# We give the user to option to only upload repositories that have a license file.

parser.add_argument('--check-repo-license', dest='checkLicenses', action='store_true', 
    help='Use the GitHub API to double check for each repository if it has a license file. (default: False)')

parser.add_argument('--github-token', metavar='TOKEN', 
    default=os.environ.get('GITHUB_TOKEN'), 
    help='''personal access token for GitHub 
(by default, the environment variable GITHUB_TOKEN is used)''')

args = parser.parse_args()

if not os.path.isfile(args.database_path):
    sys.exit('database file was not found')

# We check if the user wants to double check the licenses of the repositories and 
# if so ask for a github token.

github_token = None

if args.checkLicenses:
    github_token = args.github_token
    if not github_token:
        input_github_token = input(''' > In order to check the licenses of the repositories
(before uploading them) please provide a GitHub Access Token:\n''')
        if not input_github_token:
            sys.exit('''Without a GitHub Token you cannot check the licenses.
You can still run the script without the license check.''')
        else:
            github_token = input_github_token

#-------------------------------------------------------------------------------

# Globally we store the current JSON Object that is constructed before it is uploaded to
# the remote database.
document = {}
license = ""

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
# and also a helping function that can handle request throttling if we run into the
# rate limit of the GitHub API.

def get(url, params={}):
    global http_requests
    time.sleep(0.72) # throttle github api
    try:
        res = requests.get(url, params, headers={'Authorization': f'token {github_token}'})
    except requests.ConnectionError:
        print("\nERROR :: There seems to be a problem with your internet connection.")
        return signal_handler(0,0)
    http_requests += 1
    if res.status_code == 403:
        return handle_rate_limit_error(res)
    elif res.status_code != 200:
        return 0 #res.raise_for_status()
    else:
        return res

def handle_rate_limit_error(res):
    t = res.headers.get('X-RateLimit-Reset')
    if t is not None:
        t = max(0, int(int(t) - time.time()))
    else: 
        t = int(res.headers.get('Retry-After', 60))
    err_msg = f'Exceeded rate limit. Retrying after {t} seconds...'
    old_msg = update_status(err_msg)
    time.sleep(t)
    update_status(old_msg)
    return get(res.url)

#-------------------------------------------------------------------------------

# With this helper function we can check if a repository has a license that we can use.
# We call the GitHub api for each repository and check if the license exists and if it
# is in the list of hardcoded open source licenses.
# This step is only necessary if the argument --check-repo-license is set to True.
# We do not use the '/license' endpoint of GitHub because it returns a status 404 if the 
# repository does not have a license.

def check_license(row):
    licenses = ['apache-2.0', 'agpl-3.0', 'bsd-2-clause', 'bsd-3-clause', 'bsl-1.0',
            'cc0-1.0', 'epl-2.0', 'gpl-2.0', 'gpl-3.0', 'lgpl-2.1', 'mit',
            'mpl-2.0', 'unlicense']
    res = get('https://api.github.com/repos/' + row[2] + '')
    if res == 0:
        return False
    elif res.json()['license'] and res.json()['license']['key'] and res.json()['license']['key'] in licenses:
        global license
        license = res.json()['license']['key']
        return True
    else:
        return False

# This is a helper function that we use to extract the compiler version from the
# source code of a Solidity file. We use a regular expression to extract the version.

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

# We have another helper function that can check if a file is a json file.
# Due to an error in the scraper some wrong .json files were downloaded.
def check_file_extension(path):
    if path.endswith('.json'):
        return True
    else:
        return False

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
    # open source. If the license is not open source we skip the repository.

    if args.checkLicenses and not check_license(row):
        update_status('Missing License for: %s' % row[2])
        repos_handled += 1
        continue

    # Each row represents a repository. Hence we select all files from this repository
    # from the files table and store them in a list. We can now iterate over the files,
    # get the corrosponding commits from the comit table and construct the data object
    # that should be uploaded to the mongodb database.

    dbcursor.execute("SELECT file_id, name, path, sha FROM file WHERE repo_id = ?", (row[0],))

    for file_id, f_name, f_path, f_sha in dbcursor.fetchall():

        update_status('Processing file: "%s" from "%s"' % (f_path, row[2]))

        # The crawler has a bug that sometimes adds a file to the database that include a
        # .sol in the name but that are actually .json files and not Solidity files. We want
        # to skip these files.
        if check_file_extension(f_path):
            update_status('Skipping file: "%s" from "%s" because its JSON' % (f_path, row[2]))
            time.sleep(5)
            continue

        document = {
            # "_id": { "$oid": "63f64e1cd56ad6d1d7c1a887" },
            "name": f_name,
            "path": f_path,
            "sha": f_sha,
            "language": "Solidity",
            "license": license,
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
            FROM comit WHERE file_id = ? ORDER BY created''', (file_id,))

        # We loop over the commits and add them to the data object.

        vid = 0
        for v_sha, v_message, v_size, v_created, v_content, v_parents in dbcursor.fetchall():
            document['versions'].append({
                "version_id": vid,
                "sha": v_sha,
                "message": v_message,
                "size": v_size,
                "created": v_created,
                "compiler_version": get_compiler_version(v_content),
                "content": v_content,
                "parents": v_parents
            })
            vid += 1
            commits_handled += 1

        files_handled += 1

        # In order to avoid documents with no versions, we check if the document has any
        # versions. If not, we skip it and continue with the next file.

        if not document['versions']:
            update_status('File "%s" has no versions' % f_path)
            continue

        # Before we upload the data to the remote database we need to check if the file is already
        # in the database to avoid duplicates.
        # Usually the file sha uniquely identifies the file. However, if forks are included in 
        # the database, the same file sha can exist for different files. Therefore uniqueness
        # can only be guaranteed if the repo_id and the file sha are combined.
        
        duplicate = mongo_collection.find_one({"repo.repo_id": row[0], "sha": f_sha})
        if duplicate:
            duplicate_files += 1
            update_status('File "%s" already exists in MongoDB' % f_path)
            continue

        # As a final step we use pymongo to insert the data object into the remote database.
        # We check if the insertion was successful and if not we print an error message.
        # Afterwards we update the UI to show the user that a file has been processed.

        inserted = mongo_collection.insert_one(document).inserted_id
        if not inserted:
            update_status('ERROR :: Inserting "%s" into MongoDB failed' % f_path)
            time.sleep(1)

        finished += 1
        document = {}
        clear_footer()
        print_summary()

    license = ""
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
