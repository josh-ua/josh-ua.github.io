
# Imports
import time
import sqlite3
import requests
import json
import random
import re

# Database Connection
conn = sqlite3.connect('data.db')
cursor = conn.cursor()

# Scraper Configuration

#working_hash = "t3_7hjhml"
#subreddit = "brogress"
#regex = r"^(M|F)\/(\d+)\/(\d).(\d?\d?).?\s*\[(\d+)\w+\s*to\s*(\d+).*]\s*\((\d+)\s*(\w+)"


working_hash = "t3_7jwd5u"
subreddit = "progresspics"
regex = r"^(M|F)\/(\d+)\/(\d).(\d?\d?).?\s*\[(\d+)\w+\s*&gt;\s*(\d+).*]\s*\((\d+)\s*(\w+)"

# Initialize SQLite3 Database
def initialize_database():
  # Create `threads` table
  conn.execute( """
    CREATE TABLE IF NOT EXISTS threads (
      reddit_uid STRING NOT NULL PRIMARY KEY,
      subreddit STRING NOT NULL,
      working_hash STRING NOT NULL,
      flair_text STRING,
      thumbnail STRING,
      created_utc INT NOT NULL,
      title STRING NOT NULL,
      url STRING NOT NULL,
      permalink STRING NOT NULL,
      score INT
    );
  """ )
  
  conn.execute( """
    CREATE TABLE IF NOT EXISTS threads_parsed (
      reddit_uid STRING NOT NULL PRIMARY KEY,
      gender STRING,
      age INT,
      height_ft INT,
      height_in INT,
      weight_start INT,
      weight_end INT,
      length_amount INT,
      length_unit STRING
    );
  """ )

# Generate a Reddit API Query URL
def generate_scape_path():
  return "https://api.reddit.com/r/" + subreddit + "/new/?count=25&after=" + working_hash

# Pull data from the API
def fetch_remote():
  p = generate_scape_path()
  r = requests.get( p, headers = {'User-agent': 'Chrome'} )
  
  if r.status_code != 200:
    print( "** Error querying, retrying in 5 seconds... [" + generate_scape_path() + "]" )
    print( r.content )
    
    time.sleep( 5 )
    return fetch_remote()
    
  else:
    return r.content

# Begin scraping!
def scrape():
  global working_hash
  
  # Sleep to avoid spamming blacklist
  time.sleep( 1 )
  
  # Query and get the data
  reddit_response_raw = fetch_remote()
  reddit_response = json.loads( reddit_response_raw )
  reddit_response = reddit_response['data']

  # Print the next working has
  print( "*** WORKING HASH: " + working_hash )
  print( "*** NEXT WORKING HASH: " + reddit_response['after'] )

  # Loop through each posted body transformation thread in the response
  for thread_raw in reddit_response['children']:
    # Get the actual data
    thread = thread_raw['data']

    # Attempt to parse based on the title
    matchObj = re.match( regex, thread['title'], re.M|re.I )

    if( matchObj ):
        print( "Parsed" );
        
        # Insert this parsed thread
        conn.execute( "INSERT INTO threads_parsed VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", (
          thread['id'].encode('ascii','ignore'),
          matchObj.group(1),
          int(matchObj.group(2) or 0),
          int(matchObj.group(3) or 0),
          int(matchObj.group(4) or 0),
          int(matchObj.group(5) or 0),
          int(matchObj.group(6) or 0),
          int(matchObj.group(7) or 0),
          matchObj.group(8)
        ) )
    else:
        print( "Match Failure" )

    # Insert it into the database
    conn.execute( "INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", (
      thread['id'].encode('ascii','ignore'),
      thread['subreddit'].encode('ascii','ignore'),
      working_hash,
      "",
      thread['thumbnail'].encode('ascii','ignore'),
      int(thread['created_utc']),
      thread['title'].encode('ascii','ignore'),
      thread['url'].encode('ascii','ignore'),
      thread['permalink'].encode('ascii','ignore'),
      thread['score'],
    ) )
  
    # Commit to the database
    conn.commit()
  
  # Get the next working hash
  working_hash = reddit_response['after']
  
  # Run Recursivley
  scrape()

# Start the script
initialize_database()
scrape()
