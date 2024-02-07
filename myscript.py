import requests
import urllib.parse
import json
import psycopg2
import schedule
import time

def fetch_data():
    ACCOUNT_ID = "3796945"
    QUERY_KEY = "NRAK-9TZ9N8EN9M93EELVGR0IW856PJF"
    NRQL = """
        {
            actor{
                account(id: 3796945){
                    nrql(query: "FROM MobileVideo, PageAction, RokuVideo SELECT filter(uniqueCount(viewId), where actionName = 'CONTENT_START') - filter(uniqueCount(viewId), WHERE actionName = 'CONTENT_ERROR' and contentPlayhead < 1000) as 'Plays',sum(playtimeSinceLastEvent / 60000) as 'Minutes', average(playtimeSinceLastEvent / 60000) as 'Average',uniqueCount(session) AS 'Unique',uniqueCount(countryCode) AS 'Country',uniqueCount(regionCode) AS 'Region',uniqueCount(city) AS 'City' FACET vid, aid, contentDuration as 'duration', dateOf(timestamp) as 'Date' since today WHERE trackerName = 'videojs'"){
                        results
                    }
                }
            }
        }   
    """
    NRQL2 = """
        {
            actor{
                account(id: 3796945){
                    nrql(query: "SELECT sum(click) AS 'Clicks', sum(conversion) as 'Conversions' FROM PageAction FACET vid, aid, dateOf(timestamp) SINCE today"){
                        results
                    }
                }
            }
        }   
    """
    # PostgreSQL connection parameters
    DB_NAME = 'mydb'
    DB_USER = 'postgres'
    DB_PASSWORD = 'postgres'
    DB_HOST = '127.0.0.1'
    DB_PORT = '5432'

    # headers = { "Accept": "application/json", "X-Query-Key": QUERY_KEY }
    # url = "https://insights-api.newrelic.com/v1/accounts/{}/query?nrql={}".format(ACCOUNT_ID, urllib.parse.quote_plus(NRQL))

    endpoint = "https://api.newrelic.com/graphql"
    headers = {'API-Key': f'{QUERY_KEY}'}
    response = requests.post(endpoint, headers=headers, json={"query": NRQL})

    if response.status_code == 200:
        # Extract and transform data
        dict_response = json.loads(response.content)
        results = dict_response['data']['actor']['account']['nrql']['results']
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Insert data into PostgreSQL database
        for result in results:
            vid = result["facet"][0]
            aid = result["facet"][1]
            date = result["facet"][3]
            plays = result["Plays"]
            minutes = result["Minutes"]
            uniquee = result["Unique"]
            average = result["Average"]
            country = result["Country"]
            region = result["Region"]
            city = result["City"]

            cursor.execute("select * from stats where date=%s and vid=%s and aid=%s",(date, vid, aid))
            rows = cursor.fetchall()
            if not rows:
                cursor.execute("INSERT INTO stats (date, vid, aid, plays, minutes, uniquee, average, country, region, city, clicks, conversions) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,0,0)", (date, vid, aid, plays, minutes, uniquee, average, country, region, city))
            else:
                cursor.execute("UPDATE stats SET plays = %s, minutes = %s, uniquee = %s, average = %s, country = %s, region = %s, city = %s WHERE date = %s and vid = %s and aid = %s", (plays, minutes, uniquee, average, country, region, city, date, vid, aid))         

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
    else:
        print(f"Error executing NRQL query: {response.text}")

    response = requests.post(endpoint, headers=headers, json={"query": NRQL2})

    if response.status_code == 200:
        # Extract and transform data
        dict_response = json.loads(response.content)
        results = dict_response['data']['actor']['account']['nrql']['results']
        print(results)
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Insert data into PostgreSQL database
        for result in results:
            vid = result["facet"][0]
            aid = result["facet"][1]
            date = result["facet"][2]
            clicks = result["Clicks"]
            conversions = result["Conversions"]
            
            cursor.execute("UPDATE stats SET clicks= %s, conversions = %s WHERE date = %s and vid = %s and aid = %s", (clicks, conversions, date, vid, aid))         

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
    else:
        print(f"Error executing NRQL2 query: {response.text}")

schedule.every(1).minutes.do(fetch_data)
while True:
    schedule.run_pending()
    time.sleep(1)

