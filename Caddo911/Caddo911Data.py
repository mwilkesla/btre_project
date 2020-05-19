import pandas as pd
import psycopg2

tables = pd.read_html('http://ias.ecc.caddo911.com/All_ActiveEvents.aspx', header=0)
            #http://ias.ecc.caddo911.com/All_ActiveEvents.aspx
tables = tables[0]
df = pd.DataFrame(tables)
df.fillna('null', inplace=True)
df['Street'] = df['Street'].astype('string')
df['Cross Streets'] = df['Cross Streets'].astype('string')
df['Mun'] = df['Mun'].astype('string')



print(df)


def doQuery(conn):
    delQuery = "DELETE FROM rawcalls"

    sql = ("INSERT INTO rawcalls "
                            "(agency, units, time, description, street, cross_streets, mun, time_pulled)"
                            "VALUES (%s,%s,%s,%s,%s,%s,%s, CURRENT_TIMESTAMP);")
    
    mergeQuery = ("""INSERT INTO totalcalls (agency, units, time, description, street, cross_streets, mun, time_pulled)
                    SELECT agency, units, time, description, street, cross_streets, mun, time_pulled
                    FROM rawcalls a
                    WHERE NOT EXISTS
                    (SELECT FROM totalcalls b WHERE a.agency = b.agency and a.time = b.time 
                    and a.street = b.street and a.cross_streets = b.cross_streets
                    and a.description = b.description);""")

    try:
        #Delete data from rawcalls
        cur = conn.cursor()
        cur.execute(delQuery)
        conn.commit()
        cur.close()

        #Insert new data into rawcalls
        cur = conn.cursor()

        for index, row in df.iterrows():
                        c = row['Agency']
                        c2 = row['Units']
                        c3 = row['Time'] #This is going to need to be a varchar for now
                        c4 = row['Description']
                        c5 = row['Street']
                        c6 = row['Cross Streets']
                        c7 = row['Mun']

                        cur.execute(sql, [c,c2,c3,c4,c5,c6,c7])
        conn.commit()
        cur.close()

        # #Merge rawcalls into TotalCalls
        cur = conn.cursor()
        cur.execute(mergeQuery)
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        if conn is not None:
            conn.close()

conn = psycopg2.connect(host="167.99.112.137", database="Test", user="dbadmin", password="abc123!")
doQuery(conn)
conn.close()