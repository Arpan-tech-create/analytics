
import psycopg2


def connect_to_database():
    try:
        conn = psycopg2.connect(
            dbname='vedas',
            user='postgres',
            password='sac123',
            host='localhost',
            port='5432'
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None





def get_latest_date():
    conn = connect_to_database()

    if conn:
        cursor = conn.cursor()
  
        query = "SELECT MAX(timestamp) FROM tags"
        cursor.execute(query)

        latest_date = cursor.fetchone()[0]
        print("latest date is:", latest_date)

        cursor.close()
        conn.close()

        return latest_date

    else:
        # Handle the case when the database connection could not be established
        print("Failed to connect to the database")
        return None


def get_distinct_ip_count(from_date=None, to_date=None):
    conn = connect_to_database()

    if conn:
        cursor = conn.cursor()

        query = "SELECT count(*) FROM (SELECT distinct ip FROM access_logs WHERE timestamp >= %s AND timestamp <= %s) t"
        
        if from_date and to_date:
            cursor.execute(query, (from_date, to_date))
        else:
            cursor.execute(query)

        result = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return result
    
def get_distinct_referer_count(from_date=None, to_date=None):
    conn = connect_to_database()

    if conn:
        cursor = conn.cursor()

        query = "SELECT count(*) FROM (SELECT distinct value FROM tags WHERE timestamp >= %s AND timestamp <= %s AND tag_type='domain') t"
        
        if from_date and to_date:
            cursor.execute(query, (from_date, to_date))
        else:
            cursor.execute(query)

        result = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return result
    
def get_distinct_url_count(from_date=None, to_date=None):
    conn = connect_to_database()

    if conn:
        cursor = conn.cursor()

        query = "SELECT count(*) FROM (SELECT distinct value FROM tags WHERE timestamp >= %s AND timestamp <= %s and tag_type='base_url') t"
        
        if from_date and to_date:
            cursor.execute(query, (from_date, to_date))
        else:
            cursor.execute(query)

        result = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return result




def get_daily_tag_counts(from_date=None, to_date=None):  #bar_chart
    conn = connect_to_database()
    if conn:
        cursor = conn.cursor() 

        if from_date and to_date:
            query = "SELECT DATE_TRUNC('day', timestamp) AS date, COUNT(*) AS count FROM tags WHERE timestamp >= %s AND timestamp <= %s GROUP BY date ORDER BY date;"
            cursor.execute(query, (from_date, to_date))
        else:
            query = "SELECT DATE_TRUNC('day', timestamp) AS date, COUNT(*) AS count FROM tags GROUP BY date ORDER BY date;"
            cursor.execute(query)

        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data



def get_distinct_tags(tag_type,from_date=None,to_date=None):
 
   
    conn = connect_to_database()
    if conn:
        cursor = conn.cursor() 
        if from_date and to_date:
            query="select distinct value from tags where tag_type=%s and timestamp >= %s AND timestamp <= %s "
            cursor.execute(query,(tag_type,from_date,to_date))
        else:
              query="select distinct value from tags where tag_type=%s"
              cursor.execute(query,(tag_type,))

        apps = [row[0] for row in cursor.fetchall()]
      
        cursor.close()
        conn.close()
        return apps
    


def distinct_urls_by_tag(value, tag_type):
    conn = connect_to_database() 
    
    if conn:
        cursor = conn.cursor() 
        query = """
            SELECT distinct access_logs.url 
            FROM access_logs 
            JOIN tags ON access_logs.id = tags.req_id 
            WHERE tags.tag_type = %s AND tags.value LIKE %s
        """
        cursor.execute(query, (tag_type, f'%{value}%'))
        urls = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return urls




    

def get_tag_counts_in_date_range(tag_type, from_date=None, to_date=None):   #3 pie_charts(services,applications,sources)
    conn = connect_to_database()
    if conn:
        cursor = conn.cursor()
        if from_date and to_date:
            query = "SELECT value, COUNT(*) AS cnt FROM tags WHERE tag_type = %s AND timestamp >= %s AND timestamp <= %s and value!='vedas.sac.gov.in' and value!='vedas-sac-gov-in.translate.goog' and value!='vedas.sac.gov.in?page=6'  and value!='localhost' GROUP BY value ORDER BY cnt DESC LIMIT 15;"
            cursor.execute(query, (tag_type, from_date, to_date))
        else:
            query = "SELECT value, COUNT(*) AS cnt FROM tags WHERE tag_type = %s and value!='vedas.sac.gov.in' and value!='vedas-sac-gov-in.translate.goog' and  value!='localhost' and value!='vedas.sac.gov.in?page=6'  GROUP BY value ORDER BY cnt DESC LIMIT 15;"
            cursor.execute(query, (tag_type,))

        top_data = cursor.fetchall()
        cursor.close()
        conn.close()
        return top_data



        




























































































































































































































































































































































































































































































































































