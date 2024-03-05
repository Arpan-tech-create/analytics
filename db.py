import psycopg2

def connect_to_database():
    conn = psycopg2.connect(
        dbname='vedas',
        user='postgres',
        password='sac123',
        host='localhost',
        port='5432'
    )
    return conn

def get_log_entry(conn, id):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ip, timestamp, url, response, size, referer, client
        FROM access_logs
        WHERE id = %s
    ''', (id,))
    
    row = cursor.fetchone()
    if row:
        columns = ('ip', 'timestamp', 'url', 'response', 'size', 'referer', 'client')
        log_entry = dict(zip(columns, row))
        return log_entry
    else:
        return None

def insert_log_entry(conn, ip, timestamp, url, response, size, referer, client):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO access_logs (ip, timestamp, url, response, size, referer, client)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    ''', (
        ip, timestamp, url, response, size, referer, client
    ))
    conn.commit()
    inserted_id = cursor.fetchone()[0]
    return inserted_id

def insert_tags(conn, tags):
    cursor = conn.cursor()
    insert_tags_query = '''
        INSERT INTO tags(req_id, tag_type, value, timestamp)
        VALUES (%s, %s, %s,%s)
    '''
    tag_data = [(tag['req_id'], tag['tag_type'], tag['value'],tag['timestamp']) for tag in tags]
    cursor.executemany(insert_tags_query, tag_data)
    conn.commit()
    cursor.close()

def fetch_access_logs_from_db(conn):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, ip, timestamp, url, response, size, referer, client
        FROM access_logs
    ''')
    rows = cursor.fetchall()
    columns = ('id', 'ip', 'timestamp', 'url', 'response', 'size', 'referer', 'client')
    access_logs = [dict(zip(columns, row)) for row in rows]
    cursor.close()
    return access_logs

if __name__ == '__main__':
    conn = connect_to_database()

    while True:
        try:
            action = input("Enter 'r' to retrieve, 'i' to insert, 'f' to fetch access logs, or 'exit' to quit: ")
            if action.lower() == 'exit':
                break
            
            if action.lower() == 'r':
                log_id = int(input("Enter log entry ID: "))
                log_entry = get_log_entry(conn, log_id)
                if log_entry:
                    print("Retrieved Data:")
                    for key, value in log_entry.items():
                        print(f"{key}: {value}")
                else:
                    print("Log entry not found.")
            
            elif action.lower() == 'i':
                ip = input("Enter IP: ")
                timestamp = input("Enter Timestamp: ")
                url = input("Enter URL: ")
                response = int(input("Enter Response: "))
                size = int(input("Enter Size: "))
                referer = input("Enter Referer: ")
                client = input("Enter Client: ")
                
                inserted_id = insert_log_entry(conn, ip, timestamp, url, response, size, referer, client)
                print(f"Inserted new log entry with ID: {inserted_id}")
            
            elif action.lower() == 'f':
                access_logs = fetch_access_logs_from_db(conn)
                if access_logs:
                    print("Fetched Access Logs:")
                    for log_entry in access_logs:
                        print(log_entry)
                else:
                    print("No access logs found.")
            
            else:
                print("Invalid action. Please enter 'r', 'i', 'f', or 'exit'.")
        except ValueError:
            print("Invalid input. Please enter valid values.")
    
    conn.close()
