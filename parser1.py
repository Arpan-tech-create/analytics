import psycopg2
import re
import db
from tag_gen import tag_generators
import gzip, os
from multiprocessing import Pool

def extract_log_info(log_line):
    pattern = r'^(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "\w+ (.*?) HTTP\/\d\.\d" (\d+) (\d+) "(.*?)" "(.*?)"$'
    match = re.match(pattern, log_line)
    if match:
        ip = match.group(1)
        timestamp = match.group(2)
        url = match.group(3)
        response = int(match.group(4))
        size = int(match.group(5))
        referer = match.group(6)
        client = match.group(7)

        return ip, timestamp, url, response, size, referer, client
    else:
        return None

def process_gz_file(gz_file_path):
    log_file_path = gz_file_path[:-3]
    if os.path.exists(log_file_path):
        print(f'Log file {log_file_path} already exists & skip it!!')
        return
    
    with gzip.open(gz_file_path, 'rb') as f_in, open(log_file_path, 'wb') as f_out:
        f_out.write(f_in.read())  
    print(f'Extracted {gz_file_path} to {log_file_path}')

    with gzip.open(gz_file_path, 'rt') as f:
        conn = psycopg2.connect(
            dbname='vedas',
            user='postgres',
            password='sac123',
            host='localhost', 
            port='5432'
        )

        cursor = conn.cursor()
        
        for line in f:
            log_info = extract_log_info(line)
           
            if log_info:
                log_id = db.insert_log_entry(conn, *log_info) 
                generated_tags = []
                log_entry_dict = {
                    'id': log_id,
                    'ip': log_info[0],
                    'timestamp': log_info[1],
                    'url': log_info[2],
                    'response': log_info[3],
                    'size': log_info[4],
                    'referer': log_info[5],
                    'client': log_info[6]
                }
                for tag_generator in tag_generators:
                    generated_tags.extend(tag_generator([log_entry_dict])) 
                db.insert_tags(conn, generated_tags)
                
        conn.commit()  
        cursor.close()
        conn.close()

def process_folder(folder_path):
    gz_files = [os.path.join(root, file) for root, dirs, files in os.walk(folder_path) for file in files if file.endswith(".gz")]
    
    with Pool(10) as pool: 
        pool.map(process_gz_file, gz_files)

if __name__ == '__main__':
    folder_path = 'F:/ARPAN/NGINX_LOGS'
    process_folder(folder_path)
