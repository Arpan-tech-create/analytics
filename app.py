from flask import Flask, render_template,jsonify,request
import db1


app = Flask(__name__, template_folder='templates')

@app.route('/', methods=['GET'])
def main():
    to_date = db1.get_latest_date()
    
    formatted_to_date = to_date.strftime("%Y-%m-%d")
    print("Formatted to_date:", formatted_to_date) 
    return render_template('dashboard.html',to_date=formatted_to_date)


@app.route('/get_distinct_ip_count', methods=['GET'])
def get_distinct_ip_count_route():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    distinct_ip = db1.get_distinct_ip_count(from_date,to_date)
    data = {
        "distinct_ip": distinct_ip,
      }
    return jsonify(data)

@app.route('/get_distinct_referer_count', methods=['GET'])
def get_distinct_ref_count_route():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    distinct_referer = db1.get_distinct_referer_count(from_date,to_date)
    data = {
        "distinct_referer": distinct_referer,
      }
    return jsonify(data)

@app.route('/get_distinct_url_count', methods=['GET'])
def get_distinct_url_count_route():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    distinct_url = db1.get_distinct_url_count(from_date,to_date)
    data = {
        "distinct_url": distinct_url,
      }
    return jsonify(data)

@app.route('/get_daily_tag_counts', methods=['GET'])   #for Daily Occurrence Bar Chart
def daily_tag_counts():
 
  
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    print(f'From Date: {from_date}, To Date: {to_date}')
    tag_table_bar = db1.get_daily_tag_counts(from_date,to_date)
    tag_data = [(row[0].strftime("%d-%b-%Y"), row[1]) for row in tag_table_bar]
    return jsonify(tag_data)



@app.route('/get_tag_counts_service', methods=['GET']) #for top service Pie chart
def tag_cnt_date_range_service():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    tag_count = db1.get_tag_counts_in_date_range('base_url',from_date,to_date)
    return jsonify(tag_count)



@app.route('/get_tag_counts_application', methods=['GET'])   #for  Top applications pie chart
def tag_cnt_date_range_app():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    application_count_pie_chart = db1.get_tag_counts_in_date_range('referrer',from_date,to_date)
    return jsonify(application_count_pie_chart)



@app.route('/get_tag_counts_source', methods=['GET'])  #for top sources pie chart
def tag_cnt_date_range_source():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    top_sources_pie_chart = db1.get_tag_counts_in_date_range('domain',from_date,to_date)
    return jsonify(top_sources_pie_chart)




@app.route('/get_distinct_tags', methods=['GET'])   #for dropdown values(base_url --> dropdown)
def get_dropdown_tags():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    
    apps = db1.get_distinct_tags('base_url',from_date,to_date)
    return jsonify(apps)

@app.route('/get_urls_by_dropdown_tags', methods=['GET']) # get full urls table by click on dropdown base_urls
def get_urls_for_dropdown_tatgs():
    selected_app = request.args.get('app')
    if selected_app:
        urls = db1.distinct_urls_by_tag(selected_app,'base_url')
    else:
        urls = []
    return urls


@app.route('/get_hourly_data_for_date', methods=['GET'])  #get hourly bar chart by date & selected  values(from 3 piecharts(service,application,source))
def get_hourly_data_for_date():
    selected_date = request.args.get('date')
    value = request.args.get('value')
   
    if selected_date: 
        conn = db1.connect_to_database()
        if conn:
            query = "SELECT EXTRACT(HOUR FROM timestamp) AS hour, COUNT(*) AS count " \
                    "FROM tags " \
                    "WHERE DATE(timestamp) = %s "

            parameters = (selected_date,)

            if value is not None:
                query += " AND value = %s "
                parameters += (value,)

            query += "GROUP BY hour " \
                     "ORDER BY hour;"

            with conn.cursor() as cur:
                cur.execute(query, parameters)
                
                data = cur.fetchall()
                print(data)

            return jsonify(data)
    
    return jsonify([])

@app.route('/update_tag_date_occurrence')
def update_tag_date_occurrence():
    tag_name = request.args.get("tag_name")
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    
    with db1.connect_to_database() as conn:
        if conn:
            # SQL query with date range filtering
            query = f"SELECT DATE_TRUNC('day', timestamp) AS date, COUNT(*) AS count FROM tags WHERE value = '{tag_name}' AND tag_type = 'base_url' AND timestamp BETWEEN '{from_date}' AND '{to_date}' GROUP BY date ORDER BY date;"

            with conn.cursor() as cur:        
                cur.execute(query)
                data = cur.fetchall()
                
                # Format the data to be returned as JSON
                date_occurrence_data = [(row[0].strftime("%d-%b-%Y"), row[1]) for row in data]
                
            # Return the formatted data as JSON
            return jsonify(date_occurrence_data)
        else:
            # If there's no connection to the database, return an empty list
            return jsonify([])
    

@app.route('/update_tag_bar_chart_by_sources')    #change of daily barchart by source
def tag_by_sources():

    source_name=request.args.get("source_name")
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")

    with db1.connect_to_database() as conn:
        if conn:
            query = f"SELECT DATE_TRUNC('day', timestamp) AS date, COUNT(*) AS count FROM tags WHERE value = '{source_name}' and tag_type='domain' AND timestamp BETWEEN '{from_date}' AND '{to_date}' GROUP BY date ORDER BY date;"
            with conn.cursor() as cur:
                cur.execute(query)
                data=cur.fetchall()
                tags_date_by_sources=[(row[0].strftime("%d-%b-%Y"),row[1]) for row in data]
            return jsonify(tags_date_by_sources)
        else:
            return jsonify([])
    

@app.route('/update_tag_bar_chart_by_applications') #change of daily barchart by application
def tag_by_app():
    

    app_name = request.args.get("app_name")
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    with db1.connect_to_database() as conn:
        if conn:
            query = f"SELECT DATE_TRUNC('day', timestamp) AS date, COUNT(*) AS count FROM tags WHERE value = '{app_name}' and tag_type ='referrer' AND timestamp BETWEEN '{from_date}' AND '{to_date}' GROUP BY date ORDER BY date;"
            with conn.cursor() as cur:
                cur.execute(query)
                data = cur.fetchall()
                tags_date_by_apps = [(row[0].strftime("%d-%b-%Y"), row[1]) for row in data]
                print(tags_date_by_apps)

            return jsonify(tags_date_by_apps)
        else:
            return jsonify([])
        

@app.route('/get_urls', methods=['GET'])
def get_urls():
    selected_app = request.args.get('app')
    urls = db1.distinct_urls_by_tag(selected_app,'base_url')
    return jsonify({'urls': urls})
if __name__ == "__main__":
    app.run(host='0.0.0.0',debug=True, port=5003)