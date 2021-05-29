from flask import Flask, request, jsonify
import mysql.connector
import requests
import geopy
import traceback
import numpy as np
import timeit
import datetime


def get_db_password():
    with open("/root/credentials", 'r') as f:
        db_password = f.readline()
        return db_password


app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

param_list = []
db = mysql.connector.connect(user='root', password=get_db_password(), host='127.0.0.1', database='gps',
                             auth_plugin='mysql_native_password')
cursor = db.cursor()
cursor.execute("SET SESSION interactive_timeout=31536000")
cursor.execute("SET SESSION wait_timeout=31536000")


def get_max_speed(coordinate1, coordinate2):
    url = "http://178.20.231.217:8989/route"
    param_str = f"?point={coordinate1[0]},{coordinate1[1]}&point={coordinate2[0]},{coordinate2[1]}&details=max_speed"
    r = requests.get(url + param_str)
    if r.status_code != 200:
        return None

    res = r.json()
    max_speeds = res['paths'][0]['details']['max_speed']

    if not max_speeds:
        return None

    max_speeds = [x[2] for x in max_speeds if x[2] is not None]

    if not max_speeds:
        return None

    return max(max_speeds)


def get_last_coordinate(coordinate):
    last_coordinate_query = "select * from test where id = %s and time_stamp < %s order by time_stamp desc limit 1"
    last_coordinate_query_values = (coordinate['id'], coordinate['time'])
    cursor.execute(last_coordinate_query, last_coordinate_query_values)
    prev_coordinate_raw = cursor.fetchall()
    return prev_coordinate_raw


def get_speed(cur_coordinate, prev_coordinate):
    distance = geopy.distance.geodesic((cur_coordinate['x'], cur_coordinate['y']),
                                       (prev_coordinate[0][4], prev_coordinate[0][5])).km
    elapsed_time = (cur_coordinate['time'] - prev_coordinate[0][2]).seconds / 3600.0
    speed = distance / elapsed_time
    return speed


# needs to add expected_speed
def get_snap(coordinates):
    url = "http://178.20.231.217:5000/match/v1/driving/"

    expected_speed = None

    for index, coor in enumerate(coordinates):
        prev_coordinate = get_last_coordinate(coor)
        if prev_coordinate:
            param_str = prev_coordinate[0][5] + "," + prev_coordinate[0][4] + ';'
            param_str += coor['y'] + "," + coor['x']
            r = requests.get(url + param_str)

            if r.status_code == 200:
                res = r.json()
                xy = [res['tracepoints'][-1]['location'][1], res['tracepoints'][-1]['location'][0]]
                coordinates[index]['x'], coordinates[index]['y'] = xy[0], xy[1]
                coordinates[index]['snapped'] = 1

            else:
                coordinates[index]['snapped'] = 0

            coordinates[index]['speed'] = get_speed(coor, prev_coordinate)
            expected_speed = get_max_speed((prev_coordinate[0][4], prev_coordinate[0][5]),
                                           (coor['x'], coor['y']))

        else:
            coordinates[index]['snapped'] = 0
            coordinates[index]['speed'] = None

        coordinates[index]['expected_speed'] = expected_speed

    return coordinates


# insert may be fail due to long query
def insert_many_rows(coordinate_list):
    values_list = [(c['id'],
                    c['device_type'],
                    c['time_stamp'],
                    c['x'],
                    c['y'],
                    c['speed'],
                    c['expected_speed'],
                    c['snapped_flag']) for c in coordinate_list]

    query = "insert into test (id,device_type,time_stamp,latitude,longitude,speed,expected_speed,snapped) " \
            "values " + ",".join("(%s, %s, %s, %s, %s, %s, %s, %s)" for _ in values_list)

    flattened_values = [item for sublist in values_list for item in sublist]

    try:
        cursor.execute(query, flattened_values)
    except:
        print(traceback.format_exc())


"""
id ile eşleşen bütün rowları al order by time_stamp
lap=0 
(gps cihazları için aç kapat tutuluyor)
row in rows:
    lap yoksa hesapla
        if row1date - row2_date
        
    update row
    
    
0,0,0,0,1,1,1,1,0,0,0    
None,10,None,11,11,None
None,None,None,None    
    
"""


def calculate_laps(device_id, since_date):
    start_time = timeit.default_timer()
    current_lap_id = 1
    # cursor.execute("SELECT * FROM test WHERE id = %s and time_stamp >= now() - INTERVAL 1 DAY ORDER BY time_stamp;",
    #                (device_id,))

    cursor.execute("SELECT * FROM test WHERE id = %s and time_stamp >= %s ORDER BY time_stamp;",
                   (device_id, since_date))

    rows = cursor.fetchall()

    cursor.execute("SELECT * FROM test WHERE id = %s and time_stamp <= %s ORDER BY lap DESC LIMIT 1;",
                   (device_id, since_date))
    max_lap_row = cursor.fetchall()
    if max_lap_row:
        max_lap_row = max_lap_row[0]
        if len(max_lap_row) == 11:
            if max_lap_row[9] is not None:
                current_lap_id = max_lap_row[9]

    device_type = rows[0][2]
    lap_ids = [x[9] for x in rows]
    backup_battery_status = [x[10] for x in rows]
    filtered_lap_ids = list(filter(lambda l_id: l_id is not None, lap_ids))
    if filtered_lap_ids:
        current_lap_id = min(filtered_lap_ids)

    cursor.execute("SELECT * FROM test WHERE id = %s and time_stamp < %s ORDER BY time_stamp DESC LIMIT 1;",
                   (device_id, since_date))
    last_row = cursor.fetchall()
    if last_row:
        last_row = last_row[0]

    updated_lap_ids = []

    if device_type == 'h02':
        prev_status = 0
        if last_row:
            prev_status = last_row[10]
        # 0 dan 1 = tur bitiş, 1 den 0 = yeni tur
        for status in backup_battery_status:
            if prev_status == 1 and status == 0:
                current_lap_id += 1
            prev_status = status
            updated_lap_ids.append(current_lap_id)

    elif device_type == 'mobile':
        pass

    # updating only changed laps
    values = [(updated_lap_ids[i], rows[i][0]) for i in range(len(rows)) if rows[i][9] != updated_lap_ids[i]]
    print(len(values))
    cursor.executemany("UPDATE test SET lap = %s WHERE row_id = %s ",
                       values)
    db.commit()
    print(f'elapsed_time:{timeit.default_timer()-start_time}')
    return values
# x = calculate_laps(device_id,since_date)


@app.route('/')
def test():
    param = request.args.get("add")
    if param:
        if param == 'clear':
            param_list.clear()
        else:
            param_list.append(param)
    return jsonify(param_list)


@app.route('/offline', methods=['POST'])
def get_offline_data():
    data_json = request.get_json()
    coordinate_list = data_json['coordinate_list']
    coordinate_list = get_snap(coordinate_list)

    insert_many_rows(coordinate_list)

    return jsonify(data_json)


@app.route('/laps', methods=['POST'])
def laps():
    device_id = request.form.get('device_id')
    secret_key = request.form.get('key')

    if secret_key == "api_key":
        since = request.form.get('since')
        since_date = datetime.datetime.strptime(since, '%d-%m-%Y')
        updated_lap_ids = calculate_laps(device_id, since_date)

        return {"updated lap ids": dict(updated_lap_ids),
                "updated row count": len(updated_lap_ids)
                } #jsonify(updated_lap_ids)
    else:
        return jsonify("Invalid API Key")


@app.route('/search')
def search():
    device_id = request.args.get("device_id")
    func_id = request.args.get('function_id')

    if device_id:
        device_id = int(device_id)

    if func_id:
        func_id = int(func_id)

    if func_id == 1:
        cursor.execute("SELECT * FROM test WHERE id = %s ORDER BY time_stamp desc limit 1;",
                       (device_id,))

        row = cursor.fetchall()[0]
        if row:
            print(f'row:{row}')
            lap_id = row[9]
            if lap_id:
                print(f'lap:{lap_id}')
                cursor.execute("SELECT * FROM test WHERE id = %s and lap = %s ORDER BY time_stamp;",
                               (device_id, lap_id))

                rows = cursor.fetchall()
                if len(rows) > 1:
                    start = rows[0][3]
                    end = rows[-1][3]
                    coordinates = []
                    for row_i in rows:
                        coordinates.append({
                            "long": row_i[5],
                            "lat": row_i[4],
                            "limit": row_i[7],
                            "speed": row_i[6]
                        })

                    results = {
                        "Lap id": lap_id,
                        "Start": start,
                        "End": end,
                        "Coordinates": coordinates
                    }
                    return results

    elif func_id == 2:
        device_type = request.args.get("device_type")

        q = """
        SELECT tt.id, tt.time_stamp, tt.latitude, tt.longitude
        FROM test tt
        INNER JOIN
            (SELECT id, MAX(time_stamp) AS MaxDateTime
            FROM test
            GROUP BY id) groupedtt 
        ON tt.id = groupedtt.id 
        AND tt.time_stamp = groupedtt.MaxDateTime
        """

        if device_type == 'mobile':
            q += "WHERE tt.device_type = 'mobile'"
        elif device_type == 'car':
            q += "WHERE tt.device_type <> 'mobile'"

        cursor.execute(q)
        last_rows = cursor.fetchall()

        results = []
        for row in last_rows:
            results.append({'Device Id': row[0], 'latitude': row[2], 'longitude': row[3], 'date': row[1]})

        return jsonify(results)



    # bitmeyen tur durumu tutulmuyor, hesaplandıysa verinin tur id si var
    elif func_id == 3:
        date_s = request.args.get("date")
        date = datetime.datetime.strptime(date_s, '%d-%m-%Y')

        cursor.execute("SELECT DISTINCT lap FROM test "
                       "WHERE id = %s and time_stamp >= %s and time_stamp < %s + INTERVAL 1 DAY "
                       "ORDER BY lap;",
                       (device_id, date, date))
        laps = cursor.fetchall()

        # if laps:
        #     laps = laps[0]

        results = []

        for lap in laps:
            cursor.execute("SELECT min(time_stamp),max(time_stamp) FROM test "
                           "WHERE id = %s and lap = %s and time_stamp >= %s and time_stamp < %s + INTERVAL 1 DAY "
                           "ORDER BY time_stamp;",
                           (device_id, lap[0], date, date))

            start_end_dates = cursor.fetchall()
            if start_end_dates:
                start_end_dates = start_end_dates[0]
            results.append({
                "Lap Id": lap[0],
                "Start": start_end_dates[0],
                "End": start_end_dates[1]
            })
        return jsonify(results)

    elif func_id == 8:
        start = request.args.get("start")
        end = request.args.get("end")
        start_date = datetime.datetime.strptime(start, '%d-%m-%Y')
        end_date = datetime.datetime.strptime(end, '%d-%m-%Y')

        cursor.execute("SELECT * FROM test WHERE id = %s and time_stamp >= %s and time_stamp <= %s ORDER BY time_stamp;",
                       (device_id, start_date, end_date))

        rows = cursor.fetchall()

        rows_json = []
        for row_i in rows:
            rows_json.append({
                "Timestamp": row_i[3],
                "lon": row_i[5],
                "lat": row_i[4]
            })

        #"Timestamp":
        results = {
            "Device Id": device_id,
            "Result": rows_json
        }

        return results

    return "Something went wrong"
