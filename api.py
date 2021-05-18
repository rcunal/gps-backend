from flask import Flask, request, jsonify
import mysql.connector
import requests
import geopy
import traceback


def get_db_password():
    with open("/root/credentials", 'r') as f:
        db_password = f.readline()
        return db_password


app = Flask(__name__)
param_list = []
db = mysql.connector.connect(user='root', password=get_db_password(), host='127.0.0.1', database='gps',
                             auth_plugin='mysql_native_password')
cursor = db.cursor()
cursor.execute("SET SESSION interactive_timeout=31536000")


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
row in rows:
    lap yoksa hesapla
        if row1date - row2_date
        
    update row
"""


def calculate_laps(device_id):
    cursor.execute()



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


@app.route('/laps')
def laps():



    return "test"
