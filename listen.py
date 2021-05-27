import socket
import os
from _thread import *
from datetime import datetime
import mysql.connector
import geopy
import geopy.distance
import sys
import logging
import logging.config
import requests
import polyline
import traceback
import atexit

"""" 
TODO:
[-] handle mobile data
"""


def get_db_password():
    with open(os.path.dirname(os.path.abspath(__file__)) + '/credentials', 'r') as f:
        db_password = f.readline()
        return db_password


logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def connect_db():
    db = mysql.connector.connect(user='root', password=get_db_password(), host='127.0.0.1', database='gps',
                                 auth_plugin='mysql_native_password')

    if db.is_connected():
        print('Connected to mysql')
    # cursor = db.cursor()
    # cursor.execute("SET SESSION interactive_timeout=31536000")
    # cursor.execute("SET SESSION wait_timeout=31536000")

    return db


def get_cursor(db):
    cursor = db.cursor()
    cursor.execute("SET SESSION interactive_timeout=31536000")
    cursor.execute("SET SESSION wait_timeout=31536000")
    return cursor


#db = connect_db()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

port = 1000
s.bind(('', port))
print("socket binded to %s" % port)
s.listen(5)
print("socket is listening")

ThreadCount = 0


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


def get_snap(coordinates):
    url = "http://178.20.231.217:5000/match/v1/driving/"
    param_str = ""
    for i in coordinates:
        param_str += str(i[1]) + ',' + str(i[0]) + ';'
    r = requests.get(url + param_str[:-1])
    if r.status_code != 200:
        return {}

    res = r.json()
    # routes = polyline.decode(res['matchings'][0]['geometry'])
    start_point = [res['tracepoints'][0]['location'][1], res['tracepoints'][0]['location'][0]]
    end_point = [res['tracepoints'][-1]['location'][1], res['tracepoints'][-1]['location'][0]]
    distance = res['matchings'][0]['distance']
    duration = res['matchings'][0]['duration']

    try:
        speed = (distance / duration) * 3.6
    except ZeroDivisionError:
        speed = 0

    out = {  # 'route': routes,
        'start_point': start_point,
        'end_point': end_point,
        'distance': distance,
        'duration': duration,
        'speed': speed
    }

    return out


def h02_data_split(data):
    splitted_data = data.split(',')
    logger.info(f'data:{splitted_data}')
    imei = splitted_data[1]
    valid_flag = splitted_data[4]
    hhmmss = splitted_data[3]
    ddmmyy = splitted_data[11]
    vehicle_status = convert_bytes(splitted_data[12])
    backup_battery = int(vehicle_status[1][4])
    time_obj = datetime.strptime(hhmmss + ddmmyy, "%H%M%S%d%m%y")
    timestamp = time_obj.strftime('%Y-%m-%d %H:%M:%S')
    latitude_s = splitted_data[5]
    D = splitted_data[6]
    longitude_s = splitted_data[7]
    G = splitted_data[8]
    geo_str = latitude_s[:2] + ' ' + latitude_s[2:] + "' " + D + ' ' + longitude_s[:3] + ' ' + longitude_s[
                                                                                               3:] + "' " + G
    p = geopy.point.Point(geo_str)

    formatted_result = {'id': imei,
                        'device_type': 'h02',
                        'time_obj': time_obj,
                        'time_stamp': timestamp,
                        'latitude': p.latitude,
                        'longitude': p.longitude,
                        'backup_battery': backup_battery,
                        'validation': valid_flag}

    return formatted_result


def mobile_data_split(data):
    splitted_data = data.split(',')
    logger.info(f'data:{splitted_data}')
    id = splitted_data[0]
    latitude = splitted_data[1]
    longitude = splitted_data[2]

    # date object format should be corrected later
    time_obj = datetime.strptime(splitted_data[3], "%H%M%S%d%m%y")
    timestamp = time_obj.strftime('%Y-%m-%d %H:%M:%S')

    formatted_result = {'id': id,
                        'device_type': 'mobile',
                        'time_obj': time_obj,
                        'time_stamp': timestamp,
                        'latitude': latitude,
                        'longitude': longitude}

    return formatted_result


def get_speed(cur_coordinate, prev_coordinate):
    distance = geopy.distance.geodesic((cur_coordinate['xy']), (prev_coordinate['xy'])).km
    elapsed_time = (cur_coordinate['time'] - prev_coordinate['time']).seconds / 3600.0
    speed = distance / elapsed_time
    return speed


def convert_bytes(x):
    binary = bin(int(x, 16))[2:].zfill(8)
    inverse_s = ''.join(['1' if i == '0' else '0'
                         for i in binary])
    return [inverse_s[index : index + 8] for index in range(0, len(inverse_s), 8)]


def process_data(formatted_data, prev_coordinate, cur_coordinate, db, cursor):
    if 'validation' in formatted_data.keys():
        if formatted_data['validation'] != "A":
            return prev_coordinate, cur_coordinate

    prev_coordinate = cur_coordinate

    cur_coordinate = {'xy': (formatted_data['latitude'], formatted_data['longitude']),
                      'time': formatted_data['time_obj']
                      }
    # ilk veri,
    # ya da yeni bağlantı
    if not prev_coordinate:

        last_coordinate_query = "select * from test where id = %s and time_stamp < %s order by time_stamp desc limit 1"
        last_coordinate_query_values = (formatted_data['id'], cur_coordinate['time'])
        cursor.execute(last_coordinate_query, last_coordinate_query_values)
        prev_coordinate_raw = cursor.fetchall()

        # bağlantı kopmuş, yeniden bağlanmış
        if prev_coordinate_raw:
            prev_coordinate = {'xy': (prev_coordinate_raw[0][4], prev_coordinate_raw[0][5]),
                               'time': prev_coordinate_raw[0][3]
                               }

    snapped_flag = False
    backup_battery = 0
    if 'backup_battery' in formatted_data.keys():
        backup_battery = formatted_data['backup_battery']

    if prev_coordinate:
        try:

            raw_speed = get_speed(cur_coordinate, prev_coordinate)
            if raw_speed > 5 or formatted_data['device_type'] != 'mobile':
                snapped_result = get_snap([prev_coordinate['xy'], cur_coordinate['xy']])
                if snapped_result:
                    cur_coordinate['xy'] = snapped_result['end_point']
                    snapped_flag = True

            speed = get_speed(cur_coordinate, prev_coordinate)
            expected_speed = get_max_speed(prev_coordinate['xy'], cur_coordinate['xy'])
            print(f'prev:{prev_coordinate} \n cur:{cur_coordinate}')
            query = """
            INSERT INTO `test` (id,device_type,time_stamp,latitude,longitude,speed,expected_speed,snapped,backup_battery)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (formatted_data['id'],
                      formatted_data['device_type'],
                      formatted_data['time_stamp'],
                      cur_coordinate['xy'][0],
                      cur_coordinate['xy'][1],
                      speed,
                      expected_speed,
                      int(snapped_flag),
                      backup_battery)

            cursor.execute(query, values)
            db.commit()
            print(f'sql operation successful:{query}' % values)

        except:
            print(traceback.format_exc())
        finally:
            print('------------')
            return prev_coordinate, cur_coordinate

    else:
        print('first coordinate received')
        print('------------')
        return prev_coordinate, cur_coordinate


def threaded_client(connection):
    prev_coordinate, cur_coordinate = None, None
    db = connect_db()
    cursor = get_cursor(db)
    while True:
        try:
            data_raw = connection.recv(128)
            print(data_raw)
            data = data_raw.decode("utf-8")

            if data:
                if data[-1] == '#':
                    formatted_data = h02_data_split(data)
                    prev_coordinate, cur_coordinate = process_data(formatted_data, prev_coordinate, cur_coordinate, db, cursor)

                elif data[-1] == '@':
                    # parsing data
                    formatted_data = mobile_data_split(data)
                    prev_coordinate, cur_coordinate = process_data(formatted_data, prev_coordinate, cur_coordinate, db, cursor)

                else:
                    print('corrupted data', data)
                    break
            else:
                print('no data received')
                break
        except Exception as e:
            print(traceback.format_exc())
            logger.error(e)
            break
    connection.close()


def close_socket(s):
    s.close()


atexit.register(close_socket, s)

try:
    while True:
        try:
            socket_connection, addr = s.accept()
            print('Got connection from', addr)
            start_new_thread(threaded_client, (socket_connection,))

        except KeyboardInterrupt:
            break

        except:
            # if not db.is_connected():
            #     print('db disconnected')
            #     db = connect_db()
            print(traceback.format_exc())
except:
    print(traceback.format_exc())
finally:
    s.close()
