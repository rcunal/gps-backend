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

"""" 
TODO:

"""

with open(os.path.dirname(os.path.abspath(__file__)), 'r') as f:
    db_password = f.readline()




logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

db = mysql.connector.connect(user='root', password=db_password, host='127.0.0.1', database='gps',
                             auth_plugin='mysql_native_password')

if db.is_connected():
    print('Connected to mysql')
cursor = db.cursor()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

port = 1000
s.bind(('', port))
print("socket binded to %s" % (port))
s.listen(5)
print("socket is listening")

ThreadCount = 0


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
        #print(traceback.format_exc())
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
    hhmmss = splitted_data[3]
    ddmmyy = splitted_data[11]
    time_obj = datetime.strptime(hhmmss + ddmmyy, "%H%M%S%d%m%y")
    timestamp = time_obj.strftime('%Y-%m-%d %H:%M:%S')
    latitude_s = splitted_data[5]
    D = splitted_data[6]
    longitude_s = splitted_data[7]
    G = splitted_data[8]
    geo_str = latitude_s[:2] + ' ' + latitude_s[2:] + "' " + D + ' ' + longitude_s[
                                                                       :3] + ' ' + longitude_s[
                                                                                   3:] + "' " + G
    p = geopy.point.Point(geo_str)

    formatted_result = {'id': imei,
                        'device_type': 'h02',
                        'time_obj': time_obj,
                        'time_stamp': timestamp,
                        'latitude': p.latitude,
                        'longitude': p.longitude}

    return formatted_result


def mobile_data_split(data):
    splitted_data = data.split(',')
    logger.info(f'data:{splitted_data}')


def get_speed(cur_coordinate, prev_coordinate):
    distance = geopy.distance.geodesic((cur_coordinate['xy']), (prev_coordinate['xy'])).km

    elapsed_time = (cur_coordinate['time'] - prev_coordinate['time']).seconds / 3600.0
    print(f'distance:{distance}, time:{elapsed_time}')
    speed = distance / elapsed_time
    return speed


def threaded_client(connection, db):
    prev_coordinate, cur_coordinate = None, None

    while True:
        try:
            data_raw = connection.recv(128)
            if data_raw:
                logger.info(data_raw)
            data = data_raw.decode("utf-8")

            if data:
                if data[-1] == '#':
                    formatted_data = h02_data_split(data)
                    prev_coordinate = cur_coordinate

                    cur_coordinate = {'xy': (formatted_data['latitude'], formatted_data['longitude']),
                                      'time': formatted_data['time_obj']
                                      }
                    # ilk veri,
                    # ya da yeni bağlantı
                    if not prev_coordinate:

                        last_coordinate_sql = "select * from test where id = '{}' and time_stamp < '{}' order by time_stamp desc limit 1".format(
                            formatted_data['id'], cur_coordinate['time'])
                        cursor.execute(last_coordinate_sql)
                        prev_coordinate_raw = cursor.fetchall()

                        # bağlantı kopmuş, yeniden bağlanmış
                        if prev_coordinate_raw:
                            prev_coordinate = {'xy': (prev_coordinate_raw[0][3], prev_coordinate_raw[0][4]),
                                               'time': prev_coordinate_raw[0][2]
                                               }
                            # elapsed_date = (cur_coordinate['time'] - prev_coordinate['time'])
                            # elapsed_time = elapsed_date.seconds

                            # # yeni eski arasında 30dk dan fazla geçmiş
                            # if elapsed_time > 1800 and elapsed_date.days >= 0:
                            #     cur_coordinate['lap'] = prev_coordinate['lap'] + 1
                            # # eski yeni arasında 30 dk dan az geçmiş
                            # elif elapsed_date.days >= 0:
                            #     cur_coordinate['lap'] = prev_coordinate['lap']
                            # # cur_coordinate çok eski veri gelmiş
                            # else:
                            #     pass
                        # else:
                        #     cur_coordinate['lap'] = 1
                    # else:
                    #     cur_coordinate['lap'] = prev_coordinate['lap']

                    snapped_flag = False
                    expected_speed = None

                    if prev_coordinate:
                        try:
                            snapped_result = get_snap([prev_coordinate['xy'], cur_coordinate['xy']])
                            if snapped_result:
                                cur_coordinate['xy'] = snapped_result['end_point']
                                snapped_flag = True
                                expected_speed = snapped_result['speed']

                            speed = get_speed(cur_coordinate, prev_coordinate)
                            print(f'prev:{prev_coordinate} \n cur:{cur_coordinate}')
                            if expected_speed:
                                sql = "insert into test (id,device_type,time_stamp,latitude,longitude,speed," \
                                      "expected_speed,snapped) values ('{}','h02','{}','{}','{}','{}','{}','{}')" \
                                    .format(formatted_data['id'], formatted_data['time_stamp'],
                                            cur_coordinate['xy'][0],
                                            cur_coordinate['xy'][1], speed, expected_speed,
                                            int(snapped_flag))
                            else:
                                sql = f"insert into test (id,device_type,time_stamp,latitude,longitude,speed,snapped) " \
                                      f"values ('{formatted_data['id']}','h02','{formatted_data['time_stamp']}','{cur_coordinate['xy'][0]}'," \
                                      f"'{cur_coordinate['xy'][1]}','{speed}','{int(snapped_flag)}')"

                            # sql2 = "insert into h02_snapped (id,time_stamp,latitude,longitude,speed) values ('{}', '{}', '{}', '{}', '{}')".format(
                            #     imei, timestamp, cur_coordinate['xy'][0], cur_coordinate['xy'][1], speed)
                            cursor.execute(sql)
                            db.commit()
                            print(f'sql operation successful:{sql}')

                        except Exception as e:
                            print('exception', e)
                            print(traceback.format_exc())
                        print('------------')

                    else:
                        print('first coordinate received')




                elif data[-1] == '@':
                    # parsing data
                    # formatted_data = mobile_data_split(data)
                    print('mobile connection is not ready yet')
                    pass

                    # splitted_data = data.split(',')
                    # logger.info(f'data:{splitted_data}')
                    # id, time_stamp = splitted_data[0], splitted_data[1]
                    #
                    # # time_stamp -> timeobj
                    # prev_coordinate = cur_coordinate
                    # cur_coordinate = {'xy': [splitted_data[2], splitted_data[3]],
                    #                   'time': timeobj
                    #                   }
                    #
                    # try:
                    #     snapped_flag = False
                    #     distance = geopy.distance.geodesic((cur_coordinate['xy']), (prev_coordinate['xy'])).km
                    #     elapsed_time = (cur_coordinate['time'] - prev_coordinate['time']).seconds / 3600.0
                    #     speed = distance / elapsed_time
                    #     expected_speed = -1  # db ye null atmaya bak
                    #     if speed > 3:
                    #         result = get_snap([prev_coordinate['xy'], cur_coordinate['xy']])
                    #         if result:
                    #             snapped_flag = True
                    #             cur_coordinate['xy'] = result['end_point']
                    #             distance = geopy.distance.geodesic((cur_coordinate['xy']), (prev_coordinate['xy'])).km
                    #             speed = distance / elapsed_time
                    #             expected_speed = (result['distance'] / result['duration']) * 3.6
                    #
                    #     sql2 = "insert into mobile_devices (id,time_stamp,latitude,longitude,speed, expected_speed) values ('{}', '{}', '{}', '{}', '{}', '{}')".format(
                    #         imei, timestamp, cur_coordinate['xy'][0], cur_coordinate['xy'][1], speed, expected_speed)
                    #     cursor.execute(sql2)
                    #
                    # except Exception as e:
                    #     print('exception:', e)

                else:
                    print('corrupted data', data)
                    break
            else:
                print('no data received')
                break
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            logger.error(e)
            break
    connection.close()


try:
    while True:
        try:
            socket_connection, addr = s.accept()
            print('Got connection from', addr)
            start_new_thread(threaded_client, (socket_connection, db,))
            # print('Active connection count: ' + str(ThreadCount))
        except Exception as e:
            print(e)
            print(traceback.format_exc())
except e:
    print('exception:', e)
    print(traceback.format_exc())
finally:
    s.close()
