import codecs
import socketserver
from threading import Thread, current_thread
import time
import socket
from crccheck.crc import CrcX25
from datetime import datetime, date
from pytz import timezone
import atexit
# --- global variables ---
import gps_tracker_bw09
import process_data
import mysql.connector
import os
import geopy
import geopy.distance
import requests
import traceback

dev_ip_list = {}


def get_db_password():
    with open('/root/credentials', 'r') as f:
        db_password = f.readline()
        return db_password


def connect_db():
    db = mysql.connector.connect(user='root', password=get_db_password(), host='127.0.0.1', database='gps',
                                 auth_plugin='mysql_native_password')

    if db.is_connected():
        print('Connected to mysql')

    return db


def get_cursor(db):
    cursor = db.cursor()
    cursor.execute("SET SESSION interactive_timeout=31536000")
    cursor.execute("SET SESSION wait_timeout=31536000")
    return cursor


def calculate_checksum(data):
    data = bytearray.fromhex(data[4:-8])  # Returns a new bytearray object initialized from a string of hex numbers.
    crc = hex(CrcX25.calc(data))
    return crc


def get_speed(cur_coordinate, prev_coordinate):
    distance = geopy.distance.geodesic((cur_coordinate['xy']), (prev_coordinate['xy'])).km
    elapsed_time = (cur_coordinate['time'] - prev_coordinate['time']).seconds / 3600.0
    speed = distance / elapsed_time
    return speed


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


def proceed(formatted_data, prev_coordinate, cur_coordinate, db, cursor):
    prev_coordinate = cur_coordinate

    cur_coordinate = {'xy': (formatted_data['latitude'], formatted_data['longitude']),
                      'time': formatted_data['time_obj']
                      }

    if not prev_coordinate:

        last_coordinate_query = "select * from test2 where id = %s and time_stamp < %s order by time_stamp desc limit 1"
        last_coordinate_query_values = (formatted_data['id'], cur_coordinate['time'])
        cursor.execute(last_coordinate_query, last_coordinate_query_values)
        prev_coordinate_raw = cursor.fetchall()

        if prev_coordinate_raw:
            prev_coordinate = {'xy': (prev_coordinate_raw[0][4], prev_coordinate_raw[0][5]),
                               'time': prev_coordinate_raw[0][3]
                               }
    snapped_flag = False
    backup_battery = 0

    if formatted_data['voltage'] == 0:
        backup_battery = 1

    if prev_coordinate:
        snapped_result = get_snap([prev_coordinate['xy'], cur_coordinate['xy']])
        if snapped_result:
            cur_coordinate['xy'] = snapped_result['end_point']
            snapped_flag = True

        speed = get_speed(cur_coordinate, prev_coordinate)
        expected_speed = get_max_speed(prev_coordinate['xy'], cur_coordinate['xy'])
        print(f'prev:{prev_coordinate} \n cur:{cur_coordinate}')
        query = """
                    INSERT INTO `test2` (id,device_type,time_stamp,latitude,longitude,speed,expected_speed,snapped,backup_battery)
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

        return prev_coordinate, cur_coordinate
    else:
        print('First coordinate received')
        return prev_coordinate, cur_coordinate


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):

        prev_coordinate, cur_coordinate = None, None
        db = connect_db()
        cursor = get_cursor(db)

        equipment_id = ''

        while True:
            # --- Print device ip ---
            client_address = self.client_address[0]
            #print('{} - Incoming connection from {}'.format(time.strftime('%d.%m.%Y %H:%M:%S', time.localtime()), client_address))
            # print("\n")

            # --- Data received ---
            data = self.request.recv(76)
            data_new = data.hex()

            # data_new = "78780d0108680030320624030026cc820d0a78781f1214021a101c3bc7028d849709b3518d0014f101d6005218002b86000388220d0a78780a1344060400020006c96b0d0a"
            if data_new:
                print()
                print('{} - Incoming connection from {}'.format(time.strftime('%d.%m.%Y %H:%M:%S', time.localtime()),
                                                                self.client_address))
                print(data_new)
                #print("Response from GPS Tracker:", data_new)
            # print("Data type:", type(data_new))
            # print("Data length:", len(data_new))

            # --- make a list of received data packet ---
            data_count = data_new.count("0d0a")
            data_list = data_new.split("0d0a")[:-1]
            counter = 0
            while counter < data_count:
                data_list[counter] = data_list[counter] + "0d0a"
                counter += 1
            # print(data_list)

            for item in data_list:
                # --- Login message ---
                if item[0:4] == "7878" and item[4:6].lower() == "0d" and item[6:8].lower() == "01":
                    print("Login message")
                    #print(item)
                    start_bit = item[0:4]
                    packet_length = item[4:6]
                    protocol_no = item[6:8]
                    equipment_id = item[8:24]
                    serial_no = item[24:28]
                    error_chk = item[28:32]
                    error_chk_sum = calculate_checksum(item)
                    stop_bit = item[32:36]

                    # --- store device's IP against respective device ID ---
                    # --- check if equipment id is in the list ---
                    if dev_ip_list.get(equipment_id) is None:
                        dev_ip_list[equipment_id] = client_address
                    # --- update client address in the list for new address ---
                    if dev_ip_list.get(equipment_id) != client_address:
                        dev_ip_list[equipment_id] = client_address
                    # print("IP list: ", dev_ip_list)

                    # --- response to terminal ---
                    response_pkt = start_bit + "0501" + serial_no + error_chk_sum[2:] + stop_bit
                    response_pkt = response_pkt.lower()
                    # print("Server response:", response_pkt)

                    # --- convert response to hex ---
                    if len(response_pkt) == 20:
                        res_hex = codecs.decode(response_pkt, "hex_codec")
                        # print(res_hex)
                        self.request.send(res_hex)
                        print("Response send for GPS Tracker: ", str(response_pkt))
                # --- Status Information data ---
                elif item[0:4] == "7878" and item[4:6].lower() == "0a" and item[6:8].lower() == "13":
                    print("Status Information")
                    #print(item)
                    start_bit = item[0:4]
                    packet_length = item[4:6]
                    protocol_no = item[6:8]
                    status_info = item[8:18]
                    serial_no = item[18:22]
                    error_chk = item[22:26]
                    error_chk_sum = calculate_checksum(item)
                    stop_bit = item[26:30]

                    # --- response to terminal ---
                    response_pkt = start_bit + "0513" + serial_no + error_chk_sum[2:] + stop_bit
                    response_pkt = response_pkt.lower()
                    # print(response_pkt)

                    # --- convert response to hex ---
                    if len(response_pkt) == 20:
                        res_hex = codecs.decode(response_pkt, "hex_codec")
                        # print(res_hex)
                        self.request.send(res_hex)
                        print("Response send for GPS Tracker: ", str(response_pkt))
                # --- Location data ---
                elif item[0:4] == "7878" and (item[4:6].lower() == "1f" or item[4:6].lower() == "21") and item[6:8].lower() == "12":
                    print("Location data")
                    #print(item)
                    # --- get ID of which equipment is sending the data ---
                    for id, ip in dev_ip_list.items():
                        # print("location data come from IP: ", ip)
                        # print("location data come from client address: ", client_address)
                        if ip == client_address:
                            # print("location data come from ID: ", id)
                            # print("item: ", item)
                            equipment_id = id
                            formatted_data = process_data.bw09(item, equipment_id)
                            print(formatted_data)
                            print("---------------------------")
                            try:
                                prev_coordinate, cur_coordinate = proceed(formatted_data, prev_coordinate, cur_coordinate, db, cursor)
                            except:
                                print(traceback.format_exc())
                            # # --- Create an object for processing data(BW09 or BW906) ---
                            # data_processing = Query()
                            # # --- Pass received data stream as an argument of function device_data_process ---
                            # data_processing.device_data_process(item, equipment_id)
                            # del data_processing


                # --- Alarm data ---
                elif item[0:4] == "7878" and item[4:6].lower() == "25" and item[6:8].lower() == "16":
                    print("Alarm data")
                    #print(item)
                    # --- get ID of which equipment is sending the data ---
                    for id, ip in dev_ip_list.items():
                        # print("location data come from IP: ", ip)
                        # print("location data come from client address: ", client_address)
                        if ip == client_address:
                            # print("location data come from ID: ", id)
                            # print("item: ", item)
                            equipment_id = id

                            # # --- Create an object for processing data(BW09 or BW906) ---
                            # data_processing = Query()
                            # # --- Pass received data stream as an argument of function device_data_process ---
                            # data_processing.device_data_process(item, equipment_id)
                            # del data_processing


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


def shutdown_and_close(s):
    s.shutdown()
    s.server_close()


if __name__ == "__main__":

    host = ''
    port = 1001
    print('Host ip: ', host, '\nPort: ', port)
    server = ThreadedTCPServer((host, port), ThreadedTCPRequestHandler)
    server.allow_reuse_address = True
    atexit.register(shutdown_and_close, server)
    print("Server loop running in thread:\n")
    try:
        server.serve_forever()
    except:
        pass
    finally:
        shutdown_and_close(server)

