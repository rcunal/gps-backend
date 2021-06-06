from datetime import datetime


def bw09(client_data, equipment_id):

    # --- status information ---
    if client_data[4:6].lower() == "0a":
        print("status information")
        terminal_info = bin(int(client_data[8:10], 16))[2:].zfill(8)
        if terminal_info[0:1] == '0':
            oil_electricity_connected = True
        else:
            oil_electricity_connected = False
        if terminal_info[1:2] == '0':
            gps_tracking_on = False
        else:
            gps_tracking_on = True
        if terminal_info[5:6] == '0':
            battery_charging = False
        else:
            battery_charging = True
        if terminal_info[6:7] == '0':
            acc = False
        else:
            acc = True
        terminal_battery_info = int(client_data[8:10], 16)
        gsm_strength = int(client_data[8:10], 16)

    # --- location data without ADC value ---
    if client_data[4:6].lower() == "1f":
        print("location data without fuel value")
        device_year = int(client_data[8:10], 16)
        device_month = int(client_data[10:12], 16)
        device_day = int(client_data[12:14], 16)
        device_hour = int(client_data[14:16], 16)
        device_minute = int(client_data[16:18], 16)
        device_second = int(client_data[18:20], 16)
        time_date = str(device_year) + "-" + str(device_month) + "-" + str(device_day) + " " + str(device_hour) + ":" + str(
            device_minute) + ":" + str(device_second)
        time_date_obj = datetime.strptime(time_date, "%y-%m-%d %H:%M:%S")
        # print("date: ", str(device_year) + "-" + str(device_month) + "-" + str(device_day) + " " + str(device_hour) + ":" + str(device_minute) + ":" + str(device_second))
        latitude = int(client_data[22:30], 16)
        latitude = float(latitude) / 30000
        latitude = round(float(latitude / 60), 6)
        longitude = int(client_data[30:38], 16)
        longitude = float(longitude) / 30000
        longitude = round(float(longitude / 60), 6)
        print("lat: ", latitude, " lon: ", longitude)
        speed = int(client_data[38:40], 16)
        course_status = bin(int(client_data[40:44], 16))[2:].zfill(16)
        voltage = int(course_status[:10], 2) / 10
        acc = course_status[0:1]
        input2 = course_status[1:2]
        real_time_gps = course_status[2:3]
        gps_positioning = course_status[3:4]
        east_or_west_longitude = course_status[4:5]
        south_or_north_longitude = course_status[5:6]
        course = int(course_status[6:].zfill(8), 2)
        mcc = int(client_data[44:48], 16)
        mnc = int(client_data[48:50], 16)
        lac = int(client_data[50:54], 16)
        cell_id = int(client_data[54:60], 16)

        # message_all = {"time_date": "2019-10-31 16:11:24", "cell_id": "0868003032433422", "lat": "23.774352", "lon": "90.366212", "bearing": "15", "speed": "1", "battery": "65"}
        message_all = {"time_stamp": time_date, 'id': str(equipment_id), 'latitude': str(latitude), 'longitude': str(longitude),
                       "bearing": course, 'speed': str(speed), 'time_obj': time_date_obj, 'device_type': 'bw09', 'voltage': voltage}
        return message_all
