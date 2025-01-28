import asyncio
import RPi.GPIO as GPIO
import logging
import paho.mqtt.client as mqtt
import threading
import time
from datetime import datetime

from aiohttp import ClientConnectorError, ClientError, ClientSession

from nettigo_air_monitor import (
    ApiError,
    AuthFailedError,
    ConnectionOptions,
    InvalidSensorDataError,
    NettigoAirMonitor,
)

logging.basicConfig(level=logging.DEBUG)

pinNum = 7  # Change pin number as you wish
GPIO.setmode(GPIO.BOARD)  # Check if you are looking at pins by BOARD or BCM 
GPIO.setup(pinNum, GPIO.OUT)
GPIO.output(pinNum, GPIO.LOW)

HOST = "192.168.0.14"
USERNAME = "Milenkovic"
PASSWORD = "vukvucko2013"

global cnt
global cnt_loss
global pm10
global pm25

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        # Subscribe to the in_topic
        client.subscribe("outTopic")
    else:
        print("Connection failed with code", rc)

# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    print(f"Received message: {msg.payload.decode()} on topic {msg.topic}")

# Setup MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect("localhost", 1883, 60)

# Start the loop
client.loop_start()

# Control flag for running the main loop
run_main_loop = True

def time_control():
    global run_main_loop
    while True:
        current_hour = datetime.now().hour
        if 7 <= current_hour < 22:
            run_main_loop = True
        else:
            run_main_loop = False
            GPIO.output(pinNum, GPIO.LOW)  # Turn off the GPIO pin
        time.sleep(60)  # Check time every minute

async def main():
    global cnt 
    cnt = 1
    global cnt_loss 
    cnt_loss = 0
    global pm10
    pm10 = 0
    global pm25
    pm25 = 0

    options = ConnectionOptions(host=HOST, username=USERNAME, password=PASSWORD)

    async with ClientSession() as websession:
        nam = await NettigoAirMonitor.create(websession, options)

        while True:
            if run_main_loop:
                try:
                    data = await nam.async_update()
                    mac = await nam.async_get_mac_address()
                except (
                    ApiError,
                    AuthFailedError,
                    ClientConnectorError,
                    ClientError,
                    InvalidSensorDataError,
                    asyncio.TimeoutError,
                ) as error:
                    print(f"Error: {error}")
                else:
                    print(f"Auth enabled: {nam.auth_enabled}")
                    print(f"Firmware: {nam.software_version}")
                    print(f"MAC address: {mac}")
                    print(f"Data: {data}")
                    print(f"Data PM10: {data.sds011_p1}")
                    print(f"Data PM2.5: {data.sds011_p2}")

                    if data.sds011_p1 is not None:
                        pm10 = pm10 + data.sds011_p1
                        pm25 = pm25 + data.sds011_p2
                    else:
                        pm10 = 0
                        pm25 = 0
                        cnt_loss = cnt_loss + 1

                    print("PM10 avg: ", pm10/cnt)
                    print("PM2.5 avg: ", pm25/cnt)
                    if (cnt % 8 == 0):
                        if (cnt_loss == 0):
                            pm10_mean = pm10/8
                            pm25_mean = pm25/8
                        elif (cnt_loss == 8):
                            pm10_mean = 0
                            pm25_mean = 0
                        else:
                            pm10_mean = pm10/(8-cnt_loss)
                            pm25_mean = pm25/(8-cnt_loss)
                        pm10 = 0
                        pm25 = 0
                        cnt_loss = 0
                        if(pm10_mean > 40 or pm25_mean > 20):
                            print("High level of pollution!")
                            GPIO.output(pinNum, GPIO.HIGH)  # Set GPIO pin to ON
                            client.publish("inTopic", "1")
                        else:
                            print("Low level of pollution.")
                            GPIO.output(pinNum, GPIO.LOW)
                            client.publish("inTopic", "0")
                        cnt = 1
                    else: 
                        cnt = cnt + 1

                # Delay for a specified number of seconds before fetching data again
                await asyncio.sleep(145)  # Adjust the delay time as needed - now it is 145 seconds 
            else:
                print("Outside operating hours. Sleeping...")
                await asyncio.sleep(60)

if __name__ == "__main__":
    # Start the time control thread
    time_thread = threading.Thread(target=time_control, daemon=True)
    time_thread.start()

    # Run the main asyncio loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())

