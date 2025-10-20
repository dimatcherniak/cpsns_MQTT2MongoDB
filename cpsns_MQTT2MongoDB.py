import math
from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import time
from datetime import datetime
import struct
import queue
import argparse
import json
import bson
import sys
import os
import numpy as np

from pymongo import MongoClient


PRIVATE_CONFIG_FILE_DEFAULT = "private_config.json" # locate this file in the private folder and chmod 600 it.
PUBLIC_CONFIG_FILE_DEFAULT = "public_config.json"   # this file can be located in a public folder 

msgQueue = queue.Queue()

def on_connect_in(mqttc_in, userdata, flags, rc, properties=None):
    global json_config_public, json_config_private
    print("MQTT_IN: Connected with response code %s" % rc)
    for topic in json_config_public["MQTT_IN"]["TopicsToSubscribe"]:
        print(f"MQTT_IN: Subscribing to the topic {topic}...")
        mqttc_in.subscribe(topic, qos=json_config_private["MQTT_IN"]["QoS"])

def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print("Subscribed. Message: " + str(msg))

def on_message(client, userdata, msg):
    #print(f"on_message: Topic: {msg.topic}")
    msgQueue.put(msg)

def main():
    global json_config_private, json_config_public

    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="Write the description here...")
    parser.add_argument('--config_private', type=str, help='Specify the JSON configuration file for PRIVATE data. Defaults to ' + PRIVATE_CONFIG_FILE_DEFAULT, default=PRIVATE_CONFIG_FILE_DEFAULT)
    parser.add_argument('--config_public', type=str, help='Specify the JSON configuration file for PUBLIC data. Defaults to ' + PUBLIC_CONFIG_FILE_DEFAULT, default=PUBLIC_CONFIG_FILE_DEFAULT)

    # Parse the arguments
    args = parser.parse_args()

    # Name of the configuration file
    strConfigFile = args.config_private
    # Read the configuration file
    print(f"Reading private configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config_private = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # Name of the configuration file
    strConfigFile = args.config_public
    # Read the configuration file
    print(f"Reading public configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config_public = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # Parameters check
    if len(json_config_public["MQTT_IN"]["TopicsToSubscribe"]) > 1:
        print(f"Error: Multiple topics are not yet supported.", file=sys.stderr)    
        sys.exit(1)

    # MQTT_IN stuff
    mqttc_in = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    #mqttc_in = MQTTClient()

    # Set username and password
    if json_config_private["MQTT_IN"]["userId"] != "":
        mqttc_in.username_pw_set(json_config_private["MQTT_IN"]["userId"], json_config_private["MQTT_IN"]["password"])

    mqttc_in.on_connect = on_connect_in
    mqttc_in.on_message = on_message
    mqttc_in.on_subscribe = on_subscribe
    mqttc_in.connect(json_config_private["MQTT_IN"]["host"], json_config_private["MQTT_IN"]["port"], 60) # we subscribe to the topics in on_connect callback

    mqttc_in.loop_start()

    # MongoDB
    # Establish connection to MongoDB 
    server_url = json_config_private["MongoDB"]["host"]
    port = json_config_private["MongoDB"]["port"]
    username = json_config_private["MongoDB"]["username"]
    password = json_config_private["MongoDB"]["password"]
    database_name = json_config_private["MongoDB"]["database_name"]

    # Create MongoDB connection string
    uri = f"mongodb://{username}:{password}@{server_url}:{port}/{database_name}?tls=true"

    # Establish connection
    client = MongoClient(uri)

    # Check if operational
    try:
        client.admin.command("ping")
        print("MongoDB connection is operational.")
    except ConnectionFailure as e:
        print(f"MongoDB connection failed: {e}", file=sys.stderr)
        sys.exit(1)    
    
    db = client[database_name]

    bIsMetadataRead = False
    while True:
        msg = msgQueue.get()

        # Business logic
        # 1. Analyse the topic
        topic = msg.topic
        substrings = topic.split('/')
        DAQ = substrings[2]
        bIsMetadata = True
        if substrings[-1] == "data":
            bIsMetadata = False
        elif substrings[-1] == "metadata":
            bIsMetadata = True
        else:
            printf(f"Unknown last topic component: {substrings[-1]}. Skip the message!", file=sys.stderr)
            continue

        # 2. Generate collection name from the topic
        collectionName = DAQ
        if collectionName not in db.list_collection_names():
            db.create_collection(collectionName)
            print(f"Collection '{collectionName}' created.")

        # 3. Get the sampling frequency from the metadata
        if not bIsMetadataRead and bIsMetadata:
            bIsMetadataRead = True
            payload = json.loads(msg.payload)
            Fs = payload["DataChunk"]["Fs"]

        # 4. If data message, prepare the document and store it
        if not bIsMetadata and bIsMetadataRead:
            # Fields: 'timestamp', 'sampling_rate', 'data_shape', 'data_dtype', 'data'
            # Parse the data...
            payload = msg.payload
            # Header format (must match the one used in encoding)
            header_format = ">HHQQQHH"
            header_size = struct.calcsize(header_format)
            header_bytes = payload[:header_size]
            data_bytes = payload[header_size:]
            # Unpack the header fields
            descriptor_length, version, seconds, nanoseconds, samples_from_start, elem_size, columns = struct.unpack(header_format, header_bytes)
            total_seconds = seconds + nanoseconds / 1e9
            utcTimeStamp = datetime.utcfromtimestamp(total_seconds)            
            rows = int((len(payload)-header_size)/elem_size/columns)
            array_shape = (rows, columns)
            array_dtype = f'float{8*elem_size}'
            document = {
                "timestamp": utcTimeStamp,
                "sampling_rate": Fs,
                "data_shape": array_shape,       # helpful for reconstructing
                "data_dtype": array_dtype,        # e.g., 'float32'
                "data": bson.Binary(data_bytes)
            }
            db[collectionName].insert_one(document)
            print(f'Document is inserted to collection {collectionName} with timestamp {utcTimeStamp}!')

        # TODO: new collection if data interrupted (e.g., new metadata or new UTCAtDAQStart)
        '''
        if bIsMetadata:
            payload = json.loads(msg.payload)
            collectionName = f'{DAQ}_Started_at_{payload["DataChunk"]["UTCAtDAQStart"]}'
            if collectionName not in db.list_collection_names():
                db.create_collection(collectionName)
                print(f"Collection '{collectionName}' created.")
        '''

if __name__ == "__main__":
    main()
