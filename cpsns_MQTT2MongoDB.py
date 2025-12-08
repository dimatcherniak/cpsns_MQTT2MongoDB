import math
from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import time
from datetime import datetime, timezone
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

'''
Recursively compares two JSON-like Python objects (dict or list) and returns a list of differences.
Reports missing keys in either object.
Detects list length mismatches and element differences.
Highlights value differences at specific paths.
The path parameter tracks the current location within nested structures for clear reporting.
'''
def compare_json(obj1, obj2, path=""):
    """Recursively compare two JSON-like objects and return a list of differences with detailed paths."""
    diffs = []
    if isinstance(obj1, dict) and isinstance(obj2, dict):
        for key in obj1.keys() | obj2.keys():
            new_path = f"{path}.{key}" if path else key
            if key not in obj2:
                diffs.append(f"Key '{new_path}' missing in second JSON")
            elif key not in obj1:
                diffs.append(f"Key '{new_path}' missing in first JSON")
            else:
                diffs.extend(compare_json(obj1[key], obj2[key], new_path))
    elif isinstance(obj1, list) and isinstance(obj2, list):
        for i, (v1, v2) in enumerate(zip(obj1, obj2)):
            diffs.extend(compare_json(v1, v2, f"{path}[{i}]"))
        if len(obj1) != len(obj2):
            diffs.append(f"List length mismatch at '{path}'")
    else:
        if obj1 != obj2:
            diffs.append(f"Value mismatch at '{path}': {obj1} != {obj2}")
    return diffs

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
    stored_json_metadata = []
    while True:
        msg = msgQueue.get() # <-- blocks indefinitely until an item is available

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

        # 2a Create data collection. Its name comes from the MQTT topic
        collection_name = DAQ
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
            print(f"Collection '{collection_name}' created.")
            # index
            db[collection_name].create_index([("timestamp",1)])
            print("Index created on 'timestamp'.")

        # 2b Create metadata collection.
        metadata_collection_name = f"{collection_name}_metadata"
        # create if it does not exist
        if metadata_collection_name not in db.list_collection_names():
            db.create_collection(metadata_collection_name)
            print(f"Collection '{metadata_collection_name}' created.")
            # index
            db[metadata_collection_name].create_index([("UTCAtNewBatch",1)], unique=True) # or use upsert?
            print("Index created on 'UTCAtNewBatch'.")        

        # 3. Add the metadata to the metadata colletion
        if bIsMetadata:
            bIsMetadataRead = True
            current_json_metadata = json.loads(msg.payload)
            # Get the sampling frequency from the metadata
            Fs = current_json_metadata["DataChunk"]["Fs"]

            if stored_json_metadata != current_json_metadata:
                # Add a document to the metadata collection
                # The idea is that for each data collection, like e.g., "3053-B-120_sn_105283"
                # I will have a corresponding "3053-B-120_sn_105283_metadata" collection. This collection will have documents that
                # 1. Have a field "DataCollectionName", e.g., "3053-B-120_sn_105283"
                # 2. Have a UNIQUE INDEXED field "UTCAtDAQStart" --> "UTCAtNewBatch", that contains the timestamp (actually taken from the metadata, ["DataChunk"]["UTCAtDAQStart"])
                # 3. Have a field "Metadata", that contains the latest metadata. "UTCAtDAQStart" is a part of the metadata

                # Note:
                # See AddMetadataCollectiontoDB.ipynb
                # For the time being (Nov-2025), whilst the DTU test is running, I won't dare to change the code
                # After the test, this should be done in this code
                # For the time being, I MANUALLY added a "***_metadata" collection to the database
                # See **** ---> AddMetadataCollectiontoDB.ipynb

                document = {
                    "DataCollectionName": collection_name,
                    #"UTCAtDAQStart": datetime.fromisoformat(jsonMetadata["DataChunk"]["UTCAtDAQStart"]),
                    "UTCAtNewBatch": datetime.now(timezone.utc),
                    "Metadata": current_json_metadata
                }

                # Before adding to the collection, let's check if there is already a document with the same index
                # Add to the collection    
                try:
                    db[metadata_collection_name].insert_one(document) # or .update_one(..., upsert=True)?
                    print(f"A document with index {document['UTCAtNewBatch']} inserted to the collection {metadata_collection_name}.")
                    print("Reason:")
                    if not stored_json_metadata:
                        print("This function block is just started --> expected a gap in the recordings.")
                    else:
                        print("While running, the metadata has changed. The detected different is:")
                        diff = compare_json(current_json_metadata, stored_json_metadata, path="")
                        print(json.dumps(diff,indent=2))

                except DuplicateKeyError:
                    print(f"Duplicated key: a document with index {document['UTCAtNewBatch']} already exist in the collection named {metadata_collection_name}", file=sys.stderr)
                else:
                    stored_json_metadata = current_json_metadata

        # 5. If data message, prepare the document and store it
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
            # simple statistics on the data
            data_array = np.frombuffer(data_bytes, dtype=array_dtype).reshape(array_shape)
            rms = np.sqrt(np.mean(data_array**2, axis=0))
            print(f"rms: {rms}", file=sys.stderr)
            document = {
                "timestamp": utcTimeStamp,
                "sampling_rate": Fs,            # redundant, can be found in the metadata collection. Left for compatibility
                "data_shape": array_shape,       # helpful for reconstructing
                "data_dtype": array_dtype,        # e.g., 'float32'
                "rms": rms.tolist(),
                "data": bson.Binary(data_bytes),
                "samples_from_DAQ_start": samples_from_start # introduced 8-Dec-2025
            }
            db[collection_name].insert_one(document)
            print(f'Document is inserted to collection {collection_name} with timestamp {utcTimeStamp}!')

if __name__ == "__main__":
    main()
