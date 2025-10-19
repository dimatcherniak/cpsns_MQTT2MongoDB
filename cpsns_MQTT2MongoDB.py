import math
from paho.mqtt.client import Client as MQTTClient
#from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import time
import queue
import argparse
import json
import sys
import os
import uuid
import re
import numpy as np

PRIVATE_CONFIG_FILE_DEFAULT = "private_config.json" # locate this file in the private folder and chmod 600 it.
PUBLIC_CONFIG_FILE_DEFAULT = "public_config.json"   # this file can be located in a public folder 

bReading = False
bWriting = False

jobs = {}

# Replaces the subtopics of the topic by the strings in the list
def replace_subtopics(topic, replacements):
    subtopics = topic.split('/')
    for i in range(min(len(subtopics), len(replacements))):
        if replacements[i]:
            subtopics[i] = replacements[i]
    return '/'.join(subtopics)

def on_publish(client, userdata, mid):
    print(f"Message {mid} published.")

def on_connect_in(mqttc_in, userdata, flags, rc, properties=None):
    global json_config_public
    print("MQTT_IN: Connected with response code %s" % rc)
    for topic in json_config_public["MQTT_IN"]["TopicsToSubscribe"]:
        print(f"MQTT_IN: Subscribing to the topic {topic}...")
        mqttc_in.subscribe(topic, qos=json_config_public["MQTT_IN"]["QoS"])

def on_connect_out(mqttc_in, userdata, flags, rc, properties=None):
    print("MQTT_OUT: Connected with response code %s" % rc)

def on_subscribe(self, mqttc_in, userdata, msg):
    print("Subscribed. Message: " + str(msg))

def on_message(client, userdata, msg):
    global bReading, bWriting
    global jobs

    print(f"Topic: {msg.topic}\nPayload:\n{msg.payload}")

    while bReading:        
        time.sleep(0.01) # make the thread sleep
    bWriting = True

    # Generate a "job" and add it to the jobs
    job = {"State": "New", "Topic": msg.topic, "Payload": msg.payload}
    jobs[uuid.uuid4()] = job

    bWriting = False

# Case specific functions:
def replace_result_in_UCI_file(filename, str_guid):
    """
    Replaces the word 'result' in the line that contains 'GO SPREADSHEET FILE =' with a new word.
    Args:
        filename (str): Path to the text file.
        str_guid (str): The word to replace 'result' with.
    """
    updated_lines = []

    with open(filename, 'r') as file:
        for line in file:
            if "GO SPREADSHEET FILE =" in line:
                # Split at the equals sign and reconstruct
                parts = line.split('=')
                if len(parts) == 2:
                    line = parts[0] + '= ' + str_guid + '\n'
            updated_lines.append(line)

    with open(filename, 'w') as file:
        file.writelines(updated_lines)

def replace_entry_in_UCI_file(input_file, output_file, new_filename):
    """
    Replaces the file name in the last 'READ PERMAS FILE =' line with a new file name.

    Parameters:
        input_file (str): Path to the input text file.
        output_file (str): Path to write the updated file.
        new_filename (str): New file name to insert in the last matching line.
    """
    target_prefix = "    READ PERMAS FILE = "
    
    with open(input_file, "r") as f:
        lines = f.readlines()
    
    # Find index of the last matching line
    last_index = -1
    for i, line in enumerate(lines):
        if line.startswith(target_prefix):
            last_index = i
    
    # Replace if found
    if last_index != -1:
        lines[last_index] = f"{target_prefix}{new_filename}\n"

    # Write updated lines to the output file
    with open(output_file, "w") as f:
        f.writelines(lines)

def replace_in_OMADAT_file(input_file, output_file, new_values):
    """
    Replaces the values after '& 0.33333' in the first N matching lines of a file.

    Parameters:
        input_file (str): Path to the input file.
        output_file (str): Path to save the modified file.
        new_values (list of float): Values to replace the existing ones.
    """
    pattern = re.compile(r'^(\s*&\s*0\.33333\s+)([0-9.+\-Ee]+)')
    replacements = [f"{v:.6E}" for v in new_values]

    with open(input_file, "r") as f:
        lines = f.readlines()

    updated_lines = []
    match_count = 0

    for line in lines:
        match = pattern.match(line)
        if match and match_count < len(replacements):
            prefix = match.group(1)
            new_line = prefix + replacements[match_count] + "\n"
            updated_lines.append(new_line)
            match_count += 1
        else:
            updated_lines.append(line)

    with open(output_file, "w") as f:
        f.writelines(updated_lines)

def StartExecution(guid, payload):
    global json_config_public
    byte_string = payload
    strr = byte_string.decode('utf-8')
    data = json.loads(strr)
    f1 = data["f1"]
    f2 = data["f2"]
    f3 = data["f3"]

    # opt DAT file
    strOPTDATname = str(guid)+"_opt.dat"
    #strCommand = "cp oma_opt_with_tipmass.dat " + strOPTDATname
    strCommand = "cp " + json_config_public["PERMAS"]["dat_3"] + " " + strOPTDATname
    os.system(strCommand)
    replace_in_OMADAT_file(input_file=strOPTDATname, output_file=strOPTDATname, new_values=[f1, f2, f3])

    # UCI file 
    # Replace in the "RESULT" section
    strUCIname = str(guid)+".uci"
    #strCommand = "cp beam_opt_with_tipmass.uci " + strUCIname
    strCommand = "cp " + json_config_public["PERMAS"]["uci"] + " " + strUCIname
    os.system(strCommand)
    replace_result_in_UCI_file(strUCIname, str(guid))
    # Replace in the "READ PERMAS FILE" section
    replace_entry_in_UCI_file(strUCIname, strUCIname, strOPTDATname)

    #strCommand = "permasEDU " + strUCIname + " 1> /dev/null &" # run silently
    strCommand = "permasEDU " + strUCIname # ren verbously
    os.system(strCommand)

def ReadDesignVariables(filename):
    xd = np.genfromtxt(filename, delimiter=';', dtype=np.float64, skip_header=1)
    return xd[-1, 1], xd[-1, 2], 1.e6 * xd[-1, 3], int(xd[-1, 0])

def CleanupFiles(guid):
    # Remove the files
    strFiles = str(guid) + "*.*"
    strCommand = "rm -f " + strFiles
    os.system(strCommand)

# Here: all three frequencies must present and not be NaN
def IsJobValid(payload):
    """
    Checks if the given job is valid.   
    Args:
        job -- tuple
    Returns:
        bValid (bool): True if the job is valid, False otherwise
        strReason (str): Explanation if invalid, or "Valid" if it's fine
    """
    # convert the payload to JSON
    try:
        byte_string = payload
        strr = byte_string.decode('utf-8')
        data = json.loads(strr)
    except Exception as e:
        return False, repr(e)

    try:
        f1 = data["f1"]
    except Exception as e:
        return False, "'f1' is not found"
    try:
        f2 = data["f2"]
    except Exception as e:
        return False, "'f2' is not found"
    try:
        f3 = data["f3"]
    except Exception as e:
        return False, "'f3' is not found"
    
    if math.isnan(f1):
        return False, "'f1' is NaN"
    if math.isnan(f2):
        return False, "'f2' is NaN"
    if math.isnan(f3):
        return False, "'f3' is NaN"

    return True, "Valid"


def main():
    global json_config_private, json_config_public
    global bReading, bWriting
    global jobs

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

    # MQTT_IN stuff
    #mqttc_in = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    mqttc_in = MQTTClient()

    # Set username and password
    if json_config_private["MQTT_IN"]["userId"] != "":
        mqttc_in.username_pw_set(json_config_private["MQTT_IN"]["userId"], json_config_private["MQTT_IN"]["password"])

    mqttc_in.on_connect = on_connect_in
    mqttc_in.on_message = on_message
    mqttc_in.on_subscribe = on_subscribe
    mqttc_in.connect(json_config_private["MQTT_IN"]["host"], json_config_private["MQTT_IN"]["port"], 60) # we subscribe to the topics in on_connect callback

    mqttc_in.loop_start()
    # MQTT_IN done

    # MQTT_OUT stuff
    #mqttc_out = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    mqttc_out = MQTTClient()

    # Set username and password
    if json_config_private["MQTT_OUT"]["userId"] != "":
        mqttc_out.username_pw_set(json_config_private["MQTT_OUT"]["userId"], json_config_private["MQTT_OUT"]["password"])

    mqttc_out.on_connect = on_connect_out
    mqttc_out.on_publish = on_publish
    mqttc_out.connect(json_config_private["MQTT_OUT"]["host"], json_config_private["MQTT_OUT"]["port"], 60) 

    mqttc_out.loop_start()
    # MQTT_OUT done

    while True:
        time.sleep(0.1)

        while bWriting:        
            time.sleep(0.01) # make the thread sleep
        bReading = True

        # Check the jobs the jobs
        for key, value in jobs.items():
            if value["State"] == "New":
                print(f"New job found. Topic = {value['Topic']}")
                bValid, strReason = IsJobValid(value["Payload"])
                if bValid:
                    print('Job is valid. Start the execution...')
                    value["State"] = "Valid"
                else:
                    print(f'Job is invalid. Reason: {strReason}')
                    value["State"] = "Invalid"

                if value["State"] == "Valid":
                    # Execute
                    StartExecution(key, value["Payload"])
                    value["State"] = "Executing"
                    print(f"Execution started in background. GUID: {key}")
                else:
                    # Delete
                    print(f"The job from {value['Topic']} is deleted.")

            if value["State"] == "Executing":
                # check if the corresponding XDHIS.CSV is ready
                xdhisName = str(key) + "_xdhis.csv" 
                if os.path.isfile(xdhisName):
                    value["State"] = "Finished"
                else:
                    continue
            
            if value["State"] == "Finished":
                xdhisName = str(key) + "_xdhis.csv" 
                dl, kRot, m, iter = ReadDesignVariables(xdhisName)
                print(f"After {iter} iterations, the tip mass is estimated to {m} gr, dl= {dl} mm, kRot = {kRot} N/m")
                # Publish the result
                # Replace the topic
                newTopic = replace_subtopics(value["Topic"], json_config_public["Output"]["ModifySubtopics"])
                # Compose the payload
                byte_string = value["Payload"]
                strr = byte_string.decode('utf-8')
                data = json.loads(strr)
                utcTimeAsString = data["UTC_TimeStamp"]
                newPayload = json.dumps({"UTC_TimeStamp": utcTimeAsString, "m": m, "dl": dl, "kRot": kRot})
                print(f"Publishing the result to {newTopic}...")
                mqttc_out.publish(newTopic, newPayload, qos=json_config_private["MQTT_OUT"]["QoS"])
                print(f"Result published to {newTopic}.")
                # Clean up
                CleanupFiles(key)
                print(f"Files cleaned up for job {key}.")

        # Clean up the jobs dictionary
        for key in list(jobs.keys()):
            if jobs[key]["State"] == "Finished":
                del jobs[key]
                print(f"Finished job {key} is deleted.")
                continue
            if jobs[key]["State"] == "Invalid":
                del jobs[key]
                print(f"Invalid job {key} is deleted.")
                continue

        bReading = False

if __name__ == "__main__":
    main()
