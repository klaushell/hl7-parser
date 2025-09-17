import os
import json

obr = 3
segment_field = 0
observation_identifier_field = 4
time_stamp_field = 6
message_type = 8
tests_to_extract = ["Creatinine", "Lipoprotein (a)", "Hemoglobin A1c %"]
producer = "Entity responsible for producing HL7 file"
value_type = "NM"
observation_result_status = "F"

def create_hl7_dump(dict):

    # Creating HL7 message segments
    message = dict["HL7"].splitlines()
    msh_segment = message[segment_field].split("|")
    msh_segment[message_type]= "ORU^R01"
    message[0] = "|".join(msh_segment)
    obr_segment = message[obr].split("|")

    # Extracting the results from JSON
    results = dict.get("results", {})
    extracted_results = {}
    for sample_id, sample_results in results.items():
        for test in tests_to_extract:
            if test in sample_results:
                extracted_results[test] = sample_results[test]

    counter = 1
    list = []
    for index, i in enumerate(message):
        list.append(i)
        obr_segment = message[index].split("|")
        if index > 2 and obr_segment[segment_field] == "OBR":
            biomarker = obr_segment[observation_identifier_field][2:]
            if biomarker in extracted_results:  
              list.append(f'OBX|{counter}|{value_type}|{counter+3}^{biomarker}||{extracted_results[biomarker]["value"]}|{extracted_results[biomarker]["units"]}|{extracted_results[biomarker]["range"]}||||{observation_result_status}|||{msh_segment[time_stamp_field]}|{producer}')
    hl7_file  = '\r'.join(list)
    return hl7_file

def parse_JSON(file: str):
    with open(file) as file:
        json_file = file.read()

    dict = json.loads(json_file)
    file_path = f'./data/exports/{dict["ph_data"]["order_id"]}_PID_{dict["ph_data"]["last_name"]}_{dict["ph_data"]["first_name"]}.hl7'

    data = create_hl7_dump(dict)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(data)

    print(f"Data written to {file_path}")

def parse_files(input_path: str):
    print("Parsing JSON files...")
    root_dir = os.path.dirname(__file__)
    path = f'{root_dir}/{input_path}'
    files = os.listdir(path)

    json_files = [file for file in files if file.endswith('.json')]
    print("Files to process..." ,json_files)
    for file in json_files:
        parse_JSON(f'{path}/{file}')

parse_files('data/imports')