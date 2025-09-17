import hl7
import json
import os

pid_segment = 1
obr_segment = 3
first_field = 0
id_field = 3
name_field = 5
order_id_field = 2
birth_date_field = 7
sex_at_birth_field = 8
sample_id_field = 3
sample_test_field = 4
first_name_component = 0
last_name_component = 1
test_name_component = 1
into_component = 0

def create_json_dump(message):
    json_dump = {
        "ph_data": {
            "patient_id": str(message
                              [pid_segment]
                              [id_field]
                              ),
            "order_id": str(message
                            [obr_segment]
                            [order_id_field]
                            ),
            "last_name": str(message
                             [pid_segment]
                             [name_field]
                             [into_component]
                             [last_name_component]
                             ),
            "first_name": str(message
                              [pid_segment]
                              [name_field]
                              [into_component][first_name_component]
                              ),
            "birth_date": str(message
                              [pid_segment]
                              [birth_date_field]
                              [into_component]
                              ),
            "sex_at_birth": str(message
                                [pid_segment][sex_at_birth_field]
                                ),
        },
        "blood_samples": [],
        "HL7": str(message)
    }

    for index, i in enumerate(message[3:], start=3):
        if f'{i[first_field]}' == "OBR":
            sample_id = str(i[sample_id_field])
            test = str(i
                       [sample_test_field]
                       [into_component]
                       [test_name_component]
                       ) 
            sample_exists = any(sample["sample_id"] == sample_id for sample in json_dump["blood_samples"])
            if sample_exists:
                for sample in json_dump["blood_samples"]:
                    if sample["sample_id"] == sample_id:
                        sample["tests"].append(test)
                        break
            else:
                json_dump["blood_samples"].append({"sample_id": sample_id, "tests": [test]})

    return json_dump

def parse_hl7(file: str):
    with open(file, "r") as file:
        message = file.read().splitlines()

    concat_message = "\r".join(message)
    parsed_message = hl7.parse(concat_message)

    file_path = f'./data/exports/{parsed_message[obr_segment][order_id_field]}_PID_{parsed_message[pid_segment][name_field][into_component][last_name_component]}_{parsed_message[pid_segment][name_field][into_component][first_name_component]}.json'

    json_dump = create_json_dump(parsed_message)
    data = json.dumps(json_dump, ensure_ascii=False, indent=4)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(data)

    print(f"Data written to {file_path}")

def parse_files(input_path: str):
    print("Parsing HL7 files...")
    root_dir = os.path.dirname(__file__)
    path = f'{root_dir}/{input_path}'
    files = os.listdir(path)

    python_files = [file for file in files if file.endswith('.hl7')]
    print("Files to process..." ,python_files)
    for file in python_files:
        parse_hl7(f'{path}/{file}')

parse_files('data/imports')