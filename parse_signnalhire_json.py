import json
import os
import csv
import time
import datetime


def parse_json(json_folder_path):
    # List all JSON files in the specified folder
    # Filter JSON files based on a specific creation date

    json_files = [f for f in os.listdir(json_folder_path) if f.endswith('.json')]
    # Loop through the files and upload each one
    for json_file in json_files:
        file_path = os.path.join(json_folder_path, json_file)
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                # Access data
                uid = data.get('item', None)
                candidate_info = data.get('candidate', {})
                full_name = candidate_info.get('fullName', 'unknown')
                contacts = candidate_info.get('contacts', [])
                experience = candidate_info.get('experience', [])

                # Check if experience list is not empty before accessing
                if experience:
                    recent_experience_position = experience[0].get('position', 'N/A')
                    recent_experience_company = experience[0].get('company', 'N/A')
                    recent_experience_summary = experience[0].get('summary', 'N/A')
                else:
                    recent_experience_position = 'N/A'
                    recent_experience_company = 'N/A'
                    recent_experience_summary = 'N/A'

                email_contacts = [contact['value'] for contact in contacts if contact['type'] == "email"]

                with open('people_output_li.csv', 'a', newline='') as csvfile:
                    fieldnames = ['linkedin', 'full_name', 'email_contacts', 'recent_experience_position', 'recent_experience_company', 'recent_experience_summary']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                    # Write header if file is empty
                    if csvfile.tell() == 0:
                        writer.writeheader()

                    writer.writerow({
                        'linkedin': uid,
                        'full_name': full_name,
                        'email_contacts': email_contacts,
                        'recent_experience_position': recent_experience_position,
                        'recent_experience_company': recent_experience_company,
                        'recent_experience_summary': recent_experience_summary
                    })
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except json.JSONDecodeError:
            print(f"Error decoding JSON from file: {file_path}")
        except IndexError:
            print(f"Error accessing index in an empty list for file: {file_path}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")


parse_json('./json')