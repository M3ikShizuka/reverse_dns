import json
import sys

def get_args():
    # Get file path from arguments.
    file_path = None

    try:
        file_path = sys.argv[1]
    except IndexError:
        print("File path isn't set!")
        exit(code=1)

    return file_path

def get_words(file_path):
    try:
        with open(file_path, 'r') as file:
            words = [line.rstrip() for line in file]
    except OSError as exc:
        print("Can't open file for reading!")
        exit(code=1)

    return words

def generate_data(words):
    json_data_list = []

    for _, word in enumerate(words):
        json_data_list.append(word)
         
    return json.dumps(json_data_list) 

def save_data(data):
    try:
        with open("result.json", 'w', encoding='utf-8') as file:
            file.write(data)
    except OSError as exc:
        print("Can't open file for writing!")
        exit(code=1)

def main():
    file_path = get_args()
    words = get_words(file_path)
    result_data = generate_data(words)
    save_data(result_data)

if __name__ == "__main__":
	main()