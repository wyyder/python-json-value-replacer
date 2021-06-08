import json
import sys
from pymongo import MongoClient

MONGO_URL = "mongodb://localhost:27017"


def get_mongo_collection(database_name: str = 'users', collection_name: str = 'email'):
    client = MongoClient(MONGO_URL)
    db = client.get_database(database_name)
    collection = db.get_collection(collection_name)
    return collection


def is_document_exist(email: str):
    query = {'email': email}
    if get_mongo_collection().find_one(filter=query):
        return True
    return False


def get_document(email: str):
    query = {'email': email}
    return get_mongo_collection().find_one(filter=query)


def get_document_id(email: str):
    return str(get_document(email=email)['_id'])


def update__db_replace_with_id_generate_new_result(file_name: str):
    f = open(file_name)
    data = json.load(f)
    f.close()

    new_data = {}
    for email in data:
        # ID Fetch
        email_id = ""
        if is_document_exist(email=email):
            email_id = get_document_id(email)
        else:
            doc = {'email': email}
            get_mongo_collection().insert_one(document=doc)
            email_id = get_document_id(email)

        # Replace Key with ID
        new_data[email_id] = data[email]

    # Writing to output file
    with open(file_name.replace('.json', '_out.json'), "w") as outfile:
        outfile.write(json.dumps(new_data, indent=4))


if __name__ == '__main__':
    print("Started..")
    if len(sys.argv) == 1:
        print("Error : file not supplied - Supply file as argument ")
    file_name = sys.argv[1]
    update__db_replace_with_id_generate_new_result(file_name=file_name)
    print("completed !")
