import sys
import json
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


def get_last_number_and_increment():
    collection_name = 'settings'
    collections = get_mongo_collection(collection_name=collection_name)
    query = {'id': "number"}
    doc = collections.find_one(filter=query)
    if doc:
        last_number = doc['number']
        new_doc = {"$set": {"number": last_number + 1}}
        last_number = doc['number']
        get_mongo_collection(collection_name=collection_name).update_one(filter=query, update=new_doc)
    else:
        last_number = 1
        doc = {'id': "number", "number": last_number + 1}
        get_mongo_collection(collection_name=collection_name).insert_one(document=doc)
    return last_number


def get_document(email: str):
    query = {'email': email}
    return get_mongo_collection().find_one(filter=query)


def get_document_id(email: str):
    return str(get_document(email=email)['id'])


def update__db_replace_with_id_generate_new_result(file_name: str):
    f = open(file_name)
    data = json.load(f)
    f.close()

    new_data = {}
    for email in data:
        # ID Fetch
        if is_document_exist(email=email):
            email_id = get_document_id(email)
        else:
            doc = {'email': email, "id": get_last_number_and_increment()}
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
