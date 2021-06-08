import os
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
    return str(get_document(email=email)['id'])


def rename_images_with_id():
    for filename in os.listdir(os.curdir):
        new_filename: str = ''
        ext: str = ''
        if filename.endswith(".jpg"):
            ext = ".jpg"
            new_filename = filename.replace('.jpg', '')
        elif filename.endswith(".png"):
            ext = ".png"
            new_filename = filename.replace('.png', '')
        elif filename.endswith(".jpeg"):
            ext = ".jpeg"
            new_filename = filename.replace('.jpeg', '')

        if is_document_exist(email=new_filename):
            email_id = get_document_id(new_filename)
            os.rename(filename, email_id + ext)


if __name__ == '__main__':
    print("Started..")
    rename_images_with_id()
    print("completed !")
