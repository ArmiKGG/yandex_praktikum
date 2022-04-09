import pymongo
# pip install pymongo dnspython

client = pymongo.MongoClient('mongodb+srv://arman:arman@cluster0.qmeif.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
data_store = client.yandex_praktikum.btc_usd
for i in data_store.find({}):
    del i['_id']
    print(i)
