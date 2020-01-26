import pymongo
import re

mongo = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db = mongo['flu']

# db['BS301'].delete_many({'visitOrdNo': '2000000278'})

x_set = []
for x in db.BS301.find({'diagnosis.diseaseCode': re.compile(r"^J(?i)")}):
    x_set.append(x)

for y in x_set:
    print(y)

mongo.close()
