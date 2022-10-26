## pymongo 4.3.2
## https://pymongo.readthedocs.io/en/stable/tutorial.html

## MongoClient 연결
from pymongo import MongoClient

client = MongoClient()
# client = MongoClient('localhost', 27017) #위와 같음
# client = MongoClient('mongodb://localhost:27017/') #위와 같음
# MongoClient() 에는 URL, 포트를 지정한다


## 데이터베이스 가져오기
db = client.test_db
# db = client['test_db'] #딕셔너리 방식으로 가져오기
print(db)

## 컬렉션 가져오기 *데이터베이스가져오기와 동일한작동을 한다
collection = db.test_collection
# collection = db['test_collection'] #딕셔너리 방식으로 가져오기
print(collection)

## MongoDB의 컬렉션(및 데이터베이스)에 대한 중요한 참고 사항
# 느리게 생성된다 위의 명령 중 어느 것도 MongoDB 서버에서 실제로 어떤 작업도 수행하지 않는다.
# 컬렉션과 데이터베이스는 첫 번째 문서가 삽입될 때 생성된다.


# 컬렉션 리스트 출력
listCollection = db.list_collection_names()
print(listCollection)

## 도큐먼트
# JSON-style의 도큐먼트로 표현(저장)된다. key : value
import datetime
post = {"author": "Mike",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
        "date": datetime.datetime.utcnow()}
print(post)




## 도큐먼트 삽입 Create
# insert_one() method:
posts = collection #위에서 가져온 test_db, test_collenction을 posts 라는변수에 저장
# posts = db.test_collection2 # 이런식으로 없던 컬렉션을 지정하고 데이터를 삽입하면 컬렉션이 새로 생김
post_id = posts.insert_one(post).inserted_id #JSON-style의 post를 posts에 삽입
print(post_id) # ObjectId('...') 가 출력됨







## 도큐먼트 조회 Read
# find_one() method:
# 가장 기본적인 쿼리유형
# 쿼리와 일치하는 단일 도큐먼트를 반환

posts = collection #위에서 가져온 test_db, test_collenction을 posts 라는변수에 저장
import pprint # 출력하기 위한 모듈
pprint.pprint(posts.find_one()) # posts에서 find_one

pprint.pprint(posts.find_one({"author": "Mike"})) # author : Mike 찾아서출력

findEliot = posts.find_one({"author": "Eliot"}) # author : Eliot
if findEliot:
    #있다면
    print(findEliot)
else:
    #없다면
    print("찾지 못했습니다.")


## ObjectId로 쿼리방법
objectID = posts.find_one() #하나를 찾는다
print(objectID['_id']) #찾은것의 objectID 출력
print(type(objectID)) #딕셔너리타입
print(type(objectID['_id'])) # binary json 타입 bson.objectid.ObjectId
# 주의 ObjectId는 문자열 표현과 동일하지 않다.
if type(objectID['_id']) == type("hello"):
    print("같다")
else:
    print("아니다")

## 문자열 binary json 타입 변환 후 find_one()
from bson.objectid import ObjectId
def get(post_id):
    # 문자열에서 변환해야 한다.
    document = client.db.collection.find_one({'_id': ObjectId(post_id)})


## 대량 도큐먼트 삽입 insert_many()
new_posts = [{"author": "Mike",
              "text": "Another post!",
              "tags": ["bulk", "insert"],
              "date": datetime.datetime(2009, 11, 12, 11, 14)},
             {"author": "Eliot",
              "title": "MongoDB is fun",
              "text": "and pretty easy too!",
              "date": datetime.datetime(2009, 11, 10, 10, 45)}]

result = posts.insert_many(new_posts)
# 대괄호 사용과 insert_many() 를 이용


## 둘 이상의 도큐먼트 조회 쿼리 find()
# 컬렉션내의 모든 도큐먼트 출력
posts = collection #위에서 가져온 test_collection
for post in posts.find():
    pprint.pprint(post)

# find_one() 처럼 조건을 줘서 결과 반환
for post in posts.find({"author": "Mike"}):
  pprint.pprint(post)



## 카운팅하기 count_documents()
posts = collection #위에서 가져온 test_collection
posts.count_documents({}) # 컬렉션의 모든 문서 숫자
posts.count_documents({"author": "Mike"}) # 특정 쿼리 일치하는 문서 숫자


## 범위 쿼리
# 특정 날짜보다 오래된 게시물로 결과를 제한하고 작성자별로 결과를 정렬하는 쿼리
d = datetime.datetime(2009, 11, 12, 12)
posts = collection #위에서 가져온 test_collection
for post in posts.find({"date": {"$lt": d}}).sort("author"):
  pprint.pprint(post)

# find()와 sort() 사용에 주목한다.


## 인덱싱
# 인덱스를 추가하면 특정 쿼리를 가속화하는 데 도움이 될 수 있으며 문서 쿼리 및 저장에 기능을 추가할 수도 있다.
import pymongo
# 'user_id' 라는 인덱스를 추가
result = db.profiles.create_index([('user_id', pymongo.ASCENDING)],
                                  unique=True)

# 인덱스 확인 db.profiles.index_information()
print(sorted(list(db.profiles.index_information())))

# 인덱스는 이미 컬렉션에 있는 문서를 삽입하는것을 방지함
# 기본 도큐먼트
user_profiles = [
    {'user_id': 211, 'name': 'Luke'},
    {'user_id': 212, 'name': 'Ziltoid'}]
result = db.profiles.insert_many(user_profiles)

#새로 추가할 도큐먼트
new_profile = {'user_id': 213, 'name': 'Drew'}
duplicate_profile = {'user_id': 212, 'name': 'Tommy'}
result = db.profiles.insert_one(new_profile)  # 저장됨
result = db.profiles.insert_one(duplicate_profile) # Traceback 오류
# Traceback (most recent call last):
# DuplicateKeyError: E11000 duplicate key error index: test_database.profiles.$user_id_1 dup key: { : 212 }
# 212 user_id가 이미 기본 도큐먼트에 있기 때문에 삽입이 되지 않는다.

