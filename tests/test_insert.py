from faker import Faker
from src.core import Core

fake = Faker()

data = []

for _ in range(3):
    name = fake.name()
    age = fake.random_int(min=18, max=80)
    email = fake.email()

    record = {
        'name': name,
        'age': age,
        'email': email
    }

    data.append(record)

db = Core("D:/OOP/NoSQL Database project/db/users.json")
db.insert("users", data)