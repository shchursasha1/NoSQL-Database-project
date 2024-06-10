from faker import Faker
from src.core import Core

def test_insert(n):
    fake = Faker()

    data = []

    for _ in range(n):
        name = fake.name()
        age = fake.random_int(min=18, max=80)
        email = fake.email()

        record = {
            'name': name,
            'age': age,
            'email': email
        }

        data.append(record)
    return data

db = Core("D:/OOP/NoSQL Database project/db")

for obj in test_insert(20):
    db.insert("users", obj)