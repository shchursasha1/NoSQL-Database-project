from flask import Flask, jsonify
import json

app = Flask(__name__)

@app.route('/')
def get_users():
	print("Using jsonify")
	users = json.load(open('D:/OOP/NoSQL Database project/db/users.json'))
	return jsonify({'users': users}) 


if __name__ == '__main__':
	app.run()
