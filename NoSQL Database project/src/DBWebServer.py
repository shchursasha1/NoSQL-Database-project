import logging
import os
from flask import Flask, request, jsonify, render_template_string
from src.core import Core
from logging.handlers import RotatingFileHandler

class DBWebServer:
    def __init__(self, db_path, host='127.0.0.1', port=5000):
        self.core = Core(db_path=db_path)
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self._setup_logging()
        self._setup_routes()

    def _setup_logging(self):
            log_file = 'D:/OOP/NoSQL Database project/server_logs/db_web_server.log'
            
            handler = RotatingFileHandler(log_file, maxBytes=10000, backupCount=1)
            handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')
            handler.setFormatter(formatter)
            self.app.logger.addHandler(handler)
            self.app.logger.setLevel(logging.INFO)
            self.app.logger.info("Logging is set up.")

    def _setup_routes(self):
        @self.app.route('/')
        def index():
            return "Welcome to the NoSQL Database Server!", 200
        
        @self.app.route('/insert', methods=['POST'])
        def insert():
            data = request.get_json()
            table = data.get('table')
            obj = data.get('object')
            try:
                self.core.insert(table, obj)
                self.app.logger.info(f"Inserted into table {table}: {obj}")
                return jsonify({"status": "success"})
            except Exception as e:
                self.app.logger.error(f"Error inserting into table {table}: {e}")
                return jsonify({"status": "error", "message": str(e)})

        @self.app.route('/select', methods=['GET'])
        def select():
            table = request.args.get('table')
            condition = request.args.get('condition')
            try:
                result = self.core.select(table, condition)
                self.app.logger.info(f"Selected from table {table} with condition {condition}: success")
                return jsonify({"status": "success", "data": result})
            except Exception as e:
                self.app.logger.error(f"Error selecting from table {table} with condition {condition}: {e}")
                return jsonify({"status": "error", "message": str(e)})

        @self.app.route('/select_html', methods=['GET'])
        def select_html():
            table = request.args.get('table')
            condition = request.args.get('condition')
            try:
                result = self.core.select(table, condition)
                self.app.logger.info(f"Selected from table {table} with condition {condition}: success")
                return render_template_string(self._generate_html(result))
            except Exception as e:
                self.app.logger.error(f"Error selecting from table {table} with condition {condition}: {e}")
                return render_template_string(f"<h1>Error: {str(e)}</h1>")

        @self.app.route('/update', methods=['POST'])
        def update():
            data = request.get_json()
            table = data.get('table')
            condition = data.get('condition')
            updates = data.get('updates')
            try:
                self.core.update(table, condition, updates)
                self.app.logger.info(f"Updated table {table} with condition {condition} and updates {updates}: success")
                return jsonify({"status": "success"})
            except Exception as e:
                self.app.logger.error(f"Error updating table {table} with condition {condition} and updates {updates}: {e}")
                return jsonify({"status": "error", "message": str(e)})

        @self.app.route('/delete', methods=['POST'])
        def delete():
            data = request.get_json()
            table = data.get('table')
            condition = data.get('condition')
            try:
                self.core.delete(table, condition)
                self.app.logger.info(f"Deleted from table {table} with condition {condition}: success")
                return jsonify({"status": "success"})
            except Exception as e:
                self.app.logger.error(f"Error deleting from table {table} with condition {condition}: {e}")
                return jsonify({"status": "error", "message": str(e)})

    def _generate_html(self, data):
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>DB Select Results</title>
        </head>
        <body>
            <h1>Results</h1>
            <table border="1">
                <tr>
                    {headers}
                </tr>
                {rows}
            </table>
        </body>
        </html>
        """

        if not data:
            return html.format(headers="", rows="")

        headers = "".join([f"<th>{key}</th>" for key in data[0].keys()])
        rows = "".join(["<tr>" + "".join([f"<td>{value}</td>" for value in row.values()]) + "</tr>" for row in data])

        return html.format(headers=headers, rows=rows)

    def run(self):
        self.app.run(debug=True, host=self.host, port=self.port)

if __name__ == "__main__":
    db_server = DBWebServer('D:/OOP/NoSQL Database project/db')
    db_server.run()
