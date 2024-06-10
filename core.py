from bts import BTS, Node
import json
import os
import time

# from src.constants import KEY_ERROR, ITEM_ERROR, ARGS_ERROR, MAX_FILE_SIZE

MAX_FILE_SIZE = 10000000  # Maximum file size in bytes (1 MB)
FLUSH_THRESHOLD = 5  # Set N for how many deletes before a flush


class Index:
    def __init__(self, table_name, field):
        self.table_name = table_name
        self.field = field
        self.index = {}

    def add(self, key, offset):
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(offset)

    def remove(self, key, offset):
        if key in self.index:
            self.index[key].remove(offset)
            if not self.index[key]:
                del self.index[key]

    def get(self, key):
        return self.index.get(key, [])


class Core:
    def __init__(self, db_path) -> None:
        self.db_path = db_path
        self.data_files = {}
        self.indexes = {}
        self.garden = {}
        self.delete_markers = {}
        self.delete_counter = 0

        if not os.path.exists(db_path):
            os.makedirs(db_path)

    def _get_data_files(self):
        return os.listdir(self.db_path)


    @staticmethod
    def _parse_condition(condition):
        key, value = condition.split('==')
        key = key.strip().strip('"')
        value = value.strip().strip('"')
        return key, value

    @staticmethod
    def _decorator_timer(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            print(f"Function {func.__name__} executed in {end - start:.5f} seconds")
            return result

        return wrapper

    def get_bytes(self, table_name):
        file_path = self._get_table_path(table_name)
        pars = []
        correct_pars = []
        bytes_count = 0
        condition = 0
        with open(file_path, 'rb') as file:
            for line in file:
                pos = 0
                while pos < len(line):
                    if chr(line[pos]) == '{':
                        pars.append(['{', bytes_count + pos])
                    elif chr(line[pos]) == '}':
                        pars.append(['}', bytes_count + pos])
                    pos += 1
                bytes_count += len(line)
        for el in pars[1:-1]:
            if el[0] == '{' and condition == 0:
                start = el[1]
                condition += 1
            elif el[0] == '}' and condition == 1:
                end = el[1]
                cords = (start, end)
                correct_pars.append(cords)
                condition -= 1
            elif el[0] == '{':
                condition += 1
            elif el[0] == '}':
                condition -= 1
        return correct_pars

    def create_BTS(self, parametr, table_name):
        file_path = self._get_table_path(table_name)
        tree = BTS()
        bytes = self.get_bytes(table_name)
        with open(file_path, 'r') as file:
            for byte in bytes:
                start = byte[0]
                end = byte[1]
                file.seek(start)
                data = file.read(end - start)
                for line in data.split('\n'):
                    if parametr in line:
                        if line[-1] == ',':
                            line = line[:-1]
                        value = line.split(':')[1]
                        try:
                            value = int(value)
                        except ValueError:
                            value = value.strip().strip('"')
                        tree.append(Node(value, [byte]))
        return tree

    @_decorator_timer
    def insert(self, table_name, obj):  # Приблизно за 1/4 секунди інсертиться
        file_path = self._get_table_path(table_name)

        if table_name not in self.data_files:
            self.data_files[table_name] = file_path

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                file_content = file.read()
                if file_content == '':
                    data = {f"{table_name}": [obj]}
                    obj['id'] = 1  # Set the id of the first user to 1
                else:
                    file.seek(0)  # Move the cursor back to the start of the file
                    data = json.load(file)
                    if table_name not in data:
                        data[table_name] = []
                    if not data[f'{table_name}']:
                        obj['id'] = 1  # Set the id of the first user to 1
                    else:
                        max_id = max(user.get('id', 0) for user in data[f'{table_name}'])  # Find the current maximum id
                        obj['id'] = max_id + 1  # Set the id of the new user to the current maximum id + 1
                    data[f'{table_name}'].append(obj)

        self._write_table(table_name, data)

    @_decorator_timer
    def update(self, table_name, condition, updates):
        file_path = self._get_table_path(table_name)
        data = self._read_table(table_name)

        if os.path.exists(file_path):
            search_results = self.select(table_name, condition)

            if not search_results:
                raise ValueError(f"No matching records found in table {table_name} for condition {condition}")

            for obj in data[table_name]:
                if obj in search_results:
                    for key, value in updates.items():
                        obj[key] = value

            self._write_table(table_name, data)
        else:
            raise ValueError(f"Table {table_name} does not exist")

    @_decorator_timer
    def select(self, table_name, condition):
        file_path = self._get_table_path(table_name)

        if os.path.exists(file_path):
            results = []
            parsed_condition = self._parse_condition(condition)
            key, value = parsed_condition
            if key not in self.garden:
                self.garden[key] = self.create_BTS(key, table_name)

            try:
                value = int(value)  # Try to convert value to an integer (searching for id)
            except ValueError:
                pass
            cordinats = self.garden[key].search(self.garden[key].root, value)
            with open(file_path, 'r')as file:
                for borders in cordinats:
                    start = borders[0]
                    end = borders[1]
                    file.seek(start)
                    data = file.read(end - start)
                    data = data.strip()
                    if data[-1] == ',':
                        data = data[:-1]
                    results.append(json.loads(data))


            if not results:
                raise ValueError(f"No matching records found in table {table_name} for condition {condition}")

            return results
        else:
            raise ValueError(f"Table {table_name} does not exist")

    @_decorator_timer
    def delete(self, table_name, condition):
        all_data = self._read_table(table_name)
        data_to_delete = self.select(table_name, condition)

        if data_to_delete:
            if table_name not in self.delete_markers:
                self.delete_markers[table_name] = set()

            for obj in data_to_delete:
                id = obj.get('id')

                if id in self.delete_markers[table_name]:
                    print(f"Object with id {id} is already marked for deletion")

                self.delete_markers[table_name].add(id)
                self.delete_counter += 1

                if self.delete_counter >= FLUSH_THRESHOLD:
                    remaining_data = list(
                        filter(lambda obj: obj.get('id') not in self.delete_markers[table_name], all_data[table_name]))
                    self._rewrite_table(table_name, remaining_data)
                    self.delete_counter = 0
                    self.delete_markers[table_name].clear()
        else:
            raise ValueError(f"Data to delete not found in table {table_name}")

    def _is_obj_deleted(self, obj):
        return obj.get('id') in self.delete_markers.get('users', set())

    def flush(self, table_name):  # ???
        pass

    def create_index(self, table_name, fields):  # не працює, можна використати для приклада
        if table_name not in self.indexes:
            self.indexes[table_name] = {}

        for field in fields:
            print(field)
            self.indexes[table_name][field] = self._create_index_for_field(table_name, field)

    def _create_index_for_field(self, table_name, field):  # не працює, можна використати для приклада
        file_path = self._get_table_path(table_name)
        index = {}

        with open(file_path, 'r') as f:
            offset = 0
            for line in f:
                obj = json.loads(line)
                if field in obj:
                    if obj[field] not in index:
                        index[obj[field]] = []
                    index[obj[field]].append(offset)
                offset += len(line)
        return index

    def _get_table_path(self, table_name):
        base_path = os.path.join(self.db_path, f"{table_name}.json")
        if os.path.exists(base_path) and os.path.getsize(
                base_path) > MAX_FILE_SIZE:  # If the file is too large (over 1 MB), create a new file
            i = 1
            while True:
                new_path = os.path.join(self.db_path, f"{table_name}_{i}.json")
                if not os.path.exists(new_path) or os.path.getsize(new_path) <= MAX_FILE_SIZE:
                    return new_path
                i += 1
        return base_path

    def _read_table(self, table_name):
        file_path = self._get_table_path(table_name)
        with open(file_path, 'r') as file:
            return json.load(file)

    def _write_table(self, table_name, data):
        file_path = self._get_table_path(table_name)

        with open(file_path, 'w') as file:
            # file.seek(start_byte)
            json.dump(data, file, indent=4)

    def _rewrite_table(self, table_name, new_data):
        file_path = self._get_table_path(table_name)

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                data = json.load(file)
                data[table_name] = new_data  # Update the data with the new data

            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
        else:
            raise ValueError(f"Table {table_name} does not exist")

    def _get_byte_offset(self, file_path):  # не працює, можна використати для приклада
        with open(file_path, 'rb') as file:
            file.seek(0, os.SEEK_END)  # Move the cursor to the end of the file
            file_size = file.tell()  # Get the current position of the cursor, which is the size of the file
            # return f"File size: {file_size} bytes, {os.path.getsize(file_path)} bytes"
            return file.read()

    # Ці теж можна використати як приклад для реалізації

    def _get_index_by_key(self, table_name, field):
        return self.indexes[table_name][field]

    def _search_in_index_by_value(self, table_name, field, value):
        index = self._get_index_by_key(table_name, field)
        return index.get(value, [])

    def _get_from_file_by_adress(self, file_path, byte_offset):
        with open(file_path, 'r') as file:
            file.seek(byte_offset)
            return json.load(file)


def update_nested_dict(data, key, value, new_value):
    if isinstance(data, dict):
        for k, v in data.items():
            if k == key and v == value:
                data[k] = new_value
            elif isinstance(v, dict):
                update_nested_dict(v, key, value, new_value)


if __name__ == "__main__":
    db = Core("C:\\Users\\ASUS\\PycharmProjects\pythonProject\\venv\\University Project")

    print(db.select("users", '"name" == "Grisha"'))
    print(db.select("users", '"name" == "Sasha"'))
    print(db.select("users", '"name" == "Richard Hughes"'))
    print(db.select("users", '"adress" == null'))