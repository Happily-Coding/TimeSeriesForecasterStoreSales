#TODO, separate db creation and user creation? The user might already exist.
from db_interfacing import db_interface
db_interface.create_db()
db_interface.create_engineering_user_if_not_exists_and_allowed()