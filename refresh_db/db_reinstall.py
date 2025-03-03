import datetime
import glob
import json
import os
import time
from pathlib import Path

from mysql import connector as mysql_connector
from termcolor import colored, cprint
from dotenv import load_dotenv

# <editor-fold desc="Setup">
load_dotenv()
FILE_PATH = os.path.dirname(os.path.realpath(__file__)) + os.path.sep
SQL_CHANGES = Path(FILE_PATH + "sql-changes")
SQL_DUMPS = Path(FILE_PATH + "dumps")
CONFIG_FILE = Path(FILE_PATH + "db_reinstall_config.json")
RESET_DB = True

class Database:
    def __init__(self, env):
        self.host = os.getenv(f"{env}_DATABASE_HOST", 'localhost')
        self.database = os.getenv(f"{env}_DATABASE_NAME", 'default')
        self.user = os.getenv(f"{env}_DATABASE_USER", 'root')
        self.passwd = os.getenv(f"{env}_DATABASE_PASS", '')
        self.port = os.getenv(f"{env}_DATABASE_PORT", '3306')


MYSQL_IGNORED_EXCEPTIONS = {
    "Duplicate column": 1060,
    "Duplicate key name": 1061,
    "Duplicate key name in table": 1022,
    "Table already exists": 1050,
    "Query was empty": 1065,
}


target_db = Database('LOCAL')
origin_db = Database('PROD')

def get_newest_file(directory):
    files = glob.glob(os.path.join(directory, '*'))  # Get all files in directory
    if not files:  # If no files found, return None
        return None
    newest_file = max(files, key=os.path.getmtime)  # Get file with latest modification time
    return newest_file

def get_file_size_in_mb(file_path):
    return os.path.getsize(file_path) / (1024 * 1024)

def execute_scripts_from_file(filename, error_color=None):
    cnx = mysql_connector.connect(user=target_db.user, password=target_db.passwd, host=target_db.host,
                                  database=target_db.database)
    c = cnx.cursor()
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sql_file = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sql_commands = sql_file.split(';')

    # Execute every command from the input file
    for command in sql_commands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            c.execute(command)
        except (mysql_connector.ProgrammingError, mysql_connector.IntegrityError) as msg:
            if msg.errno in MYSQL_IGNORED_EXCEPTIONS.values():
                cprint(("Command skipped: " + str(msg)), error_color)
            else:
                cprint("Exception number: " + str(msg.errno), "yellow")
                cprint("Query\n: " + str(command), "yellow")
                raise msg
    c.close()
    cnx.commit()
    cnx.close()


def generate_production_dump():
    if not os.path.exists(SQL_DUMPS):
        os.makedirs(SQL_DUMPS)
        last_dump_mb = 1000
    else:
        last_dump_mb = int(get_file_size_in_mb(get_newest_file(SQL_DUMPS)))
    print(f"Today's dump not found, downloading from {origin_db.host}...")
    posix_pv_size_string = f" | pv --size {last_dump_mb}m"
    os.system(f"mysqldump --single-transaction --column-statistics=0 -h {origin_db.host} -u {origin_db.user} -p{origin_db.passwd} {origin_db.database}{posix_pv_size_string if os.name == 'posix' else ''} > dumps/{datetime.datetime.today().date().isoformat()}-dump.sql")


def get_prod_dump():
    list_of_files = list(SQL_DUMPS.glob("*.sql"))
    if not list_of_files:
        generate_production_dump()
        return get_prod_dump()

    latest_file = max(list_of_files, key=os.path.getctime)
    if datetime.datetime.fromtimestamp(os.path.getctime(latest_file)).date() != datetime.datetime.today().date():
        generate_production_dump()
        return get_prod_dump()

    return Path(latest_file)


# </editor-fold>


def main():
    start_time = time.time()

    if os.name == 'posix':
        os.system("""
    if [ `dpkg-query -W -f='${Status}' pv 2>/dev/null | grep -c "ok installed"` -lt 1 ]; then
    echo "Missing pv, installing"
    sudo apt-get install pv
    fi
    """)

    # <editor-fold desc="Database operations">
    with open('temp-db-pass.cnf', 'w') as the_file:
        the_file.write(
            f"[client]\nuser={target_db.user}\npassword={target_db.passwd}\nhost={target_db.host}\nport={target_db.port}")

    sql_cl = "mysql --defaults-file=temp-db-pass.cnf"
    sql_cl_db = "{} -D {} ".format(sql_cl, target_db.database)

    # Unsure if will work on all platforms
    quote = '"'

    dump = get_prod_dump()

    if RESET_DB:
        # Setup
        input(f"Confirm drop of {target_db.host}")
        print(f"Dropping old database {target_db.database}...")
        os.system(f"{sql_cl} -e {quote}DROP DATABASE {target_db.database}{quote}")

        print(f"Creating new database {target_db.database}...")
        os.system(f"{sql_cl} -e {quote}CREATE DATABASE {target_db.database}{quote}")

        # print("Importing dump into database...")
        print("Importing dump: ", colored(dump.name, "green"))
        if os.name == 'nt':
            os.system(f"{sql_cl_db} < {dump.absolute()}")
        elif os.name == 'posix':
            os.system("pv {} | {}".format(dump.absolute(), sql_cl_db))
        else:
            raise Exception("Mac not supported :P")

    cprint("Importing SQL changes", "cyan")
    feature_changes = SQL_CHANGES.glob("*.sql")
    for feature in feature_changes:
        execute_scripts_from_file(feature.absolute(), "red")
        print("SQL change: {} imported.".format(feature.name, colored(feature.name, "green")))

    # </editor-fold>

    # Cleanup
    os.remove("temp-db-pass.cnf")

    print("--- Completed database import in {} seconds ---".format(round(time.time() - start_time, 2)))


if __name__ == "__main__":
    main()
    # get_prod_dump()