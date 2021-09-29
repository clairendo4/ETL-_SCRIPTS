import time
from datetime import timedelta

from config.config_handler import read_properties
from data.extract.rest_handler import make_threaded_calls
from data.process.matcher import match_users
from utils.db_connections import get_connection
from utils.write_sql_dml import write_dml

init_rest = False
is_request_required = True

db_connections = read_properties("db_connections.txt")
rest_connections = read_properties("rest_connections.txt")
# total_pages = 152
rest_end_pages = 152
total_pages = rest_end_pages + 1
range_pages = 20
start = time.time()

if __name__ == '__main__':
    output_content = ""
    cnx = get_connection(db_connections)
    current_page = 1
    end_page = current_page + range_pages
    is_rest_call_required = True
    users_map = {}
    matched_users = []
    while is_rest_call_required:
        if end_page > total_pages:
            is_rest_call_required = False
            end_page = total_pages
        if current_page >= end_page:
            break
        make_threaded_calls(rest_connections, current_page, end_page, users_map)
        current_page = end_page
        end_page = current_page + range_pages
        match_users(cnx, users_map, matched_users)
    cnx.close()
    output_content = output_content + str(timedelta(seconds=(time.time() - start))) + "\n"
    output_content = output_content + "matched users " + str(len(matched_users)) + "\n"
    output_content = output_content + str(matched_users[10]) + "\n"
    with open("output.txt", 'w') as f:
        f.write(output_content)
        f.close()
    write_dml(matched_users)
