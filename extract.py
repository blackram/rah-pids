"""A module to read the pid log and extract content"""

import json
import re
import os
from datetime import datetime, timedelta
#from dateutil.parser import parse
from pathlib import Path
from tzlocal import get_localzone
import iso8601

FILENAME = 'pid_process_history.json'

# A global representing the current time zone
TZ = get_localzone()

def match_each_visible_ticket(instance, ticket):
    """A ticket filter"""

    # tickets must have a location
    if ticket["Location"] == "":
        return False

   # this is experimental - to return all tickets regardless of status, uncomment the following line
    return True

    # all non-notified tickets qualify at this point
    if ticket["Status"] != "Notified":
        return True

    # return true where the notification status is less than one (1) hour    
    iso_dt = iso8601.parse_date(ticket["StatusTime"])
    #now_dt = TZ.localize(datetime.now())
    now_dt = TZ.localize(instance)

    if now_dt - iso_dt > timedelta(hours=2):
        return False
    else:
        return True

def match_visible_tickets(instance, cache):
    """A filter so that only tickets that would be visible are returned"""

    ticket_filter = lambda ticket: match_each_visible_ticket(instance, ticket)

    return list(filter(ticket_filter, cache))

def extract_tickets(file: str, callback):
    """A method to extract tickets from a pid log"""
    i = 0
    first_line = ''
    second_line = ''
    pid_match = re.compile(r'^([0-9- \.:]+)(?= .+Current Cache).+Adding item for Paged:(.+) to.+')

    with open(file, 'r') as file:
        while True:
            first_line = file.readline()
            if first_line == '':
                break
            match = pid_match.match(first_line)
            if match != None:

                second_line = file.readline()
                if second_line == '':
                    break
                i += 1

                cache = json.loads(second_line)

                instance = parse_datetime(match.group(1))
                callback(i,
                         instance=instance,
                         pid=match.group(2),
                         tickets=len(match_visible_tickets(instance, cache)))

def print_tickets(index, instance, pid, tickets):
    """A method that prints passed tickets"""
    print("{}, {}, {}, {}".format(index, instance, pid, tickets))

def parse_datetime(value: str) ->datetime.date:
    """parse the formatted datetime string to a datetime object"""
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")

def serialise_datetime(value: datetime.date):
    """Convert the datetime to a string"""
    return datetime.strftime(value, '%Y-%m-%d %H:%M:%S.%f')

def parse_date(value: str):
    """parse the formatted date"""
    return datetime.strptime(value, '%Y-%m-%d')

def load_process_history():
    """Load the process history into memory"""
    try:
        file = open(FILENAME, 'r')
        raw = file.read()
        file.close()
        if raw == '':
            return {}
        else:
            return json.loads(raw)

    except FileNotFoundError:
        file = open(FILENAME, 'w')
        file.close()
        return {}

def write_process_history(process_history):
    """Persist the process history"""

    with open(FILENAME, 'w') as file:
        if process_history is None:
            file.write("{}")
        else:
            file.write(json.dumps(process_history))

def should_process(log_path: str, last_modified: datetime.date):
    """Return a value indicating if the log file should be reprocessed"""

    log_file = Path(log_path)
    if not log_file.is_file():
        return False
    else:
        log_modified = datetime.fromtimestamp(os.path.getmtime(log_path))
        if last_modified is None:
            return True
        elif log_modified > last_modified:
            return True
        else:
            return False

def process_logs(log_path_factory, processor, value='2017-10-31'):
    """process all dated logs"""
    work_date = parse_date(value).date()
    now = datetime.now().date()

    process_history = load_process_history()

    while work_date <= now:

        path_history = None

        path_key = f'path_{work_date}'
        if not path_key in process_history:
            process_history[path_key] = {"path": str(work_date)}

        path_history = process_history[path_key]

        log_path = log_path_factory(work_date)

        last_modified = None

        if 'last_modified' in path_history:
            last_modified = parse_datetime(path_history["last_modified"])

        if should_process(log_path, last_modified):
            print(f"Processing {log_path}")

            processor(log_path, work_date)

            timestamp = os.path.getmtime(log_path)
            path_history["last_modified"] = serialise_datetime(datetime.fromtimestamp(timestamp))
            write_process_history(process_history)
        else:
            print(f"Not processing {log_path}")

        work_date += timedelta(days=1)


def extract_writer(file_handle, instance, tickets):
    """Write the instance and number of tickets to the matching pid file."""

    file_handle.write(f"{instance}, {tickets}\n")

def file_handle_factory(file_handles, sub_folder: str, pid: str, work_date):
    """A generator of file handles. Finds an already created handle or initialises a handle"""
    file = None
    if not pid in file_handles:
        file = open(f"{sub_folder}\\{pid.replace(' ','-')}_{work_date}.dat", 'w')
        file.write("Instance, Tickets\n")
        file_handles[pid] = file

    if file is None:
        file = file_handles[pid]
    return file

def log_processor(log_file: str, work_date: datetime.date):
    """process log file and write out all extracts"""
    out_folder = Path('extracts')
    if not out_folder.is_dir():
        os.mkdir('extracts')

    file_handles = {}

    fhf = file_handle_factory
    extract_tickets(log_file,
                    lambda index,
                           instance,
                           pid,
                           tickets: extract_writer(fhf(file_handles, 'extracts', pid, work_date),
                                                   instance, tickets))

    for file_handle_key in file_handles:
        file_handles[file_handle_key].close()



min_date = '2017-10-25'
process_logs(lambda work_date: f'\\\\hlt667a032\\e$\\PQWMS\\logs\\{work_date}\\Pid.log',
             log_processor, min_date)
