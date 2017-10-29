import pandas
from datetime import datetime, timedelta

pids = {"PID-Public",
        "PID-3C-Medical-Day-And-Pathology",
        "PID-3E-Clinics", "PID-3E-Day-and-Wing-1",
        "PID-3F-Clinics",
        "PID-3G-Clinics",
        "PID-Medical-Imaging"}

def parse_date(value: str):
    """parse the formatted date"""
    return datetime.strptime(value, '%Y-%m-%d')

def analyse_extracts(log_path_factory, value='2017-10-25'):
    """process all dated extracts"""

    now = datetime.now().date()

    results = {}

    for pid in pids:
        results[pid] = {}

        pid_max = -1
        work_date = parse_date(value).date()

        while work_date <= now:
            s = pandas.read_csv(log_path_factory(pid, work_date),
                                header=0,
                                parse_dates=[],
                                infer_datetime_format=True,
                                names=['instance', 'tickets'], index_col=None)

            ticket_max = s['tickets'].max()
            pid_max = max(pid_max, ticket_max)
            work_date += timedelta(days=1)
            del s

        results[pid] = pid_max

    print(results)

analyse_extracts(lambda pid, work_date: f'extracts\{pid}_{work_date}.dat', '2017-10-25')