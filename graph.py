"""."""
import pandas
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

pids = {"PID-Public",
        "PID-3C-Medical-Day-And-Pathology",
        "PID-3E-Clinics", "PID-3E-Day-and-Wing-1",
        "PID-3F-Clinics",
        "PID-3G-Clinics",
        "PID-Medical-Imaging"}

styles = [ {'linestyle':'', 'marker':'9'},
           {'linestyle':'-.', 'marker':'x'},
           {'linestyle':'--', 'marker':'3'},
           {'linestyle':'-', 'marker':'.'},
           {'linestyle':'-', 'marker':'*'},
           {'linestyle':'-.', 'marker':'4'},
           {'linestyle':'--', 'marker':'s'},
           {'linestyle':'-', 'marker':'P'}]

work_date = "2017-10-30"

marker_style = dict(linestyle=':', color='cornflowerblue', markersize=10)

def labelled_series(pid):
    """Returns a series populated with data for the pid on the correct day"""
    s = pandas.read_csv(f'extracts\\{pid}_{work_date}.dat',
                        header=0,
                        parse_dates=[0],
                        infer_datetime_format=True,
                        names=['instance', 'tickets'], index_col=None)
    s['time'] = s['instance']# .dt.time

    s.drop('instance', axis=1, inplace=True)
    s = s.set_index('time')

    s.name = pid
    return s

series = list(map(labelled_series, pids))

plt_count = 0

for s in series:
    style = styles.pop()

    plt_count += 1
    ax = plt.subplot(3, 3, plt_count)
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H'))
    ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))

    ax.grid(True)

    for label in ax.get_yticklabels():
        label.set_fontname('Arial')
        label.set_fontsize(8)

    for label in ax.get_xticklabels():
        label.set_fontname('Arial')
    label.set_fontsize(8)

    plt.xlabel('Hour')
    plt.ylabel('Visible Tickets')
    plt.title(s.name)
    plt.plot(s,
             label=s.name,
             linestyle=style['linestyle'],
             linewidth=1,
             marker=style['marker'],
             markevery=1000)
    ax.autoscale_view()
    print(s.describe())

plt.suptitle(f"Number of tickets over {work_date} by PID")
plt.subplots_adjust(hspace=0.6, wspace=0.25)
plt.show()
