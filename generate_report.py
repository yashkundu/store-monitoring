from reports import rdb
import datetime
from datetime import datetime as dt
import pytz
from models import StoreStatus, MenuHours, Timezones
from database import db
from sqlalchemy import func
import pandas as pd
from reports import rdb

days = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}

# a time slot for which the restaurent is open at a stretch [start, end]
class MenuSlot():
    def __init__(self, start: dt, end: dt, timezone: str):
        self.timezone = timezone
        self.start = self.convert_to_utc(start)
        self.end = self.convert_to_utc(end)
        self.ob_pts: list[tuple[dt, bool]] = []

    def convert_to_utc(self, timestamp: dt) -> dt:
        try:
            local = pytz.timezone(self.timezone)
        except Exception:
            local = pytz.timezone('UTC')
        return local.localize(timestamp, is_dst=None).astimezone(pytz.UTC)

    def __repr__(self) -> str:
        return f"<MenuSlot ({self.start.strftime('%Y-%m-%d %H:%M:%S')}, {self.end.strftime('%Y-%m-%d %H:%M:%S')})>"
    
    # adds the status of a observtion time (from 1st csv)
    def add_observation_pt(self, timestamp: dt, is_active: bool):
        self.ob_pts.append((timestamp, is_active))

    
    def pre_compute(self):
        if len(self.ob_pts)==0 or self.ob_pts[0][0] != self.start:
            self.ob_pts.insert(0, (self.start, True))
        if len(self.ob_pts)<=1 or self.ob_pts[-1][0] != self.end:
            self.ob_pts.append((self.end, True))
        self.presum: datetime.timedelta = [datetime.timedelta()]*(len(self.ob_pts)+2)
        self.sufsum: datetime.timedelta = [datetime.timedelta()]*(len(self.ob_pts)+2)

        # calculating prefix sums to get a total time for consecutive actives and inactives
        for i in range(len(self.ob_pts)):
            self.presum[i+1] += self.presum[i] + ((self.ob_pts[i][0]-self.ob_pts[i-1][0]) if (i>0 and self.ob_pts[i][1] == self.ob_pts[i-1][1]) else datetime.timedelta())
        for i in range(len(self.ob_pts), 0, -1):
            self.sufsum[i] += self.sufsum[i+1] + ((self.ob_pts[i-1][0]-self.ob_pts[i-2][0])if (i>1 and self.ob_pts[i-1][1]==self.ob_pts[i-2][1]) else datetime.timedelta())


    # the uptime and downtime is calculated here. (qstart: start time of query, qend: end time of query)
    # logic 
    # 1) time between consecutive actives is treated as active
    # 2) time between consecutive inactives is treated as inactive
    # 3) time between an active and inactive observation is split into active and inactive parts in the proportion of active and inactive time 
    # 3.1) if on any one sides there are single observations it will be split 50-50 like (o | o)
    # 3.2) otherwise it will split on the basis of their consecutive time ratios (o o | | | |)
    # 4) if observations for start and end for corresponding slot are not present they are treated as active
    def get_uptime_downtime(self, qstart: dt, qend: dt) -> tuple[datetime.timedelta, datetime.timedelta]:
        uptime = datetime.timedelta()
        downtime = datetime.timedelta()
        tottime = self.get_segment_intersection(qstart, qend, self.start, self.end)
        if qstart>self.end or qend<self.start:
            return uptime, downtime
        for i in range(len(self.ob_pts)-1):
            # i, i+1
            if self.ob_pts[i][1]==self.ob_pts[i][1]:
                if self.ob_pts[i][1]:
                    uptime += self.get_segment_intersection(self.ob_pts[i][0], self.ob_pts[i+1][0], qstart, qend)
            elif self.presum[i+1].seconds==0 or self.sufsum[i+2].seconds==0:
                if self.ob_pts[i][1]:
                    uptime += self.get_segment_intersection(self.ob_pts[i][0], self.ob_pts[i][0]+(self.ob_pts[i+1][0]-self.ob_pts[i][0])/2, qstart, qend)
                else:
                    uptime += self.get_segment_intersection(self.ob_pts[i][0]+(self.ob_pts[i+1][0]-self.ob_pts[i][0])/2, self.ob_pts[i+1][0], qstart, qend)
            else:
                r1 = self.presum[i+1].seconds
                r2 = self.sufsum[i+2].seconds
                if self.ob_pts[i][1]:
                    uptime += self.get_segment_intersection(self.ob_pts[i][0], self.ob_pts[i][0] + (self.ob_pts[i+1][0]-self.ob_pts[i][0])/(r1+r2)*r1, qstart, qend)
                else:
                    uptime += self.get_segment_intersection(self.ob_pts[i][0] + (self.ob_pts[i+1][0]-self.ob_pts[i][0])/(r1+r2)*r1, self.ob_pts[i+1][0], qstart, qend)
        return uptime, tottime-uptime

                
    def get_segment_intersection(self, qstart: dt, qend: dt, start: dt, end: dt) -> datetime.timedelta:
        return min(qend, end) - max(qstart, start)




class Store:
    def __init__(self, store_id, timezone: str = 'America/Chicago'):
        self.store_id = store_id
        self.timezone = timezone
        self.menu_hours: list[tuple[dt, dt]] = [(dt.strptime("00:00:00", "%H:%M:%S"), dt.strptime("23:59:59", "%H:%M:%S"))]*7
        self.status: list[tuple(dt, bool)] = []
        self.slots: list[MenuSlot] = []

    # converts local time slots(menu_hours) to utc time slots for previous 10 days (8days for the last week & +-1 day for local to utc conversion) [cur_date-8, cur_date+1]
    def normalize(self, cur_dt: dt):
        for i in range(-1, 9):
            new_dt: dt = cur_dt + i*datetime.timedelta(days=1)
            (start, end) = self.menu_hours[days[new_dt.strftime('%A')]]
            date_str = new_dt.strftime('%Y-%m-%d')
            self.slots.append(MenuSlot(dt.strptime(f"{date_str} {start.strftime('%H:%M:%S.%f')}", "%Y-%m-%d %H:%M:%S.%f"), dt.strptime(f"{date_str} {end.strftime('%H:%M:%S.%f')}", "%Y-%m-%d %H:%M:%S.%f"), self.timezone))

    # add store status data from 1st csv
    def add_status_data(self, timestamp: dt, is_active: bool):
        if timestamp<self.slots[0].start or timestamp>self.slots[-1].end:
            return
        for slot in self.slots:
            if timestamp>=slot.start and timestamp<=slot.end:
                slot.add_observation_pt(timestamp, is_active)
                return
            
    def sort_obs_pts(self):
        for slot in self.slots:
            slot.ob_pts.sort(key=lambda x: x[0])

    def pre_compute(self):
        for slot in self.slots:
            slot.pre_compute()

    def cal_uptime_downtime(self, start: dt, end: dt) -> tuple[datetime.timedelta, datetime.timedelta]:
        start = pytz.UTC.localize(start)
        end = pytz.UTC.localize(end)
        uptime = datetime.timedelta()
        downtime = datetime.timedelta()
        for slot in self.slots:
            cur_uptime, cur_downtime = slot.get_uptime_downtime(start, end)
            uptime += cur_uptime
            downtime += cur_downtime
        return uptime, downtime




stores: dict[str, Store] = {}
    

# how time is interpreted?
# last hour - last 60 minutes
# last day - the entire previous day
# last week - the complete week prior to current week

def generate_report(report_id: str):
    session = db.SESSION()
    timezones = session.query(Timezones).all()
    for timezone in timezones:
        store_id = timezone.store_id
        stores[store_id] = Store(store_id, timezone.timezone_str)
    timezones = None

    menu_hours = session.query(MenuHours).all()
    for menu_hour in menu_hours:
        store_id = menu_hour.store_id
        if store_id not in stores:
            stores[store_id] = Store(store_id)
        stores[store_id].menu_hours[menu_hour.day] = (menu_hour.start_time_local, menu_hour.end_time_local)
    menu_hours = None

    cur_timestamp: dt = session.query(func.max(StoreStatus.timestamp_utc)).first()[0]
    for store_id in session.query(StoreStatus.store_id).distinct().all():
        if store_id[0] not in stores:
            stores[store_id[0]] = Store(store_id)

    for _, store in stores.items():
        store.normalize(cur_timestamp)

    for store_status in session.query(StoreStatus).all():
        store_id = store_status.store_id
        stores[store_id].add_status_data(pytz.UTC.localize(store_status.timestamp_utc), store_status.status)

    for _, store in stores.items():
        store.sort_obs_pts()

    df = pd.DataFrame(columns=['store_id', 'uptime_last_hour(in minutes)', 'uptime_last_day(in hours)', 'uptime_last_week(in hours)', 'downtime_last_hour(in minutes)', 'downtime_last_day(in hours)', 'downtime_last_week(in hours)'])

    for _, store in stores.items():
        store.pre_compute()

    for store_id in stores:
        last_day_start = dt.strptime(f"{cur_timestamp.strftime('%Y-%m-%d')}", "%Y-%m-%d") - datetime.timedelta(days=1)
        last_day_end = last_day_start + datetime.timedelta(hours=23, minutes=59, seconds=59, microseconds=999999)
        cur_date = dt.strptime(f"{cur_timestamp.strftime('%Y-%m-%d')}", "%Y-%m-%d")
        last_week_start = cur_date - datetime.timedelta(days=days[cur_date.strftime('%A')], weeks=1)
        last_week_end = last_week_start + datetime.timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
        last_hour_uptime, last_hour_downtime = stores[store_id].cal_uptime_downtime(cur_timestamp-datetime.timedelta(hours=1), cur_timestamp)
        last_day_uptime, last_day_downtime = stores[store_id].cal_uptime_downtime(last_day_start, last_day_end)
        last_week_uptime, last_week_downtime = stores[store_id].cal_uptime_downtime(last_week_start, last_week_end)
        df.loc[len(df.index)] = [store_id, last_hour_uptime.seconds//60, last_day_uptime.seconds//3600, last_week_uptime.seconds//3600, last_hour_downtime.seconds//60, last_day_downtime.seconds//3600, last_week_downtime.seconds//3600]

    df.to_csv('data.csv') 
    rdb.endReportProcessing(report_id)
    