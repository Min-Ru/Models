import pandas as pd
import os
import sys
import pymysql
from dotenv import load_dotenv
import datetime as dt
from datetime import datetime
import pmdarima as pm
sys.path.append("../")  # noqa
from send_post import notify_message
load_dotenv()
today = dt.date.today()


# 讀取/寫入資料庫
def db_operate(sql, db_type, operate_type, df):
    if db_type == "rms":
        db = pymysql.connect(host=os.getenv('DB_HOST'), port=3306,
                             user=os.getenv('DB_USERNAME'),
                             passwd=os.getenv('DB_PASSWORD'),
                             db=os.getenv('DB_NAME'), charset='utf8')
    elif db_type == "pms":
        db = pymysql.connect(host=os.getenv('DB_HOST_pms'),
                             port=3307, user=os.getenv('DB_USERNAME_pms'),
                             passwd=os.getenv('DB_PASSWORD_pms'),
                             db=os.getenv('DB_NAME_pms'), charset='utf8')
    cursor = db.cursor()
    if operate_type == "search":
        cursor.execute(sql)
        result = cursor.fetchall()
        col_names = [name[0] for name in cursor.description]
        result_pd = pd.DataFrame(list(result), columns=col_names)
        db.close()
        return result_pd
    else:
        cursor.executemany(sql, df.values.tolist())
        db.commit()
        db.close()


# 群組回報內容
def report(immigration_type):
    immigration_txt = '入' if immigration_type == '1' else '出'

    def generate_weekly_report(prefix, weeks):
        report_text = f"{prefix}四週機場{immigration_txt}境人數"
        for week in weeks:
            start, end, avg = week
            report_text += f"\n◈ {start} ~ {end}約 {avg} 人"
        return report_text
    # 過去四週
    past_weeks = [
        (past_week_start1, past_week_end1, past_week_avg1),
        (past_week_start2, past_week_end2, past_week_avg2),
        (past_week_start3, past_week_end3, past_week_avg3),
        (past_week_start4, past_week_end4, past_week_avg4)
    ]
    # 未來四週
    future_weeks = [
        (future_week_start1, future_week_end1, future_week_avg1),
        (future_week_start2, future_week_end2, future_week_avg2),
        (future_week_start3, future_week_end3, future_week_avg3),
        (future_week_start4, future_week_end4, future_week_avg4)
    ]

    past_report = generate_weekly_report("過去", past_weeks)
    future_report = generate_weekly_report("未來", future_weeks)

    notify = f":\n{past_report}\n-----------------------\n{future_report}"

    print(notify)
    # 【機場出入境人數】
    notify_message(notify, 389)  # 產品專案:389
    notify_message(notify, 390)  # 資訊研發分享群組:390


# 預測人數
def pred_count(data):
    # 去除今日人數
    data = data[:-1]
    dataset = data["total"]
    # Build Model
    model = pm.auto_arima(dataset, start_p=1, start_q=1,
                          information_criterion='aic',
                          test='adf',        # use adftest to find optimal 'd'
                          max_p=4, max_q=4,  # maximum p and q
                          m=1,               # frequency of series
                          d=None,            # let model determine 'd'
                          seasonal=False,    # No Seasonality
                          start_P=0,
                          D=0,
                          trace=True,
                          error_action='ignore',
                          suppress_warnings=True,
                          stepwise=True)
    fitted = model.fit(dataset)
    # Forecast
    fc = fitted.predict(28)
    # Make as pandas series
    pred = pd.Series(fc, index=range(len(dataset), (len(dataset)+28)))
    return pred


try:
    reporting = sys.argv[1]  # 1為通報
    immigration_type = sys.argv[2]  # 1為入境, 5為出境, 3為過境
except Exception:
    sys.exit()


# 搜尋過去實際出入境人數資料
prev_data = db_operate("select date, airport from `airport_records` where " +
                       "`inOutTransit`= " + str(immigration_type),
                       "rms", "search", None)
prev_data['date'] = pd.to_datetime(
    prev_data['date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
prev_data = prev_data.groupby("date").agg('count')
prev_data['date'] = prev_data.index
prev_data.columns = ['total', 'date']
prev_data = prev_data[['date', 'total']]
prev_data = prev_data.reset_index(drop=True)
prev_data.sort_values(by='date')
prev_data = prev_data.reset_index(drop=True)

# 預測未來28天
pred = pred_count(prev_data)

# 整理預測資料
future_data = pd.DataFrame({
    'date': [(today + dt.timedelta(days=days)).strftime("%Y-%m-%d") for days in range(len(pred))],  # noqa
    # 南/中/北部機場
    'south': None, 'central': None, 'north': None,
    # 全台
    'total': [int(val) for val in pred],
    # 出入境別(1:入,5:出,3:過境)
    'type': immigration_type,
    # 該天預測值第幾次被預測
    'frequency': list(range(len(pred), 0, -1)),
    'created_date': str(today)
    })


# creating column list for insertion
cols = ",".join([str(i) for i in future_data.columns.tolist()])
# 將預測資料放入資料表 airport_predictions
db_operate("insert into airport_predictions (" + cols +
           ") VALUES (" + "%s," * (len(future_data.columns) - 1) + "%s)",
           "rms", "insert", future_data)

# 通報
if reporting == '1':

    past_day_start = datetime.strftime(
        today + dt.timedelta(days=-28), '%Y-%m-%d')
    past_day_end = datetime.strftime(
        today + dt.timedelta(days=-1), '%Y-%m-%d')

    # 統計未來四週預測總人數、過去四週總人數
    def count_4week_people(day_start, day_end, df):

        week_start = today + dt.timedelta(days=day_start)
        week_end = today + dt.timedelta(days=day_end)
        # 利用txt判斷要計算未來的資料或是過去的資料
        week_avg = int(df[df.date.between(
            str(week_start), str(week_end))].total.sum())
        # 數值千位以上，標上「 , 」
        week_avg = f'{week_avg:,d}'
        week_start = week_start.strftime('%m/%d')[-5:].replace('-', '/')
        week_end = week_end.strftime('%m/%d')[-5:].replace('-', '/')

        return week_start, week_end, week_avg

    future_week_start1, future_week_end1, future_week_avg1 = count_4week_people( #noqa
        0, 6, future_data)
    future_week_start2, future_week_end2, future_week_avg2 = count_4week_people( #noqa
        7, 13, future_data)
    future_week_start3, future_week_end3, future_week_avg3 = count_4week_people( #noqa
        14, 20, future_data)
    future_week_start4, future_week_end4, future_week_avg4 = count_4week_people( #noqa
        21, 27, future_data)

    past_week_start1, past_week_end1, past_week_avg1 = count_4week_people(
        -7, -1, prev_data)
    past_week_start2, past_week_end2, past_week_avg2 = count_4week_people(
        -14, -8, prev_data)
    past_week_start3, past_week_end3, past_week_avg3 = count_4week_people(
        -21, -15, prev_data)
    past_week_start4, past_week_end4, past_week_avg4 = count_4week_people(
        -28, -22, prev_data)
    report(immigration_type)
