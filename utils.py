import datetime


def today_date() -> str:
    """日本の時刻を取得する"""
    # 日本の時差
    JST = datetime.timezone(datetime.timedelta(hours=+9))
    # 現在のJSTの時間を取得
    now_jst = datetime.datetime.now(JST)
    # 年月日までを文字列でフォーマット
    formatted_date_time = now_jst.strftime("%Y-%m-%d %H:%M")
    return formatted_date_time
