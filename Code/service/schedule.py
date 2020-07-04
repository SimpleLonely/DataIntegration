from flask import Flask, request
from datetime import date


def get_date():
    date_string = date.today().strftime("%Y%m%d")
    return date.today().strftime("%Y%m%d")


class ScheduleConfig(object):
    JOBS = [
        {
            'id': 'get_daily_line',
            'func': 'data.getInfo:get_daily_line',
            'args': [get_date()],
            'trigger': 'cron',
            'hour': 0
        }, {
            'id': 'get_daily_base',
            'func': 'data.getInfo:get_daily_base',
            'args': [get_date()],
            'trigger': 'cron',
            'hour': 0
        }, {
            'id': 'get_daily_yields_history',
            'func': 'data.get_bond_yields:get_daily_yields_history',
            'args': [get_date()],
            'trigger': 'cron',
            'hour': 0
        }, {
            'id': 'cal_daily_factor',
            'func': 'service.calculate_daily:cal_daily_factor',
            'args': [get_date()],
            'trigger': 'cron',
            'hour': 0
        },{
            'id': 'back_test',
            'func': 'service.ff_model:get_backtest_rate',
            'trigger': 'cron',
            'hour': 0
        }
    ]


def test():
    print("hello")
