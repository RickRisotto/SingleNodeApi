from flask_restful import Api, reqparse, Resource
from flask import Flask
import sqlite3
import pandas as pd
from flask import render_template
from flask_cors import cross_origin

app = Flask(__name__)

api = Api(app)


class Getter(Resource):
    @api.representation('/index.html')
    @cross_origin()
    def get(self):
        connect = sqlite3.connect('adjust.db')
        file = open('/PATH/TO/FILE.csv')
        file_read = pd.read_csv(file)
        file_read.to_sql('dataset', connect, if_exists='replace', index=False)
        data = pd.read_sql_query(''' SELECT group_concat (distinct date) as date, group_concat 
        (distinct channel) as channel, group_concat (distinct country) as country,
        group_concat (distinct os) as os FROM dataset ''', connect)
        col_names = pd.read_sql_query(''' SELECT * FROM dataset where 1=0 ''', connect)
        col_names = col_names.drop(columns=['date', 'channel', 'country', 'os'])
        col_names['CPI'] = ''  # add 'CPI' column
        data = data.to_dict()
        lst_from_str = [v.split(',') for val in data.values() for v in val.values()]
        keys = [i for i in data.keys()]
        res = dict(zip(keys, lst_from_str))
        res['col_names'] = col_names.columns
        connect.close()
        return render_template('/index.html', data=res)

    @staticmethod
    def parse_add() -> reqparse.RequestParser:
        parser = reqparse.RequestParser()
        parser.add_argument('date_from')
        parser.add_argument('date_to')
        parser.add_argument('channel')
        parser.add_argument('country')
        parser.add_argument('os')
        parser.add_argument('gr_by', action='append')
        parser.add_argument('sort_by')
        parser.add_argument('arrange_as')
        return parser

    @staticmethod
    def filter_logic(file_read, args___):
        df_dt = [file_read['date'][it] for it in (0, len(file_read['date']) -1)]
        file_read = file_read.set_index(['date'], drop=True)

        if args___['date_from'] == '1':
            args___['date_from'] = df_dt[0]

        if args___['date_to'] == '1':
            args___['date_to'] = df_dt[1]

        if args___['sort_by'] == '1':
            args___['sort_by'] = 'date'

        group_by = ['date', 'channel', 'country', 'os']
        sort_by = ['clicks', 'impressions', 'installs', 'spend', 'revenue', 'CPI']
        if args___['gr_by'] == ['1']:
            args___['gr_by'] = group_by+sort_by
        else:
            choice = list(set(sort_by + args___['gr_by']))
            args___['gr_by'] = choice

        if args___['arrange_as'] == 'asc':
            args___['arrange_as'] = True
        else:
            args___['arrange_as'] = False

        if args___['date_to'] < args___['date_from']:
            middllevar = args___['date_from']
            args___['date_from'] = args___['date_to']
            args___['date_to'] = middllevar

        data = file_read.loc[args___['date_from']: args___['date_to']]

        args = (dict(list(args___.items())[2:5]))

        res = data[[i for i in args.keys() if args[i] == '1']]
        key_lst = [k for k in args.keys() if args[k] != '1']
        if len(key_lst) > 0:
            for i in key_lst:
                data = data.loc[(data[i] == args[i]) & (data[i] == args[i])]
                data = data

        concat = pd.concat([res, data]).dropna()

        concat['CPI'] = round(concat['spend'].divide(concat['installs']), 3)

        result = concat.groupby(args___['gr_by']).size().reset_index()
        result = result.sort_values(
               by=args___['sort_by'], ascending=args___['arrange_as'])

        result = result.to_html()

        return result

    @api.representation('/result.html')
    @cross_origin()
    def post(self):
        parser = Getter.parse_add()
        args = parser.parse_args()
        file = open('/PATH/TO/FILE.csv')
        file_read = pd.read_csv(file)
        data = Getter.filter_logic(file_read, args)
        return render_template('/result.html', data=data)


api.add_resource(Getter, '/get')

if __name__ == '__main__':
    app.run()

























