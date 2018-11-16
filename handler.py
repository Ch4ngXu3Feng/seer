# coding=utf-8

import os
import sqlite3 as sql
import tornado.web
import pandas as pd
import plotly.graph_objs as go
import plotly


class PlotHandler:
    def create_html(self) -> str:
        conn = sql.connect('C:\\changxuefeng\\repo\\fork\\scrapy-tutorial\\douban.db')
        with conn:
            query = "SELECT movie_name, ranking FROM douban6 LIMIT 50;"
            df = pd.read_sql_query(query, conn)
            for name in df['movie_name']:
                print(name)
            group = df.groupby(['movie_name'])['ranking'].sum()
            print(repr(group))

            labels = df['movie_name']
            values = group
            #labels = ['Oxygen','Hydrogen','Carbon_Dioxide','Nitrogen']
            #values = [4500,2500,1053,500]
            trace = go.Pie(labels=labels, values=values)
            #data = [trace]
            #plotly.offline.plot(data)
            #import plotly.plotly as py
            #py.iplot(data, filename='basic_pie_chart')

            data = [trace]
            #filename='douban.html',
            return plotly.offline.plot(
                data, auto_open=False, include_plotlyjs=False, output_type='div'
            )

            """
            from plotly.offline.offline import _plot_html
            data_or_figure = [trace]
            #plot_html, plotdivid, width, height = _plot_html(
            #    data_or_figure, False, "", True, '100%', 525)
            plot_html, plotdivid, width, height = _plot_html(
                data_or_figure, {}, False, '100%', 525, True)
            print(plot_html)
            self.set_status(200)
            self.write(
                <html>
                <head>
                  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                </head>
                <body>
                %s
                </body>
                </html>
                % plot_html
            )
            """


class DoubanHandler(tornado.web.RequestHandler):
    def get(self) -> None:
        plot = PlotHandler()
        html = plot.create_html()
        #f = open("douban.html", "r")
        #f.read())
        self.write(
            """
            <html>
            <head>
              <script src="/static/js/plotly-latest.min.js"></script>
            </head>
            <body>
              %s
            </body>
            </html>
            """ % html
        )

        #self.write(html)
        self.set_status(200)


class DoubanApp(tornado.web.Application):
    def __init__(self) -> None:
        handlers = [
            (
                r'/douban/top250',
                DoubanHandler
            ),
        ]

        settings = dict(
            debug=False,
            static_path=os.path.join(os.path.dirname(__file__), "static"),
        )

        tornado.web.Application.__init__(self, handlers, **settings)
