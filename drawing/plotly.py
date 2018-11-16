# coding=utf-8

from typing import List, Dict
from datetime import datetime

import plotly.graph_objs as go
import plotly

from core.drawing import Drawing


class PlotlyDrawing(Drawing):
    def __init__(self) -> None:
        super().__init__()

        self.__traces: List[go.Scatter] = list()

    def add_trace(self, name: str, data: Dict[int, int]) -> None:
        x = data.keys()
        y = data.values()
        trace = go.Scatter(
            name=name,
            #x=[datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S') for t in x],
            x=list(x),
            y=list(y),
        )
        self.__traces.append(trace)
        self.__traces.append(trace)

    def draw(self):
        plotly.offline.plot(
            self.__traces, filename='data/test.html',
            #auto_open=False, include_plotlyjs=False, output_type='div',
        )
        return 1
