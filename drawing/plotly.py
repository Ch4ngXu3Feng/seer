# coding=utf-8

from typing import List, Dict, Union, Type

from numpy import ndarray
import plotly.graph_objs as go
import plotly

from core.drawing import Drawing


class PlotlyDrawing(Drawing):
    def __init__(self) -> None:
        super().__init__()
        self.__traces: List[Union[go.Scatter, go.Box]] = list()

    def __len__(self) -> int:
        return len(self.__traces)

    def add_scatter(self, name: str, data: Dict[int, int]) -> None:
        x = sorted(data.keys())
        y = [data[k] for k in x]

        # from datetime import datetime
        # x=[datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S') for t in x],
        trace = go.Scatter(name=name, x=x, y=y)
        self.__traces.append(trace)

    def add_box(self, name: str, data: Type[ndarray]) -> None:
        trace = go.Box(
            name=str(name),
            y=data,
            marker=dict(color='rgb(7,40,89)'),
            line=dict(color='rgb(7,40,89)'),
            boxmean='sd'
        )
        self.__traces.append(trace)

    def test(self) -> None:
        plotly.offline.plot(
            self.__traces, filename='data/test.html',
        )

    def draw(self) -> str:
        return plotly.offline.plot(
            self.__traces,
            auto_open=False,
            include_plotlyjs=False,
            output_type='div'
        )
