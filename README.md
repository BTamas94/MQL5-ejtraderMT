# ejtraderMT MQL5 Expert

### Development state: stable release version 3.0.3 compatible

Tested on macOS Mojave / Windows 10 in Parallels Desktop container.

Tested on Manjaro Linux / Windows 10 in VirtualBox

Working in production on Debian 10 / Wine 4.

## Table of Contents

- [About the Project](#about-the-project)
- [Installation](#installation)
- [Documentation](#documentation)
- [Usage](#usage)
- [Live data and streaming events](#live-data-and-streaming-events)
- [Streaming MT5 indicator data](#streaming-mt5-indicator-data)
- [Plot values to MT5 charts](#plot-values-to-mt5-charts)
- [The JsonAPIIndicator](#the-jsonapiindicator)
- [Error handling](#error-handling)
- [License](#license)

## About the Project

This project was developed to work as a server for the Backtrader Python trading framework. It is based on ZeroMQ sockets and uses JSON format to communicate. But now it has grown to the independent project. You can use it with any programming language that has [ZeroMQ binding](http://zeromq.org/bindings:_start).

Backtrader Python client is located here: [Python Backtrader - Metaquotes MQL5 ](https://github.com/khramkov/Backtrader-MQL5-API)

Thanks to the participation of [Gunther Schulz](https://github.com/Gunther-Schulz), the project moved to a new level.

New features:

- Support for multiple datastreams in parallel for any combination of symbols and timeframes independently of the timeframe and symbol of the attached chart
- Support for tick data
- Support for direct download as CSV files
- Automatic retry binding to sockets. When running under Wine in Linux, sockets will be blocked for 60 seconds if closed uncleanly. This can happen if the client is still connected while the EA gets reloaded.
- Skip re-initialization on chart timeframe change
- Support for spread data (ask/bid)
- Support for plotting to charts in MT5 by streaming values from the client
- Support for processing client data with MT5 indicators

In development:

- Devitation
- Stop limit orders
- Drawing of chart objects

## Installation

1. Install ZeroMQ for MQL5 [https://github.com/dingmaotu/mql-zmq](https://github.com/dingmaotu/mql-zmq)
2. Put the following files from this repo to your MetaEditor Iinclude` directory
   - `Include/Json.mqh`
   - `Include/controlerrors.mqh`
   - `Include/StringToEnumInt.mqh`
3. Put the `Indicators/JsonAPIIndicator.mq5` file from this repo to your MetaEditor `Indicators` directory
4. Download and compile `experts/JsonAPI.mq5` script.
5. Check if Metatrader 5 automatic trading is allowed.
6. Attach the `JsonAPI.mq5` script to a chart in Metatrader 5.
7. Allow DLL import in dialog window.
8. Check if the ports are free to use. (default:`15555`,`15556`, `15557`,`15558`, `15559`, `15560`,`15562`)

## Documentation

The script uses seven ZeroMQ sockets:

1. `System socket` - Recives requests from client and replies 'OK'.
2. `Data socket` - Pushes data to client depending on the request via System socket.
3. `Live socket` - Automatically pushes last candle when it closes.
4. `Streaming socket` - Automatically pushes last transaction info every time it happens.
5. `Indicator data socket` - automatically pushes indicator result values to the client.
6. `Chart Data Socket` - Recieves values to be plotted to a specific chart.
7. `Chart Indicator Socket` - Only for internal communication. Passes values to be plotted TO the supplied JsonAPIIndicator indicator

The idea is to send requests via `System socket` and recieve results/errors via `Data socket`. Event handlers should be created for `Live socket` and `Streaming socket` because the server sends data to theese sockets automatically. See examples in [Live data and streaming events](#live-data-and-streaming-events) section.

`System socket` request uses default JSON dictionary:

```
{
	"action": null,
	"actionType": null,
	"symbol": null,
	"chartTF": null,
	"fromDate": null,
	"toDate": null,
	"id": null,
	"magic": null,
	"volume": null,
	"price": null,
	"stoploss": null,
	"takeprofit": null,
	"expiration": null,
	"deviation": null,
	"comment": null,
    "chartId": None,
    "indicatorChartId": None,
    "chartIndicatorSubWindow": None,
    "style": None,
}
```

Check out the available combinations of `action` and `actionType`:

| action    | actionType            | Description                       |
| --------- | --------------------- | --------------------------------- |
| CONFIG    | null                  | Set script configuration          |
| RESET     | null                  | Reset subscribed symbols          |
| ACCOUNT   | null                  | Get account settings              |
| BALANCE   | null                  | Get current balance               |
| POSITIONS | null                  | Get current open positions        |
| ORDERS    | null                  | Get current open orders           |
| INDICATOR | ATTACH                | Attach an indicator and return ID |
| INDICATOR | REQUEST               | Get indicator data                |
| CHART     | OPEN                  | Open a new chart window           |
| CHART     | ADDINDICATOR          | Attach JsonAPIIndicator indicator |
| HISTORY   | DATA                  | Get data history                  |
| HISTORY   | TRADES                | Get trades history                |
| HISTORY   | WRITE                 | Download history data as CSV      |
| TRADE     | ORDER_TYPE_BUY        | Buy market                        |
| TRADE     | ORDER_TYPE_SELL       | Sell market                       |
| TRADE     | ORDER_TYPE_BUY_LIMIT  | Buy limit                         |
| TRADE     | ORDER_TYPE_SELL_LIMIT | Sell limit                        |
| TRADE     | ORDER_TYPE_BUY_STOP   | Buy stop                          |
| TRADE     | ORDER_TYPE_SELL_STOP  | Sell stop                         |
| TRADE     | POSITION_MODIFY       | Position modify                   |
| TRADE     | POSITION_PARTIAL      | Position close partial            |
| TRADE     | POSITION_CLOSE_ID     | Position close by id              |
| TRADE     | POSITION_CLOSE_SYMBOL | Positions close by symbol         |
| TRADE     | ORDER_MODIFY          | Order modify                      |
| TRADE     | ORDER_CANCEL          | Order cancel                      |

Python 3 API class example:

```python
import zmq

class MTraderAPI:
    def __init__(self, host=None):
        self.HOST = host or 'localhost'
        self.SYS_PORT = 15555       # REP/REQ port
        self.DATA_PORT = 15556      # PUSH/PULL port
        self.LIVE_PORT = 15557      # PUSH/PULL port
        self.EVENTS_PORT = 15558    # PUSH/PULL port
        self.INDICATOR_DATA_PORT = 15559  # REP/REQ port
        self.CHART_DATA_PORT = 15560  # PUSH port

        # ZeroMQ timeout in seconds
        sys_timeout = 1
        data_timeout = 10

        # initialise ZMQ context
        context = zmq.Context()

        # connect to server sockets
        try:
            self.sys_socket = context.socket(zmq.REQ)
            # set port timeout
            self.sys_socket.RCVTIMEO = sys_timeout * 1000
            self.sys_socket.connect('tcp://{}:{}'.format(self.HOST, self.SYS_PORT))

            self.data_socket = context.socket(zmq.PULL)
            # set port timeout
            self.data_socket.RCVTIMEO = data_timeout * 1000
            self.data_socket.connect('tcp://{}:{}'.format(self.HOST, self.DATA_PORT))

            self.indicator_data_socket = context.socket(zmq.PULL)
            # set port timeout
            self.indicator_data_socket.RCVTIMEO = data_timeout * 1000
            self.indicator_data_socket.connect(
                "tcp://{}:{}".format(self.HOST, self.INDICATOR_DATA_PORT)
            )
            self.chart_data_socket = context.socket(zmq.PUSH)
            # set port timeout
            # TODO check if port is listening and error handling
            self.chart_data_socket.connect(
                "tcp://{}:{}".format(self.HOST, self.CHART_DATA_PORT)
            )

        except zmq.ZMQError:
            raise zmq.ZMQBindError("Binding ports ERROR")

    def _send_request(self, data: dict) -> None:
        """Send request to server via ZeroMQ System socket"""
        try:
            self.sys_socket.send_json(data)
            msg = self.sys_socket.recv_string()
            # terminal received the request
            assert msg == 'OK', 'Something wrong on server side'
        except AssertionError as err:
            raise zmq.NotDone(err)
        except zmq.ZMQError:
            raise zmq.NotDone("Sending request ERROR")

    def _pull_reply(self):
        """Get reply from server via Data socket with timeout"""
        try:
            msg = self.data_socket.recv_json()
        except zmq.ZMQError:
            raise zmq.NotDone('Data socket timeout ERROR')
        return msg

    def _indicator_pull_reply(self):
        """Get reply from server via Data socket with timeout"""
        try:
            msg = self.indicator_data_socket.recv_json()
        except zmq.ZMQError:
            raise zmq.NotDone("Indicator Data socket timeout ERROR")
        if self.debug:
            print("ZMQ INDICATOR DATA REPLY: ", msg)
        return msg

    def live_socket(self, context=None):
        """Connect to socket in a ZMQ context"""
        try:
            context = context or zmq.Context.instance()
            socket = context.socket(zmq.PULL)
            socket.connect('tcp://{}:{}'.format(self.HOST, self.LIVE_PORT))
        except zmq.ZMQError:
            raise zmq.ZMQBindError("Live port connection ERROR")
        return socket

    def streaming_socket(self, context=None):
        """Connect to socket in a ZMQ context"""
        try:
            context = context or zmq.Context.instance()
            socket = context.socket(zmq.PULL)
            socket.connect('tcp://{}:{}'.format(self.HOST, self.EVENTS_PORT))
        except zmq.ZMQError:
            raise zmq.ZMQBindError("Data port connection ERROR")
        return socket

    def _push_chart_data(self, data: dict) -> None:
        """Send message for chart control to server via ZeroMQ chart data socket"""
        try:
            if self.debug:
                print("ZMQ PUSH CHART DATA: ", data, " -> ", data)
            self.chart_data_socket.send_json(data)
        except zmq.ZMQError:
            raise zmq.NotDone("Sending request ERROR")

    def construct_and_send(self, **kwargs) -> dict:
        """Construct a request dictionary from default and send it to server"""

        # default dictionary
        request = {
            "action": None,
            "actionType": None,
            "symbol": None,
            "chartTF": None,
            "fromDate": None,
            "toDate": None,
            "id": None,
            "magic": None,
            "volume": None,
            "price": None,
            "stoploss": None,
            "takeprofit": None,
            "expiration": None,
            "deviation": None,
            "comment": None,
            "chartId": None,
            "indicatorChartId": None,
            "chartIndicatorSubWindow": None,
            "style": None,
        }

        # update dict values if exist
        for key, value in kwargs.items():
            if key in request:
                request[key] = value
            else:
                raise KeyError('Unknown key in **kwargs ERROR')

        # send dict to server
        self._send_request(request)

        # return server reply
        return self._pull_reply()

    def indicator_construct_and_send(self, **kwargs) -> dict:
        """Construct a request dictionary from default and send it to server"""

        # default dictionary
        request = {
            "action": None,
            "actionType": None,
            "id": None,
            "symbol": None,
            "chartTF": None,
            "fromDate": None,
            "toDate": None,
            "name": None,
            "params": None,
            "linecount": None,
        }

        # update dict values if exist
        for key, value in kwargs.items():
            if key in request:
                request[key] = value
            else:
                raise KeyError("Unknown key in **kwargs ERROR")

        # send dict to server
        self._send_request(request)

        # return server reply
        return self._indicator_pull_reply()

    def chart_data_construct_and_send(self, **kwargs) -> dict:
        """Construct a request dictionary from default and send it to server"""

        # default dictionary
        message = {
            "action": None,
            "actionType": None,
            "chartId": None,
            "indicatorChartId": None,
            "data": None,
        }

        # update dict values if exist
        for key, value in kwargs.items():
            if key in message:
                message[key] = value
            else:
                raise KeyError("Unknown key in **kwargs ERROR")

        # send dict to server
        self._push_chart_data(message)
```

## Usage

All examples will be on Python 3. Lets create an instance of MetaTrader API class:

```python
api = MTraderAPI()
```

First of all we should configure the script `symbol` and `timeframe`. Live data stream will be configured to the same params. You can use any number of `symbols` and `timeframes`. The server subscribes to these sembols and will transmit them through the `Live data` socket

```python
print(api.construct_and_send(action="CONFIG", symbol="EURUSD", chartTF="M5"))
print(api.construct_and_send(action="CONFIG", symbol="AUDUSD", chartTF="M1"))
...
```

There is also `tick` data. You can subscribe for `tick` and `candle` data at the same `symbol`.

```python
print(api.construct_and_send(action="CONFIG", symbol="EURUSD", chartTF="TICK"))
print(api.construct_and_send(action="CONFIG", symbol="EURUSD", chartTF="M1"))
```

If you want to stop `Live data`, you should reset server subscriptions.

```python
rep = api.construct_and_send(action="RESET")
print(rep)
```

Get information about the trading account.

```python
rep = api.construct_and_send(action="ACCOUNT")
print(rep)
```

Get historical data. `fromDate` should be in timestamp format. The data will be loaded to the last candle if `toDate` is `None`. Notice, that the script sends the last unclosed candle too. You should delete it manually.

There are some issues:

- MetaTrader keeps historical data in cache. But when you make a request for the first time, MetaTrader downloads the data from a broker. This operation can exceed `Data socket` timeout. It depends on your broker. Second request will be handeled quickly.
- It takes 6-7 seconds to process `50000` M1 candles. It was tested on Windows 10 in Parallels Desktop container with 4 cores and 4GB RAM. So if you need more data there are three ways to handle it. 1) Increase `Data socket` timeout. 2) You can load data partially using `fromDate` and `toDate`. 3) You can use more powerfull hardware.

```python
rep = api.construct_and_send(action="HISTORY", actionType="DATA", symbol="EURUSD", chartTF="M5", fromDate=1555555555)
print(rep)
```

History data reply example:

```
{'data': [[1560782340, 1.12271, 1.12288, 1.12269, 1.12277, 46.0],[1560782400, 1.12278, 1.12299, 1.12276, 1.12297, 43.0],[1560782460, 1.12296, 1.12302, 1.12293, 1.123, 23.0]]}
```

Buy market order.

```python
rep = api.construct_and_send(action="TRADE", actionType="ORDER_TYPE_BUY", symbol="EURUSD", "volume"=0.1, "stoploss"=1.1, "takeprofit"=1.3)
print(rep)
```

Sell limit order. Remember to switch SL/TP depending on BUY/SELL, or you will get `invalid stops` error.

- BUY: SL < price < TP
- SELL: SL > price > TP

```python
rep = api.construct_and_send(action="TRADE", actionType="ORDER_TYPE_SELL_LIMIT", symbol="EURUSD", "volume"=0.1, "price"=1.2, "stoploss"=1.3, "takeprofit"=1.1)
print(rep)
```

All pending orders are set to `Good till cancel` by default. If you want to set an expiration date, pass the date in timestamp format to `expiration` param.

```python
rep = api.construct_and_send(action="TRADE", actionType="ORDER_TYPE_SELL_LIMIT", symbol="EURUSD", "volume"=0.1, "price"=1.2, "expiration"=1560782460)
print(rep)
```

## Live data and streaming events

Event handler example for `Live socket` and `Data socket`.

```python
import zmq
import threading

api = MTraderAPI()


def _t_livedata():
    socket = api.live_socket()
    while True:
        try:
            last_candle = socket.recv_json()
        except zmq.ZMQError:
            raise zmq.NotDone("Live data ERROR")
        print(last_candle)


def _t_streaming_events():
    socket = api.streaming_socket()
    while True:
        try:
            trans = socket.recv_json()
            request, reply = trans.values()
        except zmq.ZMQError:
            raise zmq.NotDone("Streaming data ERROR")
        print(request)
        print(reply)



t = threading.Thread(target=_t_livedata, daemon=True)
t.start()

t = threading.Thread(target=_t_streaming_events, daemon=True)
t.start()

while True:
    pass
```

There are only two variants of `Live socket` data. When everything is ok, the script sends subscribed data on new even. You can divide streams by symbol and timeframe names:

```
{"status":"CONNECTED","symbol":"EURUSD","timeframe":"TICK","data":[1581611172734,1.08515,1.08521]}
{"status":"CONNECTED","symbol":"EURUSD","timeframe":"M1","data":[1581611100,1.08525,1.08525,1.08520,1.08520,10.00000]}
```

If the terminal has lost connection to the market:

```
{"status":"DISCONNECTED"}
```

When the terminal reconnects to the market, it sends the last closed candle again. So you should update your historical data. Make the `action="HISTORY"` request with `fromDate` equal to your last candle timestamp before disconnect.

`OnTradeTransaction` function is called when a trade transaction event occurs. `Streaming socket` sends `TRADE_TRANSACTION_REQUEST` data every time it happens. Try to create and modify orders in the MQL5 terminal manually and check the expert logging tab for better understanding. Also see [MQL5 docs](https://www.mql5.com/en/docs/event_handlers/ontradetransaction).

`TRADE_TRANSACTION_REQUEST` request data:

```
{
	'action': 'TRADE_ACTION_DEAL',
	'order': 501700843,
	'symbol': 'EURUSD',
	'volume': 0.1,
	'price': 1.12181,
	'stoplimit': 0.0,
	'sl': 1.1,
	'tp': 1.13,
	'deviation': 10,
	'type': 'ORDER_TYPE_BUY',
	'type_filling': 'ORDER_FILLING_FOK',
	'type_time': 'ORDER_TIME_GTC',
	'expiration': 0,
	'comment': None,
	'position': 0,
	'position_by': 0
}
```

`TRADE_TRANSACTION_REQUEST` result data:

```
{
	'retcode': 10009,
	'result': 'TRADE_RETCODE_DONE',
	'deal': 501700843,
	'order': 501700843,
	'volume': 0.1,
	'price': 1.12181,
	'comment': None,
	'request_id': 8,
	'retcode_external': 0
}
```

## Streaming MT5 indicator data

Open a chart window and attach a MT5 indicator.

Parameters:

- `id` - a unique id string.
- `symbol` - chart symbol to open and atatch the indicator to.
- `chartTF` - timeframe to set the chart at.
- `name` - the name of the MT5 indicator to attach.
- `params` - the initialisation paramaters that the specified indicator expects.
- `linecount` - the number of buffers the indicator returns. In the example below MACD is used and it return the values for "macd" and "signal".

```python
print(api.indicator_construct_and_send(action='INDICATOR', actionType='ATTACH', id='4df306ea-e8e6-439b-8004-b86ba4bcc8c3', symbol='EURUSD', chartTF='M1', name='Examples/MACD', 'params'=['12', '26', '9', 'PRICE_CLOSE'], 'linecount'=2))
```

Stream the calculated result values of a previously attached indicator.

Parameters:

- `id` - id string of a previously attached indicator.
- `fromDate` - timestamp for which a result value is requested.

```python
print(api.indicator_construct_and_send(action='INDICATOR', actionType='REQUEST', id='4df306ea-e8e6-439b-8004-b86ba4bcc8c3', 'fromDate'=1591993860))
```

Example of the result:

```python
{'error': False, 'id': '4df306ea-e8e6-439b-8004-b86ba4bcc8c3', 'data': ['0.00008204', '0.00001132']}
```

The data field holds a list with results of the calculated indicator buffers.

## Plot values to MT5 charts

Open a new chart window to plot values to.

Parameters:

- `chartId` - a unique id string to reference the new chart window.
- `fromDate` - timestamp for which a result value is requested.
- `symbol` - chart symbol to open and atatch the indicator to.
- `chartTF` - timeframe to set the chart at.

```python
print(api.construct_and_send(action='CHART', actionType='OPEN', symbol='EURUSD', chartTF='M1', chartId='cbb82988-3193-4dda-9cea-c27faaf7835b'))
```

A common scenario would be to stream vlaues calculated by the client indictor to be plotted in MT5. This is done by attaching the supplied MT5 indicator `JsonAPIIndicator` and passing values to be plotted to it.

Initialize a plot line object by attaching a new instance of `JsonAPIIndicator`, ready to recieve values to be plotted.

Parameters:

- `chartId` - id string of a previously opened chart.
- `indicatorChartId`: a unique id string to reference the new plot line object.
- `chartIndicatorSubWindow`: chart sub window to plot to (https://www.mql5.c.om/en/docs/chart_operations/chartindicatoradd)
- `style`: style settings for the plot. `shortname` and `linelabel` can be any string value. `linewidth` expects an int. All other paramters require constants supported by MQL5.
  Supported are the following style paramers (with the corresponding MQL5 constants in braces): `color` (PLOT_LINE_COLOR), `linetype` (PLOT_DRAW_TYPE), `linestyle` (PLOT_LINE_STYLE).

```python
print(api.construct_and_send(action='CHART', actionType='ADDINDICATOR', chartId='cbb82988-3193-4dda-9cea-c27faaf7835b', indicatorChartId='5f2c1ab5-6b36-498f-96ac-3982a4a3551a', chartIndicatorSubWindow=1, style={shortname='BT-BollingerBands', linelabel='Middle', color='clrYellow', linetype='DRAW_LINE', linestyle='STYLE_SOLID', linewidth=1))
```

Stream values to a plot line object (draw a line).

Parameters:

- `chartId` - id string of a previously opened chart.
- `indicatorChartId`: id string of a previously initialized plot line object.
- `data`: list of values to plot. The last value in a list (`values[-1]`) corresponds to the most recent candle. If the size of the list of values passsed is >= 1, and the number of historic candles to plot is `n` then `values[n-1]` is the most recent candle and `values[0]` is the oldest candle.

```python
# Plot line with historic data
values=[1.1225948211353751, 1.1226243406054506, 1.1226266123404378]
print(api.chart_data_construct_and_send(action='PLOT', chartId='cbb82988-3193-4dda-9cea-c27faaf7835b', indicatorChartId='5f2c1ab5-6b36-498f-96ac-3982a4a3551a', chartIndicatorSubWindow=1, data=values))

n=len(values)
print(f'The value for the oldest candle: {values[0]} - The value for the most recent candle: {values[n-1]}')

# Extend the plotted line with the most recent values as new candles are created
print(api.chart_data_construct_and_send(action='PLOT', chartId='cbb82988-3193-4dda-9cea-c27faaf7835b', indicatorChartId='5f2c1ab5-6b36-498f-96ac-3982a4a3551a', chartIndicatorSubWindow=1, data=[1.122618120966847]))
print(api.chart_data_construct_and_send(action='PLOT', chartId='cbb82988-3193-4dda-9cea-c27faaf7835b', indicatorChartId='5f2c1ab5-6b36-498f-96ac-3982a4a3551a', chartIndicatorSubWindow=1, data=[1.1226254106923093]))
```

## The JsonAPIIndicator

The supplied indicator `JsonAPIIndicator` does not do any calculations by itself. It simply plots
incoming data to a chart which can be passed by via JSON interface to the `Chart Data Socket`. The indicator is controlled by the expert script `JsonAPI.mq5` locally via port `15562`.

## Error handling

First of all, when you send a command via `System socket`, you should always receive back `"OK"` message via `System socket`. It means that your command was received and deserialized.

All data that come through `Data socket` have an `error` param. This param will have `true` key if somethng goes wrong. Also, there will be `description` and `function` params. They will hold information about error and the name of the function with error.

This information also applies to the trade commannds. See [MQL5 docs](https://www.mql5.com/en/docs/constants/errorswarnings/enum_trade_return_codes) for possible server answers.

## License

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See `LICENSE` for more information.
