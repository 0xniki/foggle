from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar, List, Optional, Union, Callable, Any

from .contract import Contract
from .objects import (
    DOMLevel,
    Dividends,
    FundamentalRatios,
    MktDepthData,
    OptionComputation,
    TickByTickAllLast,
    TickByTickBidAsk,
    TickByTickMidPoint,
    TickData,
)
from .util import Event, dataclassRepr, isNan

nan = float("nan")


class Op:
    """Base operator class that replaces eventkit.Op."""
    
    def __init__(self, source=None):
        self._source = source
        self._listeners = []
        if source:
            source.connect(self.on_source)
    
    def connect(self, callback, *args):
        """Connect a callback to this operator."""
        self._listeners.append((callback, args))
        return self
    
    def emit(self, *args):
        """Emit event to all listeners."""
        for callback, cb_args in self._listeners:
            if cb_args:
                callback(*(args + cb_args))
            else:
                callback(*args)
    
    def on_source(self, *args):
        """Called when the source emits an event."""
        pass
    
    def set_done(self):
        """Mark this operator as done."""
        self._listeners = []


@dataclass
class Ticker:
    """
    Current market data such as bid, ask, last price, etc. for a contract.

    Streaming level-1 ticks of type :class:`.TickData` are stored in
    the ``ticks`` list.

    Streaming level-2 ticks of type :class:`.MktDepthData` are stored in the
    ``domTicks`` list. The order book (DOM) is available as lists of
    :class:`.DOMLevel` in ``domBids`` and ``domAsks``.

    Streaming tick-by-tick ticks are stored in ``tickByTicks``.

    For options the :class:`.OptionComputation` values for the bid, ask, resp.
    last price are stored in the ``bidGreeks``, ``askGreeks`` resp.
    ``lastGreeks`` attributes. There is also ``modelGreeks`` that conveys
    the greeks as calculated by Interactive Brokers' option model.

    Events:
        * ``updateEvent`` (ticker: :class:`.Ticker`)
    """

    events: ClassVar = ("updateEvent",)

    contract: Optional[Contract] = None
    time: Optional[datetime] = None
    marketDataType: int = 1
    minTick: float = nan
    bid: float = nan
    bidSize: float = nan
    bidExchange: str = ""
    ask: float = nan
    askSize: float = nan
    askExchange: str = ""
    last: float = nan
    lastSize: float = nan
    lastExchange: str = ""
    prevBid: float = nan
    prevBidSize: float = nan
    prevAsk: float = nan
    prevAskSize: float = nan
    prevLast: float = nan
    prevLastSize: float = nan
    volume: float = nan
    open: float = nan
    high: float = nan
    low: float = nan
    close: float = nan
    vwap: float = nan
    low13week: float = nan
    high13week: float = nan
    low26week: float = nan
    high26week: float = nan
    low52week: float = nan
    high52week: float = nan
    bidYield: float = nan
    askYield: float = nan
    lastYield: float = nan
    markPrice: float = nan
    halted: float = nan
    rtHistVolatility: float = nan
    rtVolume: float = nan
    rtTradeVolume: float = nan
    rtTime: Optional[datetime] = None
    avVolume: float = nan
    tradeCount: float = nan
    tradeRate: float = nan
    volumeRate: float = nan
    shortableShares: float = nan
    indexFuturePremium: float = nan
    futuresOpenInterest: float = nan
    putOpenInterest: float = nan
    callOpenInterest: float = nan
    putVolume: float = nan
    callVolume: float = nan
    avOptionVolume: float = nan
    histVolatility: float = nan
    impliedVolatility: float = nan
    dividends: Optional[Dividends] = None
    fundamentalRatios: Optional[FundamentalRatios] = None
    ticks: List[TickData] = field(default_factory=list)
    tickByTicks: List[
        Union[TickByTickAllLast, TickByTickBidAsk, TickByTickMidPoint]
    ] = field(default_factory=list)
    domBids: List[DOMLevel] = field(default_factory=list)
    domBidsDict: dict[int, DOMLevel] = field(default_factory=dict)
    domAsks: List[DOMLevel] = field(default_factory=list)
    domAsksDict: dict[int, DOMLevel] = field(default_factory=dict)
    domTicks: List[MktDepthData] = field(default_factory=list)
    bidGreeks: Optional[OptionComputation] = None
    askGreeks: Optional[OptionComputation] = None
    lastGreeks: Optional[OptionComputation] = None
    modelGreeks: Optional[OptionComputation] = None
    auctionVolume: float = nan
    auctionPrice: float = nan
    auctionImbalance: float = nan
    regulatoryImbalance: float = nan
    bboExchange: str = ""
    snapshotPermissions: int = 0

    def __post_init__(self):
        self.updateEvent = TickerUpdateEvent("updateEvent")

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return id(self)

    __repr__ = dataclassRepr
    __str__ = dataclassRepr

    def hasBidAsk(self) -> bool:
        """See if this ticker has a valid bid and ask."""
        return (
            self.bid != -1
            and not isNan(self.bid)
            and self.bidSize > 0
            and self.ask != -1
            and not isNan(self.ask)
            and self.askSize > 0
        )

    def midpoint(self) -> float:
        """
        Return average of bid and ask, or NaN if no valid bid and ask
        are available.
        """
        return (self.bid + self.ask) * 0.5 if self.hasBidAsk() else nan

    def marketPrice(self) -> float:
        """
        Return the first available one of

        * last price if within current bid/ask or no bid/ask available;
        * average of bid and ask (midpoint).
        """
        if self.hasBidAsk():
            if self.bid <= self.last <= self.ask:
                price = self.last
            else:
                price = self.midpoint()
        else:
            price = self.last
        return price


class TickerUpdateEvent(Event):
    """Extended Event class with special filtering methods for tickers."""
    
    def trades(self) -> "Tickfilter":
        """Emit trade ticks."""
        return Tickfilter((4, 5, 48, 68, 71), self)

    def bids(self) -> "Tickfilter":
        """Emit bid ticks."""
        return Tickfilter((0, 1, 66, 69), self)

    def asks(self) -> "Tickfilter":
        """Emit ask ticks."""
        return Tickfilter((2, 3, 67, 70), self)

    def bidasks(self) -> "Tickfilter":
        """Emit bid and ask ticks."""
        return Tickfilter((0, 1, 66, 69, 2, 3, 67, 70), self)

    def midpoints(self) -> "Tickfilter":
        """Emit midpoint ticks."""
        return Midpoints((), self)


class Tickfilter(Op):
    """Tick filtering event operators that ``emit(time, price, size)``."""

    def __init__(self, tickTypes, source=None):
        Op.__init__(self, source)
        self._tickTypes = set(tickTypes)

    def on_source(self, ticker):
        for t in ticker.ticks:
            if t.tickType in self._tickTypes:
                self.emit(t.time, t.price, t.size)

    def timebars(self, timer: Event) -> "TimeBars":
        """
        Aggregate ticks into time bars, where the timing of new bars
        is derived from a timer event.
        Emits a completed :class:`Bar`.

        This event stores a :class:`BarList` of all created bars in the
        ``bars`` property.

        Args:
            timer: Event for timing when a new bar starts.
        """
        return TimeBars(timer, self)

    def tickbars(self, count: int) -> "TickBars":
        """
        Aggregate ticks into bars that have the same number of ticks.
        Emits a completed :class:`Bar`.

        This event stores a :class:`BarList` of all created bars in the
        ``bars`` property.

        Args:
            count: Number of ticks to use to form one bar.
        """
        return TickBars(count, self)

    def volumebars(self, volume: int) -> "VolumeBars":
        """
        Aggregate ticks into bars that have the same volume.
        Emits a completed :class:`Bar`.

        This event stores a :class:`BarList` of all created bars in the
        ``bars`` property.

        Args:
            count: Number of ticks to use to form one bar.
        """
        return VolumeBars(volume, self)


class Midpoints(Tickfilter):
    def on_source(self, ticker):
        if ticker.ticks:
            self.emit(ticker.time, ticker.midpoint(), 0)


@dataclass
class Bar:
    time: Optional[datetime]
    open: float = nan
    high: float = nan
    low: float = nan
    close: float = nan
    volume: int = 0
    count: int = 0


class BarList(List[Bar]):
    def __init__(self, *args):
        super().__init__(*args)
        self.updateEvent = Event("updateEvent")

    def __eq__(self, other) -> bool:
        return self is other


class TimeBars(Op):
    __doc__ = Tickfilter.timebars.__doc__

    def __init__(self, timer, source=None):
        Op.__init__(self, source)
        self._timer = timer
        self.bars = BarList()
        
        # Connect to the timer event
        if hasattr(timer, 'connect'):
            # For our Event implementation
            timer.connect(self._on_timer)
        else:
            # Backward compatibility for any other event system
            timer += self._on_timer

    def on_source(self, time, price, size):
        if not self.bars:
            return
        bar = self.bars[-1]
        if isNan(bar.open):
            bar.open = bar.high = bar.low = price
        bar.high = max(bar.high, price)
        bar.low = min(bar.low, price)
        bar.close = price
        bar.volume += size
        bar.count += 1
        self.bars.updateEvent.emit(self.bars, False)

    def _on_timer(self, time):
        if self.bars:
            bar = self.bars[-1]
            if isNan(bar.close) and len(self.bars) > 1:
                bar.open = bar.high = bar.low = bar.close = self.bars[-2].close
            self.bars.updateEvent.emit(self.bars, True)
            self.emit(bar)
        self.bars.append(Bar(time))


class TickBars(Op):
    __doc__ = Tickfilter.tickbars.__doc__

    def __init__(self, count, source=None):
        Op.__init__(self, source)
        self._count = count
        self.bars = BarList()

    def on_source(self, time, price, size):
        if not self.bars or self.bars[-1].count == self._count:
            bar = Bar(time, price, price, price, price, size, 1)
            self.bars.append(bar)
        else:
            bar = self.bars[-1]
            bar.high = max(bar.high, price)
            bar.low = min(bar.low, price)
            bar.close = price
            bar.volume += size
            bar.count += 1
        if bar.count == self._count:
            self.bars.updateEvent.emit(self.bars, True)
            self.emit(self.bars)


class VolumeBars(Op):
    __doc__ = Tickfilter.volumebars.__doc__

    def __init__(self, volume, source=None):
        Op.__init__(self, source)
        self._volume = volume
        self.bars = BarList()

    def on_source(self, time, price, size):
        if not self.bars or self.bars[-1].volume >= self._volume:
            bar = Bar(time, price, price, price, price, size, 1)
            self.bars.append(bar)
        else:
            bar = self.bars[-1]
            bar.high = max(bar.high, price)
            bar.low = min(bar.low, price)
            bar.close = price
            bar.volume += size
            bar.count += 1
        if bar.volume >= self._volume:
            self.bars.updateEvent.emit(self.bars, True)
            self.emit(self.bars)