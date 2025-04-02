from typing import Dict, Union, Literal, Optional, TypedDict
from dataclasses import dataclass
from enum import Enum


class SecurityType(str, Enum):
    """Enum defining the available security types."""
    STOCK = "STK"
    FUTURE = "FUT"
    OPTION = "OPT"
    PERPETUAL = "PERP"
    CRYPTO = "CRYPTO"
    FOREX = "FOREX"
    INDEX = "IND"
    CFD = "CFD"
    BOND = "BOND"
    WARRANT = "WAR"


class OptionRight(str, Enum):
    """Enum defining option rights (call or put)."""
    CALL = "C"
    PUT = "P"


class ContractDict(TypedDict):
    """TypedDict for raw contract dictionaries."""
    symbol: str
    secType: str
    exchange: str
    currency: str
    expiration: Optional[str]
    strike: Optional[str]
    right: Optional[str]


@dataclass
class BaseContract:
    """Base contract class with common properties."""
    symbol: str
    exchange: str
    currency: str = "USD"
    
    def to_dict(self) -> dict:
        """Convert contract to dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class StockContract(BaseContract):
    """Stock contract specification."""
    sec_type: Literal[SecurityType.STOCK] = SecurityType.STOCK
    
    def to_dict(self) -> dict:
        result = super().to_dict()
        result["secType"] = self.sec_type
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StockContract':
        return cls(
            symbol=data["symbol"],
            exchange=data["exchange"],
            currency=data.get("currency", "USD")
        )


@dataclass
class FutureContract(BaseContract):
    """Future contract specification."""
    sec_type: Literal[SecurityType.FUTURE] = SecurityType.FUTURE
    expiration: str = None  # Format: YYYYMMDD
    
    def to_dict(self) -> dict:
        result = super().to_dict()
        result["secType"] = self.sec_type
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FutureContract':
        return cls(
            symbol=data["symbol"],
            exchange=data["exchange"],
            currency=data.get("currency", "USD"),
            expiration=data.get("expiration")
        )


@dataclass
class OptionContract(BaseContract):
    """Option contract specification."""
    sec_type: Literal[SecurityType.OPTION] = SecurityType.OPTION
    expiration: str = None  # Format: YYYYMMDD
    strike: float = None
    right: OptionRight = None
    
    def to_dict(self) -> dict:
        result = super().to_dict()
        result["secType"] = self.sec_type
        if isinstance(self.right, OptionRight):
            result["right"] = self.right.value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'OptionContract':
        return cls(
            symbol=data["symbol"],
            exchange=data["exchange"],
            currency=data.get("currency", "USD"),
            expiration=data.get("expiration"),
            strike=float(data["strike"]) if "strike" in data else None,
            right=OptionRight(data["right"]) if "right" in data else None
        )


@dataclass
class PerpetualContract(BaseContract):
    """Perpetual future contract specification."""
    sec_type: Literal[SecurityType.PERPETUAL] = SecurityType.PERPETUAL
    
    def to_dict(self) -> dict:
        result = super().to_dict()
        result["secType"] = self.sec_type
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'PerpetualContract':
        return cls(
            symbol=data["symbol"],
            exchange=data["exchange"],
            currency=data.get("currency", "USD")
        )


@dataclass
class CryptoContract(BaseContract):
    """Cryptocurrency contract specification."""
    sec_type: Literal[SecurityType.CRYPTO] = SecurityType.CRYPTO
    
    def to_dict(self) -> dict:
        result = super().to_dict()
        result["secType"] = self.sec_type
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CryptoContract':
        return cls(
            symbol=data["symbol"],
            exchange=data["exchange"],
            currency=data.get("currency", "USD")
        )


# Union type for all contract types
Contract = Union[StockContract, FutureContract, OptionContract, PerpetualContract, CryptoContract]


def create_contract(contract_data: Dict) -> Contract:
    """Factory function to create the appropriate contract type from dictionary."""
    sec_type = contract_data.get("secType")
    
    if sec_type == SecurityType.STOCK:
        return StockContract.from_dict(contract_data)
    elif sec_type == SecurityType.FUTURE:
        return FutureContract.from_dict(contract_data)
    elif sec_type == SecurityType.OPTION:
        return OptionContract.from_dict(contract_data)
    elif sec_type == SecurityType.PERPETUAL:
        return PerpetualContract.from_dict(contract_data)
    elif sec_type == SecurityType.CRYPTO:
        return CryptoContract.from_dict(contract_data)
    else:
        raise ValueError(f"Unsupported security type: {sec_type}")


# Pre-defined contracts (examples)
class Contracts:
    """Container class for pre-defined contracts."""
    
    AAPL_STOCK = StockContract(
        symbol="AAPL",
        exchange="IEX"
    )
    
    NQ_FUTURE = FutureContract(
        symbol="NQ",
        exchange="CME",
        expiration="20250620"
    )
    
    NVDA_PUT = OptionContract(
        symbol="NVDA",
        exchange="AMEX",
        expiration="20250404",
        strike=112.0,
        right=OptionRight.PUT
    )
    
    BTC_PERP = PerpetualContract(
        symbol="BTC",
        exchange="HYPERLIQUID"
    )
    
    ETH_PERP = PerpetualContract(
        symbol="ETH",
        exchange="HYPERLIQUID"
    )
    
    ETH_SPOT = CryptoContract(
        symbol="ETH",
        exchange="PAXOS"
    )
