"""
Database Models - Data classes for representing database entities
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from decimal import Decimal


@dataclass
class SectorInfo:
    """Data model for sector information"""
    sector_code: str
    sector_name: str
    is_active: bool = True
    symbols: List[str] = field(default_factory=list)
    last_index_value: float = 100.0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate sector data after initialization"""
        if not self.sector_code:
            raise ValueError("Sector code cannot be empty")
        if not self.sector_name:
            raise ValueError("Sector name cannot be empty")
        if self.last_index_value < 0:
            raise ValueError("Index value cannot be negative")
    
    def add_symbol(self, symbol: str) -> None:
        """Add a symbol to the sector"""
        if symbol and symbol not in self.symbols:
            self.symbols.append(symbol)
    
    def remove_symbol(self, symbol: str) -> None:
        """Remove a symbol from the sector"""
        if symbol in self.symbols:
            self.symbols.remove(symbol)
    
    def get_symbol_count(self) -> int:
        """Get the number of symbols in the sector"""
        return len(self.symbols)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations"""
        return {
            'sector_code': self.sector_code,
            'sector_name': self.sector_name,
            'is_active': self.is_active,
            'symbols': self.symbols,
            'last_index_value': self.last_index_value,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SectorInfo':
        """Create instance from dictionary"""
        return cls(
            sector_code=data.get('sector_code', ''),
            sector_name=data.get('sector_name', ''),
            is_active=data.get('is_active', True),
            symbols=data.get('symbols', []),
            last_index_value=data.get('last_index_value', 100.0),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )


@dataclass
class MarketCapData:
    """Data model for market capitalization data"""
    company: str
    timestamp: datetime
    ltp: float
    total_shares: int
    market_cap: float
    sponsor_shares: float = 0.0
    govt_shares: float = 0.0
    institute_shares: float = 0.0
    foreign_shares: float = 0.0
    public_shares: float = 0.0
    free_float_pct: float = 0.0
    free_float_mcap: float = 0.0
    price_return: Optional[float] = None
    weight: Optional[float] = None
    weighted_return: Optional[float] = None
    
    def __post_init__(self):
        """Calculate derived values and validate data"""
        self._validate_data()
        self._calculate_derived_values()
    
    def _validate_data(self) -> None:
        """Validate market cap data"""
        if not self.company:
            raise ValueError("Company name cannot be empty")
        if self.ltp <= 0:
            raise ValueError(f"Invalid LTP for {self.company}: {self.ltp}")
        if self.total_shares <= 0:
            raise ValueError(f"Invalid total shares for {self.company}: {self.total_shares}")
        if self.market_cap <= 0:
            raise ValueError(f"Invalid market cap for {self.company}: {self.market_cap}")
    
    def _calculate_derived_values(self) -> None:
        """Calculate free float percentage and market cap"""
        if self.free_float_pct == 0.0:
            self.free_float_pct = max(0, 100 - (self.sponsor_shares + self.govt_shares))
        
        if self.free_float_mcap == 0.0:
            self.free_float_mcap = self.market_cap * (self.free_float_pct / 100)
    
    def calculate_return(self, previous_data: 'MarketCapData') -> float:
        """Calculate price return compared to previous data"""
        if previous_data.free_float_mcap <= 0:
            raise ValueError(f"Invalid previous free float market cap for {self.company}")
        
        self.price_return = (self.free_float_mcap / previous_data.free_float_mcap) - 1
        return self.price_return
    
    def calculate_weight(self, total_sector_mcap: float) -> float:
        """Calculate weight in sector based on total sector market cap"""
        if total_sector_mcap <= 0:
            raise ValueError("Total sector market cap must be positive")
        
        self.weight = self.free_float_mcap / total_sector_mcap
        return self.weight
    
    def calculate_weighted_return(self) -> float:
        """Calculate weighted return (requires price_return and weight)"""
        if self.price_return is None or self.weight is None:
            raise ValueError("Price return and weight must be calculated first")
        
        self.weighted_return = self.price_return * self.weight
        return self.weighted_return
    
    def is_valid_for_calculation(self) -> bool:
        """Check if data is valid for index calculations"""
        return (
            self.ltp > 0 and 
            self.total_shares > 0 and 
            self.market_cap > 0 and 
            self.free_float_mcap > 0
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations"""
        return {
            'company': self.company,
            'timestamp': self.timestamp,
            'ltp': self.ltp,
            'total_shares': self.total_shares,
            'market_cap': self.market_cap,
            'sponsor_shares': self.sponsor_shares,
            'govt_shares': self.govt_shares,
            'institute_shares': self.institute_shares,
            'foreign_shares': self.foreign_shares,
            'public_shares': self.public_shares,
            'free_float_pct': self.free_float_pct,
            'free_float_mcap': self.free_float_mcap,
            'price_return': self.price_return,
            'weight': self.weight,
            'weighted_return': self.weighted_return
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MarketCapData':
        """Create instance from dictionary"""
        return cls(
            company=data['company'],
            timestamp=data['timestamp'],
            ltp=float(data['ltp']),
            total_shares=int(data['total_shares']),
            market_cap=float(data['market_cap']),
            sponsor_shares=float(data.get('sponsor_shares', 0)),
            govt_shares=float(data.get('govt_shares', 0)),
            institute_shares=float(data.get('institute_shares', 0)),
            foreign_shares=float(data.get('foreign_shares', 0)),
            public_shares=float(data.get('public_shares', 0)),
            free_float_pct=float(data.get('free_float_pct', 0)),
            free_float_mcap=float(data.get('free_float_mcap', 0)),
            price_return=data.get('price_return'),
            weight=data.get('weight'),
            weighted_return=data.get('weighted_return')
        )


@dataclass
class IndexResult:
    """Data model for sector index calculation results"""
    sector_code: str
    sector_name: str
    timestamp: datetime
    previous_index: float
    current_index: float
    total_return: float
    num_companies: int
    calculation_method: str = "free_float_weighted"
    base_index: float = 100.0
    period_return_pct: Optional[float] = None
    volatility: Optional[float] = None
    max_weight: Optional[float] = None
    min_weight: Optional[float] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Calculate derived metrics after initialization"""
        self._validate_data()
        self._calculate_derived_metrics()
        if self.created_at is None:
            self.created_at = datetime.now()
    
    def _validate_data(self) -> None:
        """Validate index result data"""
        if not self.sector_code:
            raise ValueError("Sector code cannot be empty")
        if not self.sector_name:
            raise ValueError("Sector name cannot be empty")
        if self.previous_index <= 0:
            raise ValueError(f"Invalid previous index for {self.sector_code}: {self.previous_index}")
        if self.current_index <= 0:
            raise ValueError(f"Invalid current index for {self.sector_code}: {self.current_index}")
        if self.num_companies <= 0:
            raise ValueError(f"Invalid number of companies for {self.sector_code}: {self.num_companies}")
    
    def _calculate_derived_metrics(self) -> None:
        """Calculate derived metrics"""
        if self.period_return_pct is None:
            self.period_return_pct = ((self.current_index / self.previous_index) - 1) * 100
    
    def get_index_change(self) -> float:
        """Get absolute index change"""
        return self.current_index - self.previous_index
    
    def get_return_percentage(self) -> float:
        """Get return as percentage"""
        return self.total_return * 100
    
    def is_positive_return(self) -> bool:
        """Check if the return is positive"""
        return self.total_return > 0
    
    def get_performance_category(self) -> str:
        """Categorize performance based on return"""
        if self.total_return > 0.02:  # > 2%
            return "Strong Positive"
        elif self.total_return > 0:
            return "Positive"
        elif self.total_return > -0.02:  # > -2%
            return "Negative"
        else:
            return "Strong Negative"
    
    def compare_to_base(self) -> Dict[str, float]:
        """Compare current performance to base index"""
        return {
            'current_vs_base': (self.current_index / self.base_index - 1) * 100,
            'previous_vs_base': (self.previous_index / self.base_index - 1) * 100
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations"""
        return {
            'sector_code': self.sector_code,
            'sector_name': self.sector_name,
            'timestamp': self.timestamp,
            'previous_index': self.previous_index,
            'current_index': self.current_index,
            'total_return': self.total_return,
            'num_companies': self.num_companies,
            'calculation_method': self.calculation_method,
            'base_index': self.base_index,
            'period_return_pct': self.period_return_pct,
            'volatility': self.volatility,
            'max_weight': self.max_weight,
            'min_weight': self.min_weight,
            'created_at': self.created_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IndexResult':
        """Create instance from dictionary"""
        return cls(
            sector_code=data['sector_code'],
            sector_name=data['sector_name'],
            timestamp=data['timestamp'],
            previous_index=float(data['previous_index']),
            current_index=float(data['current_index']),
            total_return=float(data['total_return']),
            num_companies=int(data['num_companies']),
            calculation_method=data.get('calculation_method', 'free_float_weighted'),
            base_index=float(data.get('base_index', 100.0)),
            period_return_pct=data.get('period_return_pct'),
            volatility=data.get('volatility'),
            max_weight=data.get('max_weight'),
            min_weight=data.get('min_weight'),
            created_at=data.get('created_at')
        )
    
    def __str__(self) -> str:
        """String representation of index result"""
        return (f"IndexResult(sector={self.sector_code}, "
                f"timestamp={self.timestamp.strftime('%Y-%m-%d %H:%M')}, "
                f"index={self.current_index:.2f}, "
                f"return={self.get_return_percentage():.2f}%)")
    
    def __repr__(self) -> str:
        """Detailed representation of index result"""
        return (f"IndexResult(sector_code='{self.sector_code}', "
                f"current_index={self.current_index}, "
                f"total_return={self.total_return}, "
                f"num_companies={self.num_companies})")


# Type aliases for better code readability
SectorInfoDict = Dict[str, SectorInfo]
MarketCapDataDict = Dict[str, MarketCapData]
IndexResultList = List[IndexResult]

# Utility functions for model operations
def create_sector_info_from_db_row(row) -> SectorInfo:
    """Create SectorInfo from database row"""
    return SectorInfo(
        sector_code=row.get('sector_code', ''),
        sector_name=row.get('sector_name', ''),
        is_active=bool(row.get('isActive', True)),
        last_index_value=float(row.get('last_index_value', 100.0))
    )

def create_market_cap_data_from_db_row(row) -> MarketCapData:
    """Create MarketCapData from database row"""
    return MarketCapData(
        company=row.get('company', ''),
        timestamp=row.get('timestamp', datetime.now()),
        ltp=float(row.get('LTP', 0)),
        total_shares=int(row.get('total_share', 0)),
        market_cap=float(row.get('LTP', 0)) * int(row.get('total_share', 0)),
        sponsor_shares=float(row.get('Sponsor', 0)),
        govt_shares=float(row.get('Govt', 0)),
        institute_shares=float(row.get('Institute', 0)),
        foreign_shares=float(row.get('Foreign_share', 0)),
        public_shares=float(row.get('public_share', 0))
    )

def validate_model_data(models: List[Any]) -> List[str]:
    """Validate a list of model instances and return validation errors"""
    errors = []
    for i, model in enumerate(models):
        try:
            if hasattr(model, '_validate_data'):
                model._validate_data()
        except ValueError as e:
            errors.append(f"Model {i}: {str(e)}")
    return errors