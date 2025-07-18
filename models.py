from datetime import datetime
from typing import Optional, List
from sqlalchemy import Column, Integer, String, DECIMAL, BigInteger, Text, TIMESTAMP, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.orm import Session

Base = declarative_base()

class CandleModel(Base):
    """Modelo para armazenar dados de candlesticks"""
    
    __tablename__ = 'candles'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(50), nullable=False, index=True)
    interval_time = Column(String(10), nullable=False, default='1m')
    open_price = Column(DECIMAL(20, 8), nullable=False)
    high_price = Column(DECIMAL(20, 8), nullable=False)
    low_price = Column(DECIMAL(20, 8), nullable=False)
    close_price = Column(DECIMAL(20, 8), nullable=False)
    volume = Column(DECIMAL(20, 8), nullable=False)
    timestamp = Column(BigInteger, nullable=False, index=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('symbol', 'interval_time', 'timestamp', name='unique_candle'),
        Index('idx_symbol_timestamp', 'symbol', 'timestamp'),
    )
    
    def __repr__(self):
        return f"<Candle(symbol='{self.symbol}', timestamp={self.timestamp}, close={self.close_price})>"
    
    def to_dict(self):
        """Converte o modelo para dicionário"""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'interval_time': self.interval_time,
            'open_price': float(self.open_price),
            'high_price': float(self.high_price),
            'low_price': float(self.low_price),
            'close_price': float(self.close_price),
            'volume': float(self.volume),
            'timestamp': self.timestamp,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class StatusModel(Base):
    """Modelo para controlar status das atualizações"""
    
    __tablename__ = 'status'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(50), nullable=False, index=True)
    api_provider = Column(String(50), nullable=False, default='bybit', index=True)
    last_update = Column(TIMESTAMP, server_default=func.now())
    last_timestamp = Column(BigInteger, default=0)
    total_records = Column(Integer, default=0)
    status_code = Column(String(20), default='pending')
    error_message = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('symbol', 'api_provider', name='unique_status'),
        Index('idx_last_update', 'last_update'),
    )
    
    def __repr__(self):
        return f"<Status(symbol='{self.symbol}', api='{self.api_provider}', status='{self.status_code}')>"
    
    def to_dict(self):
        """Converte o modelo para dicionário"""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'api_provider': self.api_provider,
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'last_timestamp': self.last_timestamp,
            'total_records': self.total_records,
            'status_code': self.status_code,
            'error_message': self.error_message,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class DatabaseManager:
    """Gerenciador de operações do banco de dados"""
    
    def __init__(self, engine):
        self.engine = engine
        Base.metadata.create_all(engine)
    
    def get_session(self) -> Session:
        """Retorna nova sessão do banco"""
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=self.engine)
        return Session()
    
    def get_candles(self, session: Session, symbol: str, 
                   start_time: Optional[int] = None, 
                   end_time: Optional[int] = None, 
                   limit: Optional[int] = None) -> List[CandleModel]:
        """Busca candles por critérios"""
        query = session.query(CandleModel).filter(CandleModel.symbol == symbol)
        
        if start_time:
            query = query.filter(CandleModel.timestamp >= start_time)
        
        if end_time:
            query = query.filter(CandleModel.timestamp <= end_time)
        
        query = query.order_by(CandleModel.timestamp.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def insert_candles(self, session: Session, candles: List[CandleModel]) -> int:
        """Insere candles no banco, ignorando duplicatas"""
        inserted_count = 0
        
        for candle in candles:
            try:
                session.add(candle)
                session.commit()
                inserted_count += 1
            except Exception as e:
                session.rollback()
                # Ignora duplicatas
                if "Duplicate entry" in str(e):
                    continue
                else:
                    raise e
        
        return inserted_count
    
    def get_status(self, session: Session, symbol: str, api_provider: str) -> Optional[StatusModel]:
        """Busca status de um símbolo"""
        return session.query(StatusModel).filter(
            StatusModel.symbol == symbol,
            StatusModel.api_provider == api_provider
        ).first()
    
    def update_status(self, session: Session, symbol: str, api_provider: str, 
                     last_timestamp: int, total_records: int, 
                     status_code: str = 'success', error_message: str = None) -> None:
        """Atualiza status de um símbolo"""
        status = self.get_status(session, symbol, api_provider)
        
        if status:
            status.last_update = func.now()
            status.last_timestamp = last_timestamp
            status.total_records = total_records
            status.status_code = status_code
            status.error_message = error_message
        else:
            status = StatusModel(
                symbol=symbol,
                api_provider=api_provider,
                last_timestamp=last_timestamp,
                total_records=total_records,
                status_code=status_code,
                error_message=error_message
            )
            session.add(status)
        
        session.commit()
    
    def get_latest_timestamp(self, session: Session, symbol: str) -> Optional[int]:
        """Busca o timestamp mais recente para um símbolo"""
        latest_candle = session.query(CandleModel).filter(
            CandleModel.symbol == symbol
        ).order_by(CandleModel.timestamp.desc()).first()
        
        return latest_candle.timestamp if latest_candle else None
    
    def get_all_symbols(self, session: Session) -> List[str]:
        """Retorna todos os símbolos únicos no banco"""
        symbols = session.query(CandleModel.symbol).distinct().all()
        return [symbol[0] for symbol in symbols]
    
    def get_candle_count(self, session: Session, symbol: str) -> int:
        """Conta total de candles para um símbolo"""
        return session.query(CandleModel).filter(CandleModel.symbol == symbol).count()
    
    def cleanup_old_data(self, session: Session, symbol: str, keep_days: int = 30) -> int:
        """Remove dados antigos mantendo apenas os últimos N dias"""
        cutoff_timestamp = int((datetime.now().timestamp() - (keep_days * 24 * 60 * 60)) * 1000)
        
        deleted_count = session.query(CandleModel).filter(
            CandleModel.symbol == symbol,
            CandleModel.timestamp < cutoff_timestamp
        ).delete()
        
        session.commit()
        return deleted_count