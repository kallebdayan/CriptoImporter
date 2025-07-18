import requests
import time
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from abc import ABC, abstractmethod
from config import APIConfig

class APIClient(ABC):
    """Classe base para clientes de API"""
    
    def __init__(self, config: APIConfig):
        self.config = config
        self.session = requests.Session()
        self.session.timeout = config.timeout
        self.last_request_time = 0
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _rate_limit(self):
        """Controla limite de requisições"""
        elapsed = time.time() - self.last_request_time
        wait_time = self.config.rate_limit - elapsed
        
        if wait_time > 0:
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Faz requisição HTTP com retry"""
        self._rate_limit()
        url = f"{self.config.base_url}{endpoint}"
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Tentativa {attempt + 1} falhou: {e}")
                if attempt == self.config.max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Backoff exponencial
    
    @abstractmethod
    def get_klines(self, symbol: str, interval: str = '1m', 
                   start_time: Optional[int] = None, 
                   end_time: Optional[int] = None, 
                   limit: int = 1000) -> List[Dict]:
        """Método abstrato para buscar dados de candlesticks"""
        pass
    
    @abstractmethod
    def get_symbols(self) -> List[str]:
        """Método abstrato para buscar símbolos disponíveis"""
        pass

class BybitClient(APIClient):
    """Cliente para API da Bybit"""
    
    def get_klines(self, symbol: str, interval: str = '1', 
                   start_time: Optional[int] = None, 
                   end_time: Optional[int] = None, 
                   limit: int = 1000) -> List[Dict]:
        """Busca dados de candlesticks da Bybit"""
        
        params = {
            'category': 'spot',
            'symbol': symbol,
            'interval': interval,
            'limit': min(limit, 1000)  # Bybit limita a 1000
        }
        
        if start_time:
            params['start'] = start_time
        if end_time:
            params['end'] = end_time
        
        try:
            response = self._make_request('/v5/market/kline', params)
            
            if response.get('retCode') != 0:
                raise Exception(f"Erro da API Bybit: {response.get('retMsg')}")
            
            klines = response.get('result', {}).get('list', [])
            
            # Converter formato Bybit para formato padrão
            converted_klines = []
            for kline in klines:
                converted_klines.append({
                    'open_time': int(kline[0]),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5]),
                    'close_time': int(kline[0]) + 60000,  # +1 minuto
                    'symbol': symbol
                })
            
            return converted_klines
            
        except Exception as e:
            self.logger.error(f"Erro ao buscar klines da Bybit para {symbol}: {e}")
            raise
    
    def get_symbols(self) -> List[str]:
        """Busca símbolos disponíveis na Bybit"""
        try:
            response = self._make_request('/v5/market/instruments-info', 
                                        {'category': 'spot'})
            
            if response.get('retCode') != 0:
                raise Exception(f"Erro da API Bybit: {response.get('retMsg')}")
            
            instruments = response.get('result', {}).get('list', [])
            symbols = [instrument['symbol'] for instrument in instruments 
                      if instrument.get('status') == 'Trading']
            
            return symbols
            
        except Exception as e:
            self.logger.error(f"Erro ao buscar símbolos da Bybit: {e}")
            raise

class BinanceClient(APIClient):
    """Cliente para API da Binance"""
    
    def get_klines(self, symbol: str, interval: str = '1m', 
                   start_time: Optional[int] = None, 
                   end_time: Optional[int] = None, 
                   limit: int = 1000) -> List[Dict]:
        """Busca dados de candlesticks da Binance"""
        
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': min(limit, 1000)  # Binance limita a 1000
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        try:
            response = self._make_request('/api/v3/klines', params)
            
            # Converter formato Binance para formato padrão
            converted_klines = []
            for kline in response:
                converted_klines.append({
                    'open_time': int(kline[0]),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5]),
                    'close_time': int(kline[6]),
                    'symbol': symbol
                })
            
            return converted_klines
            
        except Exception as e:
            self.logger.error(f"Erro ao buscar klines da Binance para {symbol}: {e}")
            raise
    
    def get_symbols(self) -> List[str]:
        """Busca símbolos disponíveis na Binance"""
        try:
            response = self._make_request('/api/v3/exchangeInfo')
            
            symbols = [symbol_info['symbol'] for symbol_info in response['symbols'] 
                      if symbol_info.get('status') == 'TRADING']
            
            return symbols
            
        except Exception as e:
            self.logger.error(f"Erro ao buscar símbolos da Binance: {e}")
            raise

class APIClientFactory:
    """Factory para criar clientes de API"""
    
    @staticmethod
    def create_client(api_name: str, config: APIConfig) -> APIClient:
        """Cria cliente de API baseado no nome"""
        clients = {
            'bybit': BybitClient,
            'binance': BinanceClient
        }
        
        client_class = clients.get(api_name.lower())
        if not client_class:
            raise ValueError(f"Cliente de API não suportado: {api_name}")
        
        return client_class(config)
    
    @staticmethod
    def get_supported_apis() -> List[str]:
        """Retorna lista de APIs suportadas"""
        return ['bybit', 'binance']