import time
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import ConfigManager, SystemConfig
from models import DatabaseManager, CandleModel, StatusModel
from api_client import APIClientFactory
from network_utils import NetworkChecker

class DataCollector:
    """Classe principal para coleta de dados de criptomoedas"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.get_config()
        self.logger = self._setup_logging()
        
        # Inicializar componentes
        self.network_checker = NetworkChecker(timeout=self.config.apis[self.config.selected_api].timeout)
        self.api_client = APIClientFactory.create_client(
            self.config.selected_api, 
            self.config.apis[self.config.selected_api]
        )
        
        # Inicializar banco de dados
        self.engine = create_engine(self.config.database.get_connection_string())
        self.db_manager = DatabaseManager(self.engine)
        
        self.logger.info(f"DataCollector inicializado com API: {self.config.selected_api}")
    
    def _setup_logging(self) -> logging.Logger:
        """Configura sistema de logging"""
        logger = logging.getLogger('DataCollector')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        # Evitar duplicação de handlers
        if not logger.handlers:
            # Handler para arquivo
            file_handler = logging.FileHandler(self.config.log_file, encoding='utf-8')
            file_handler.setLevel(getattr(logging, self.config.log_level))
            
            # Handler para console
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Formatador
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    def _ensure_internet_connection(self) -> bool:
        """Garante que há conectividade com a internet"""
        if not self.network_checker.check_internet_connectivity():
            self.logger.warning("Sem conectividade com a internet. Aguardando...")
            return self.network_checker.wait_for_connectivity(
                max_retries=self.config.max_connection_retries,
                retry_delay=self.config.internet_check_interval
            )
        return True
    
    def _get_collection_start_time(self, symbol: str) -> Optional[int]:
        """Determina o timestamp inicial para coleta de dados"""
        with self.db_manager.get_session() as session:
            # Verificar último timestamp no banco
            latest_timestamp = self.db_manager.get_latest_timestamp(session, symbol)
            
            if latest_timestamp:
                # Começar a partir do último timestamp + 1 minuto
                return latest_timestamp + 60000
            else:
                # Se não há dados, começar de 30 dias atrás
                start_date = datetime.now() - timedelta(days=30)
                return int(start_date.timestamp() * 1000)
    
    def _collect_symbol_data(self, symbol: str) -> Dict:
        """Coleta dados para um símbolo específico"""
        result = {
            'symbol': symbol,
            'success': False,
            'records_added': 0,
            'error': None,
            'start_time': None,
            'end_time': None
        }
        
        try:
            with self.db_manager.get_session() as session:
                # Verificar se precisa atualizar
                status = self.db_manager.get_status(session, symbol, self.config.selected_api)
                
                if status and status.status_code == 'success':
                    # Verificar se a última atualização foi há menos de 1 minuto
                    last_update = status.last_update
                    if last_update and (datetime.now() - last_update).total_seconds() < 60:
                        self.logger.info(f"{symbol}: Dados já atualizados recentemente")
                        result['success'] = True
                        return result
                
                # Determinar período de coleta
                start_time = self._get_collection_start_time(symbol)
                end_time = int(datetime.now().timestamp() * 1000)
                
                if start_time >= end_time:
                    self.logger.info(f"{symbol}: Não há novos dados para coletar")
                    result['success'] = True
                    return result
                
                result['start_time'] = start_time
                result['end_time'] = end_time
                
                # Coletar dados da API
                self.logger.info(f"{symbol}: Coletando dados de {datetime.fromtimestamp(start_time/1000)} até {datetime.fromtimestamp(end_time/1000)}")
                
                klines = self.api_client.get_klines(
                    symbol=symbol,
                    interval='1',
                    start_time=start_time,
                    end_time=end_time,
                    limit=1000
                )
                
                if not klines:
                    self.logger.warning(f"{symbol}: Nenhum dado retornado pela API")
                    result['success'] = True
                    return result
                
                # Converter para modelos do banco
                candle_models = []
                for kline in klines:
                    candle = CandleModel(
                        symbol=symbol,
                        interval_time='1m',
                        open_price=kline['open'],
                        high_price=kline['high'],
                        low_price=kline['low'],
                        close_price=kline['close'],
                        volume=kline['volume'],
                        timestamp=kline['open_time']
                    )
                    candle_models.append(candle)
                
                # Inserir no banco
                inserted_count = self.db_manager.insert_candles(session, candle_models)
                
                # Atualizar status
                latest_timestamp = max(kline['open_time'] for kline in klines) if klines else start_time
                total_records = self.db_manager.get_candle_count(session, symbol)
                
                self.db_manager.update_status(
                    session=session,
                    symbol=symbol,
                    api_provider=self.config.selected_api,
                    last_timestamp=latest_timestamp,
                    total_records=total_records,
                    status_code='success'
                )
                
                result['success'] = True
                result['records_added'] = inserted_count
                
                self.logger.info(f"{symbol}: Coletados {inserted_count} novos registros. Total: {total_records}")
                
        except Exception as e:
            error_msg = f"Erro ao coletar dados para {symbol}: {str(e)}"
            self.logger.error(error_msg)
            result['error'] = error_msg
            
            # Atualizar status com erro
            try:
                with self.db_manager.get_session() as session:
                    self.db_manager.update_status(
                        session=session,
                        symbol=symbol,
                        api_provider=self.config.selected_api,
                        last_timestamp=0,
                        total_records=0,
                        status_code='error',
                        error_message=error_msg
                    )
            except Exception:
                pass
        
        return result
    
    def collect_all_symbols(self) -> List[Dict]:
        """Coleta dados para todos os símbolos configurados"""
        results = []
        
        # Verificar conectividade
        if not self._ensure_internet_connection():
            self.logger.error("Não foi possível estabelecer conectividade com a internet")
            return results
        
        # Verificar conectividade com a API
        api_config = self.config.apis[self.config.selected_api]
        if not self.network_checker.check_api_connectivity(api_config.base_url):
            self.logger.error(f"Não foi possível conectar com a API {self.config.selected_api}")
            return results
        
        self.logger.info(f"Iniciando coleta para {len(self.config.tokens)} símbolos")
        
        #print(self.config.tokens);

        for symbol in self.config.tokens:
            try:
                result = self._collect_symbol_data(symbol)
                results.append(result)
                
                # Aguardar entre requisições para respeitar rate limit
                time.sleep(api_config.rate_limit)
                
            except Exception as e:
                self.logger.error(f"Erro crítico ao processar {symbol}: {e}")
                results.append({
                    'symbol': symbol,
                    'success': False,
                    'records_added': 0,
                    'error': str(e)
                })
        
        # Resumo da coleta
        successful = sum(1 for r in results if r['success'])
        total_records = sum(r['records_added'] for r in results)
        
        self.logger.info(f"Coleta finalizada: {successful}/{len(results)} símbolos processados com sucesso")
        self.logger.info(f"Total de novos registros: {total_records}")
        
        return results
    
    def run_continuous_collection(self) -> None:
        """Executa coleta contínua de dados"""
        self.logger.info("Iniciando coleta contínua de dados")
        
        while True:
            try:
                start_time = time.time()
                
                # Executar coleta
                results = self.collect_all_symbols()
                
                # Calcular tempo de execução
                execution_time = time.time() - start_time
                
                # Calcular tempo de espera
                wait_time = max(0, self.config.collection_interval - execution_time)
                
                if wait_time > 0:
                    self.logger.info(f"Próxima coleta em {wait_time:.1f} segundos")
                    time.sleep(wait_time)
                else:
                    self.logger.warning("Coleta demorou mais que o intervalo configurado")
                
            except KeyboardInterrupt:
                self.logger.info("Coleta interrompida pelo usuário")
                break
            except Exception as e:
                self.logger.error(f"Erro crítico na coleta contínua: {e}")
                time.sleep(60)  # Aguardar 1 minuto antes de tentar novamente
    
    def get_collection_stats(self) -> Dict:
        """Retorna estatísticas da coleta"""
        stats = {
            'total_symbols': len(self.config.tokens),
            'symbols_data': {},
            'total_records': 0,
            'api_provider': self.config.selected_api
        }
        
        with self.db_manager.get_session() as session:
            for symbol in self.config.tokens:
                count = self.db_manager.get_candle_count(session, symbol)
                status = self.db_manager.get_status(session, symbol, self.config.selected_api)
                
                stats['symbols_data'][symbol] = {
                    'total_records': count,
                    'last_update': status.last_update.isoformat() if status and status.last_update else None,
                    'status': status.status_code if status else 'unknown'
                }
                
                stats['total_records'] += count
        
        return stats