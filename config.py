import os
from typing import Dict, List, Optional
from pydantic import BaseModel, Field

class DatabaseConfig(BaseModel):
    host: str = Field(default="localhost", description="Host do banco de dados")
    port: int = Field(default=3306, description="Porta do banco de dados")
    user: str = Field(default="crypto_collector", description="Usuário do banco")
    password: str = Field(default="DwbkKxzzrLEI4gUA", description="Senha do banco")
    database: str = Field(default="crypto_collector", description="Nome do banco")
    
    def get_connection_string(self) -> str:
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

class APIConfig(BaseModel):
    name: str = Field(description="Nome da API")
    base_url: str = Field(description="URL base da API")
    rate_limit: float = Field(description="Limite de requisições por segundo")
    timeout: int = Field(default=30, description="Timeout em segundos")
    max_retries: int = Field(default=3, description="Máximo de tentativas")
    
class SystemConfig(BaseModel):
    # Configurações do banco
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    
    # Configurações das APIs
    apis: Dict[str, APIConfig] = Field(default_factory=dict)
    
    # API selecionada
    selected_api: str = Field(default="bybit", description="API selecionada")
    
    # Tokens para coleta
    tokens: List[str] = Field(default_factory=list)
    
    # Configurações de timing
    collection_interval: int = Field(default=60, description="Intervalo entre coletas em segundos")
    internet_check_interval: int = Field(default=15, description="Intervalo para verificar internet em segundos")
    
    # Configurações de retry
    max_connection_retries: int = Field(default=5, description="Máximo de tentativas de conexão")
    retry_delay: int = Field(default=15, description="Delay entre tentativas em segundos")
    
    # Configurações de logging
    log_level: str = Field(default="INFO", description="Nível de log")
    log_file: str = Field(default="crypto_collector.log", description="Arquivo de log")

# Configuração padrão do sistema
DEFAULT_CONFIG = SystemConfig(
    database=DatabaseConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "crypto_collector"),
        password=os.getenv("DB_PASSWORD", "DwbkKxzzrLEI4gUA"),
        database=os.getenv("DB_NAME", "crypto_collector")
    ),
    
    apis={
        "bybit": APIConfig(
            name="Bybit",
            base_url="https://api.bybit.com",
            rate_limit=1,  # 10 requisições por segundo
            timeout=30,
            max_retries=3
        ),
        "binance": APIConfig(
            name="Binance",
            base_url="https://api.binance.com",
            rate_limit=1,  # 20 requisições por segundo
            timeout=30,
            max_retries=3
        )
    },
    
    selected_api="bybit",
    
    tokens=[
        "BTCUSDT",
        "ETHUSDT",
        "ADAUSDT"
    ],
    
    collection_interval=60,
    internet_check_interval=15,
    max_connection_retries=5,
    retry_delay=15,
    log_level="INFO",
    log_file="crypto_collector.log"
)

class ConfigManager:
    """Gerenciador de configurações do sistema"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config.json"
        self.config = self._load_config()
    
    def _load_config(self) -> SystemConfig:
        """Carrega configurações do arquivo ou usa configurações padrão"""
        try:
            if os.path.exists(self.config_path):
                import json
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                return SystemConfig(**config_data)
            else:
                self.save_config(DEFAULT_CONFIG)
                return DEFAULT_CONFIG
        except Exception as e:
            print(f"Erro ao carregar configurações: {e}")
            return DEFAULT_CONFIG
    
    def save_config(self, config: SystemConfig) -> None:
        """Salva configurações no arquivo"""
        try:
            import json
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config.model_dump(), f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Erro ao salvar configurações: {e}")
    
    def get_config(self) -> SystemConfig:
        """Retorna configurações atuais"""
        return self.config
    
    def update_config(self, **kwargs) -> None:
        """Atualiza configurações"""
        config_dict = self.config.model_dump()
        config_dict.update(kwargs)
        self.config = SystemConfig(**config_dict)
        self.save_config(self.config)
    
    def get_selected_api_config(self) -> APIConfig:
        """Retorna configuração da API selecionada"""
        return self.config.apis.get(self.config.selected_api)
    
    def get_database_config(self) -> DatabaseConfig:
        """Retorna configuração do banco de dados"""
        return self.config.database