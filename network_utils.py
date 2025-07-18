import socket
import time
import requests
import logging
from typing import List, Optional
from urllib.parse import urlparse

class NetworkChecker:
    """Classe para verificar conectividade de rede"""
    
    def __init__(self, timeout: int = 5):
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Serviços para teste de conectividade
        self.test_urls = [
            'https://www.google.com',
            'https://www.cloudflare.com',
            'https://api.bybit.com',
            'https://api.binance.com'
        ]
        
        self.dns_servers = [
            '8.8.8.8',  # Google DNS
            '1.1.1.1',  # Cloudflare DNS
            '208.67.222.222'  # OpenDNS
        ]
    
    def check_dns_resolution(self, hostname: str = 'google.com') -> bool:
        """Verifica se a resolução DNS está funcionando"""
        try:
            socket.gethostbyname(hostname)
            return True
        except socket.gaierror:
            return False
    
    def check_tcp_connection(self, host: str, port: int) -> bool:
        """Verifica conectividade TCP para host e porta específicos"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def check_http_connectivity(self, url: str) -> bool:
        """Verifica conectividade HTTP para uma URL"""
        try:
            response = requests.head(url, timeout=self.timeout)
            return response.status_code < 500
        except Exception:
            return False
    
    def check_internet_connectivity(self) -> bool:
        """Verifica conectividade geral com a internet"""
        # Teste 1: Verificar DNS
        if not self.check_dns_resolution():
            self.logger.warning("Falha na resolução DNS")
            return False
        
        # Teste 2: Verificar conectividade TCP com servidores DNS
        dns_ok = False
        for dns_server in self.dns_servers:
            if self.check_tcp_connection(dns_server, 53):
                dns_ok = True
                break
        
        if not dns_ok:
            self.logger.warning("Falha na conectividade TCP com servidores DNS")
            return False
        
        # Teste 3: Verificar conectividade HTTP
        for url in self.test_urls:
            if self.check_http_connectivity(url):
                return True
        
        self.logger.warning("Falha na conectividade HTTP com todos os serviços de teste")
        return False
    
    def check_api_connectivity(self, api_url: str) -> bool:
        """Verifica conectividade específica com uma API"""
        try:
            parsed_url = urlparse(api_url)
            host = parsed_url.hostname
            port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 80)
            
            # Verificar conectividade TCP
            if not self.check_tcp_connection(host, port):
                return False
            
            # Verificar conectividade HTTP
            return self.check_http_connectivity(api_url)
            
        except Exception as e:
            self.logger.error(f"Erro ao verificar conectividade da API {api_url}: {e}")
            return False
    
    def wait_for_connectivity(self, max_retries: int = 5, retry_delay: int = 15) -> bool:
        """Aguarda até que a conectividade seja restaurada"""
        for attempt in range(max_retries):
            if self.check_internet_connectivity():
                self.logger.info("Conectividade restaurada")
                return True
            
            if attempt < max_retries - 1:
                self.logger.warning(f"Sem conectividade, tentativa {attempt + 1}/{max_retries}. "
                                  f"Aguardando {retry_delay} segundos...")
                time.sleep(retry_delay)
        
        self.logger.error("Falha ao restaurar conectividade após todas as tentativas")
        return False
    
    def get_network_info(self) -> dict:
        """Retorna informações sobre o estado da rede"""
        info = {
            'dns_resolution': self.check_dns_resolution(),
            'internet_connectivity': self.check_internet_connectivity(),
            'timestamp': time.time()
        }
        
        # Testar conectividade com cada URL de teste
        info['test_urls'] = {}
        for url in self.test_urls:
            info['test_urls'][url] = self.check_http_connectivity(url)
        
        return info