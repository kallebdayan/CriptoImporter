#!/usr/bin/env python3
"""
Sistema de Coleta de Dados de Criptomoedas
Desenvolvido para coletar dados de candlesticks de APIs como Bybit e Binance
"""

import sys
import argparse
import json
from datetime import datetime
from typing import Dict, List

from config import ConfigManager
from data_collector import DataCollector
from models import DatabaseManager
from sqlalchemy import create_engine

class CryptoCollectorApp:
    """Aplicação principal do sistema de coleta"""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        self.data_collector = DataCollector(self.config_manager)
    
    def run_single_collection(self) -> None:
        """Executa uma única coleta de dados"""
        print("=== Executando Coleta Única ===")
        results = self.data_collector.collect_all_symbols()
        
        # Exibir resultados
        print(f"\nResultados da Coleta:")
        print(f"{'Símbolo':<12} {'Status':<10} {'Registros':<10} {'Erro'}")
        print("-" * 60)
        
        for result in results:
            status = "✓ OK" if result['success'] else "✗ ERRO"
            error = result.get('error', '')[:30] if result.get('error') else ''
            print(f"{result['symbol']:<12} {status:<10} {result['records_added']:<10} {error}")
        
        # Resumo
        successful = sum(1 for r in results if r['success'])
        total_records = sum(r['records_added'] for r in results)
        print(f"\nResumo: {successful}/{len(results)} símbolos processados")
        print(f"Total de novos registros: {total_records}")
    
    def run_continuous_collection(self) -> None:
        """Executa coleta contínua"""
        print("=== Iniciando Coleta Contínua ===")
        print("Pressione Ctrl+C para parar")
        
        try:
            self.data_collector.run_continuous_collection()
        except KeyboardInterrupt:
            print("\nColeta interrompida pelo usuário")
        except Exception as e:
            print(f"Erro crítico: {e}")
    
    def show_stats(self) -> None:
        """Exibe estatísticas da coleta"""
        print("=== Estatísticas da Coleta ===")
        stats = self.data_collector.get_collection_stats()
        
        print(f"API Provider: {stats['api_provider']}")
        print(f"Total de Símbolos: {stats['total_symbols']}")
        print(f"Total de Registros: {stats['total_records']}")
        print(f"\nDetalhes por Símbolo:")
        print(f"{'Símbolo':<12} {'Registros':<10} {'Status':<10} {'Última Atualização'}")
        print("-" * 70)
        
        for symbol, data in stats['symbols_data'].items():
            last_update = data['last_update']
            if last_update:
                last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                last_update_str = last_update.strftime('%d/%m %H:%M')
            else:
                last_update_str = "Nunca"
            
            print(f"{symbol:<12} {data['total_records']:<10} {data['status']:<10} {last_update_str}")
    
    def configure_system(self) -> None:
        """Interface para configurar o sistema"""
        print("=== Configuração do Sistema ===")
        config = self.config_manager.get_config()
        
        while True:
            print("\nOpções de Configuração:")
            print("1. Configurar Banco de Dados")
            print("2. Configurar APIs")
            print("3. Configurar Símbolos")
            print("4. Configurar Intervalos")
            print("5. Exibir Configuração Atual")
            print("6. Salvar e Sair")
            
            choice = input("\nEscolha uma opção (1-6): ").strip()
            
            if choice == '1':
                self._configure_database()
            elif choice == '2':
                self._configure_apis()
            elif choice == '3':
                self._configure_symbols()
            elif choice == '4':
                self._configure_intervals()
            elif choice == '5':
                self._show_current_config()
            elif choice == '6':
                self.config_manager.save_config(config)
                print("Configuração salva com sucesso!")
                break
            else:
                print("Opção inválida!")
    
    def _configure_database(self) -> None:
        """Configura banco de dados"""
        print("\n--- Configuração do Banco de Dados ---")
        config = self.config_manager.get_config()
        
        host = input(f"Host [{config.database.host}]: ").strip() or config.database.host
        port = input(f"Porta [{config.database.port}]: ").strip() or str(config.database.port)
        user = input(f"Usuário [{config.database.user}]: ").strip() or config.database.user
        password = input(f"Senha [{config.database.password}]: ").strip() or config.database.password
        database = input(f"Banco [{config.database.database}]: ").strip() or config.database.database
        
        # Atualizar configuração
        config.database.host = host
        config.database.port = int(port)
        config.database.user = user
        config.database.password = password
        config.database.database = database
        
        # Testar conexão
        try:
            engine = create_engine(config.database.get_connection_string())
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            print("✓ Conexão com banco de dados testada com sucesso!")
        except Exception as e:
            print(f"✗ Erro ao conectar com banco: {e}")
    
    def _configure_apis(self) -> None:
        """Configura APIs"""
        print("\n--- Configuração das APIs ---")
        config = self.config_manager.get_config()
        
        print("APIs disponíveis:")
        for i, api_name in enumerate(config.apis.keys(), 1):
            selected = " (SELECIONADA)" if api_name == config.selected_api else ""
            print(f"{i}. {api_name.upper()}{selected}")
        
        choice = input("\nEscolha a API para usar (nome ou número): ").strip().lower()
        
        if choice.isdigit():
            api_names = list(config.apis.keys())
            if 1 <= int(choice) <= len(api_names):
                config.selected_api = api_names[int(choice) - 1]
                print(f"✓ API selecionada: {config.selected_api}")
            else:
                print("✗ Número inválido!")
        elif choice in config.apis:
            config.selected_api = choice
            print(f"✓ API selecionada: {config.selected_api}")
        else:
            print("✗ API não encontrada!")
    
    def _configure_symbols(self) -> None:
        """Configura símbolos para coleta"""
        print("\n--- Configuração dos Símbolos ---")
        config = self.config_manager.get_config()
        
        print("Símbolos atuais:")
        for i, symbol in enumerate(config.tokens, 1):
            print(f"{i}. {symbol}")
        
        print("\nOpções:")
        print("1. Adicionar símbolo")
        print("2. Remover símbolo")
        print("3. Limpar todos")
        print("4. Usar lista padrão")
        
        choice = input("\nEscolha uma opção (1-4): ").strip()
        
        if choice == '1':
            symbol = input("Digite o símbolo (ex: BTCUSDT): ").strip().upper()
            if symbol and symbol not in config.tokens:
                config.tokens.append(symbol)
                print(f"✓ Símbolo {symbol} adicionado")
            else:
                print("✗ Símbolo inválido ou já existe")
        
        elif choice == '2':
            symbol = input("Digite o símbolo para remover: ").strip().upper()
            if symbol in config.tokens:
                config.tokens.remove(symbol)
                print(f"✓ Símbolo {symbol} removido")
            else:
                print("✗ Símbolo não encontrado")
        
        elif choice == '3':
            config.tokens.clear()
            print("✓ Todos os símbolos removidos")
        
        elif choice == '4':
            config.tokens = [
                "BTCUSDT", "ETHUSDT", "ADAUSDT", "BNBUSDT", "SOLUSDT",
                "XRPUSDT", "DOGEUSDT", "TONUSDT", "TRXUSDT", "AVAXUSDT"
            ]
            print("✓ Lista padrão de símbolos carregada")
    
    def _configure_intervals(self) -> None:
        """Configura intervalos de coleta"""
        print("\n--- Configuração dos Intervalos ---")
        config = self.config_manager.get_config()
        
        print(f"Intervalo de coleta atual: {config.collection_interval} segundos")
        new_interval = input(f"Novo intervalo de coleta [{config.collection_interval}]: ").strip()
        if new_interval and new_interval.isdigit():
            config.collection_interval = int(new_interval)
            print(f"✓ Intervalo de coleta atualizado para {config.collection_interval} segundos")
        
        print(f"Intervalo de verificação de internet atual: {config.internet_check_interval} segundos")
        new_check = input(f"Novo intervalo de verificação [{config.internet_check_interval}]: ").strip()
        if new_check and new_check.isdigit():
            config.internet_check_interval = int(new_check)
            print(f"✓ Intervalo de verificação atualizado para {config.internet_check_interval} segundos")
    
    def _show_current_config(self) -> None:
        """Exibe configuração atual"""
        print("\n--- Configuração Atual ---")
        config = self.config_manager.get_config()
        
        print(f"API Selecionada: {config.selected_api}")
        print(f"Banco de Dados: {config.database.host}:{config.database.port}/{config.database.database}")
        print(f"Intervalo de Coleta: {config.collection_interval} segundos")
        print(f"Símbolos ({len(config.tokens)}): {', '.join(config.tokens)}")
        print(f"Arquivo de Log: {config.log_file}")
        print(f"Nível de Log: {config.log_level}")
    
    def test_connectivity(self) -> None:
        """Testa conectividade"""
        print("=== Teste de Conectividade ===")
        
        # Testar internet
        print("Testando conectividade com internet...")
        if self.data_collector.network_checker.check_internet_connectivity():
            print("✓ Internet OK")
        else:
            print("✗ Sem internet")
        
        # Testar API
        config = self.config_manager.get_config()
        api_config = config.apis[config.selected_api]
        
        print(f"Testando conectividade com API {config.selected_api}...")
        if self.data_collector.network_checker.check_api_connectivity(api_config.base_url):
            print("✓ API OK")
        else:
            print("✗ API indisponível")
        
        # Testar banco
        print("Testando conectividade com banco de dados...")
        try:
            with self.data_collector.db_manager.get_session() as session:
                session.execute("SELECT 1")
            print("✓ Banco OK")
        except Exception as e:
            print(f"✗ Erro no banco: {e}")

def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Sistema de Coleta de Dados de Criptomoedas')
    parser.add_argument('--single', action='store_true', help='Executa coleta única')
    parser.add_argument('--continuous', action='store_true', help='Executa coleta contínua')
    parser.add_argument('--stats', action='store_true', help='Exibe estatísticas')
    parser.add_argument('--config', action='store_true', help='Configura sistema')
    parser.add_argument('--test', action='store_true', help='Testa conectividade')
    
    args = parser.parse_args()
    
    try:
        app = CryptoCollectorApp()
        
        if args.single:
            app.run_single_collection()
        elif args.continuous:
            app.run_continuous_collection()
        elif args.stats:
            app.show_stats()
        elif args.config:
            app.configure_system()
        elif args.test:
            app.test_connectivity()
        else:
            # Menu interativo
            while True:
                print("\n=== Sistema de Coleta de Dados de Criptomoedas ===")
                print("1. Executar Coleta Única")
                print("2. Executar Coleta Contínua")
                print("3. Exibir Estatísticas")
                print("4. Configurar Sistema")
                print("5. Testar Conectividade")
                print("6. Sair")
                
                choice = input("\nEscolha uma opção (1-6): ").strip()
                
                if choice == '1':
                    app.run_single_collection()
                elif choice == '2':
                    app.run_continuous_collection()
                elif choice == '3':
                    app.show_stats()
                elif choice == '4':
                    app.configure_system()
                elif choice == '5':
                    app.test_connectivity()
                elif choice == '6':
                    print("Saindo...")
                    break
                else:
                    print("Opção inválida!")
                
                input("\nPressione Enter para continuar...")
    
    except Exception as e:
        print(f"Erro crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()