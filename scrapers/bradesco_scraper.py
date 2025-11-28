#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BRADESCO VITRINE SCRAPER - GitHub Actions Ready  
Timeout: 1h (3600 segundos)
Delays: 5s entre tipos (imóveis/veículos)
"""

import json
import time
import re
import os
import signal
from playwright.sync_api import sync_playwright
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path

BASE_URL = "https://vitrinebradesco.com.br"
OUTPUT_DIR = Path("bradesco_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Configurações
MAX_EXECUTION_TIME = 3600  # 1h


class TimeoutException(Exception):
    pass


class BradescoScraper:
    def __init__(self):
        self.start_time = time.time()
        self.should_stop = False
        
        # Setup signal handler para timeout (só se não for Windows)
        if os.name != 'nt':
            signal.signal(signal.SIGALRM, self.timeout_handler)
            signal.alarm(MAX_EXECUTION_TIME)
    
    def timeout_handler(self, signum, frame):
        self.should_stop = True
        print(f"\n⚠️ TIMEOUT: 1h alcançado, salvando...")
    
    def check_timeout(self) -> bool:
        elapsed = time.time() - self.start_time
        if elapsed > MAX_EXECUTION_TIME or self.should_stop:
            return True
        return False
    
    def get_cookies(self):
        """Captura cookies do Bradesco"""
        print("🍪 Capturando cookies...")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080}
                )
                page = context.new_page()
                page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
                time.sleep(3)
                
                # Aceita cookies
                try:
                    page.locator('button:has-text("Aceitar")').first.click(timeout=5000)
                    time.sleep(2)
                except:
                    pass
                
                cookies = context.cookies()
                browser.close()
                print(f"✅ {len(cookies)} cookies")
                return {c["name"]: c["value"] for c in cookies}, cookies
        except Exception as e:
            print(f"⚠️ Erro ao capturar cookies: {e}")
            return {}, []
    
    def organizar_por_estado(self, items):
        """Organiza itens por estado"""
        por_estado = {}
        for item in items:
            estado = item.get('estado', 'DESCONHECIDO')
            if not estado or len(str(estado)) > 2:
                estado = 'DESCONHECIDO'
            estado = str(estado).upper()
            if estado not in por_estado:
                por_estado[estado] = []
            por_estado[estado].append(item)
        return dict(sorted(por_estado.items()))
    
    def extrair_cards_html(self, html, tipo_busca):
        """Extrai cards do HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            items = []
            cards = soup.select('a.auction-container')
            
            for idx, card in enumerate(cards):
                try:
                    link = card.get('href', '')
                    if link and not link.startswith('http'):
                        link = BASE_URL + link
                    
                    codigo_match = re.search(r'/auctions/([^/?]+)', link)
                    codigo = codigo_match.group(1) if codigo_match else f"{tipo_busca}_{idx}"
                    
                    valor = None
                    price_div = card.select_one('div.price p')
                    if price_div:
                        valor_match = re.search(r'R\$[\s]*[\d.]+,\d{2}', price_div.get_text())
                        if valor_match:
                            valor = valor_match.group(0)
                    
                    descricao = None
                    desc_elem = card.select_one('p.description')
                    if desc_elem:
                        descricao = desc_elem.get_text(strip=True)
                    
                    cidade = None
                    estado = None
                    loc_elem = card.select_one('div.location p')
                    if loc_elem:
                        loc_match = re.search(r'(.+?)\s*-\s*([A-Z]{2})', loc_elem.get_text(strip=True))
                        if loc_match:
                            cidade = loc_match.group(1).strip()
                            estado = loc_match.group(2).strip()
                    
                    leilao_data = None
                    if descricao:
                        data_match = re.search(r'(\d{2}/\d{2}/\d{4})', descricao)
                        if data_match:
                            leilao_data = data_match.group(1)
                    
                    if tipo_busca == 'veiculo':
                        ano = None
                        if descricao:
                            ano_match = re.search(r'(\d{4})\s*/\s*(\d{4})', descricao)
                            if ano_match:
                                ano = f"{ano_match.group(1)}/{ano_match.group(2)}"
                        
                        item = {
                            'id': codigo, 'descricao': descricao, 'tipo': 'veiculo',
                            'cidade': cidade, 'estado': estado, 'ano': ano,
                            'valor': valor, 'leilao_data': leilao_data,
                            'link': link, 'data_coleta': datetime.now().isoformat()
                        }
                    else:
                        item = {
                            'id': codigo, 'descricao': descricao, 'tipo': 'imovel',
                            'cidade': cidade, 'estado': estado,
                            'valor': valor, 'leilao_data': leilao_data,
                            'link': link, 'data_coleta': datetime.now().isoformat()
                        }
                    
                    items.append(item)
                except:
                    continue
            
            return items
        except:
            return []
    
    def buscar_items_scroll_infinito(self, page, tipo):
        """Busca com scroll infinito"""
        tipo_nome = "IMÓVEIS" if tipo == "realstate" else "VEÍCULOS"
        tipo_tag = "imovel" if tipo == "realstate" else "veiculo"
        
        print(f"\n{'='*80}")
        print(f"🔍 BUSCANDO {tipo_nome}")
        print(f"{'='*80}")
        
        url = f"{BASE_URL}/auctions?type={tipo}"
        
        try:
            page.goto(url, wait_until="networkidle", timeout=60000)
            print("⏳ Aguardando cards renderizarem...")
            page.wait_for_selector('a.auction-container', timeout=30000)
            time.sleep(5)
            
            print("📜 Iniciando scroll infinito...")
            
            todos_items = []
            ids_vistos = set()
            scroll_count = 0
            sem_novos = 0
            MAX_SEM_NOVOS = 3
            
            while not self.check_timeout():
                scroll_count += 1
                
                html = page.content()
                items_atuais = self.extrair_cards_html(html, tipo_tag)
                
                items_novos = [i for i in items_atuais if i['id'] not in ids_vistos]
                
                if items_novos:
                    print(f"   ✅ Scroll #{scroll_count}: +{len(items_novos)} | Total: {len(ids_vistos) + len(items_novos)}")
                    todos_items.extend(items_novos)
                    for item in items_novos:
                        ids_vistos.add(item['id'])
                    sem_novos = 0
                else:
                    sem_novos += 1
                    print(f"   ⚪ Scroll #{scroll_count}: Sem novos ({sem_novos}/{MAX_SEM_NOVOS})")
                    if sem_novos >= MAX_SEM_NOVOS:
                        break
                
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(3)
                
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except:
                    pass
            
            if self.check_timeout():
                print(f"\n⏰ Timeout alcançado no scroll #{scroll_count}")
            
            print(f"\n🎯 Total: {len(todos_items)} {tipo_nome.lower()}")
            return todos_items
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            return []
    
    def scrape_all(self, cookies_raw):
        """Scrape todos os tipos"""
        print("\n" + "="*80)
        print("🦢 BRADESCO VITRINE - SCRAPING COMPLETO")
        print("="*80)
        print(f"⏰ Início: {datetime.now().strftime('%H:%M:%S')}")
        print(f"⏱️  Timeout: 1h\n")
        
        todos_items = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            
            if cookies_raw:
                context.add_cookies(cookies_raw)
            
            page = context.new_page()
            
            try:
                # Imóveis
                if not self.check_timeout():
                    imoveis = self.buscar_items_scroll_infinito(page, "realstate")
                    todos_items.extend(imoveis)
                    
                    # Delay entre tipos
                    if not self.check_timeout():
                        print("\n⏳ Aguardando 5s antes dos veículos...")
                        time.sleep(5)
                
                # Veículos
                if not self.check_timeout():
                    veiculos = self.buscar_items_scroll_infinito(page, "vehicles")
                    todos_items.extend(veiculos)
                
            finally:
                browser.close()
        
        return todos_items


def upload_to_supabase(data):
    """Envia para Supabase"""
    try:
        from supabase_client import SupabaseClient, normalize
        
        client = SupabaseClient()
        
        print(f"\n{'='*80}")
        print("📤 ENVIANDO PARA SUPABASE")
        print(f"{'='*80}\n")
        
        # 1. RAW
        print("💾 Salvando dados RAW...")
        client.insert_raw('bradesco', data)
        
        # 2. Normaliza
        print("\n🧹 Normalizando dados...")
        normalized = normalize('bradesco', data)
        print(f"✅ {len(normalized)} itens normalizados")
        
        if len(normalized) > 0:
            # 3. Insere
            print(f"\n💾 Inserindo {len(normalized)} itens normalizados...")
            inserted = client.insert_normalized(normalized)
            
            if inserted > 0:
                print(f"✅ {inserted} itens processados no banco")
                return True
        
        return False
        
    except Exception as e:
        print(f"\n❌ Erro ao enviar para Supabase: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Bradesco Scraper - GitHub Actions')
    parser.add_argument('--full-update', action='store_true', 
                       help='Todas as categorias (modo produção)')
    
    args = parser.parse_args()
    
    if not args.full_update:
        print("❌ Use --full-update para modo produção")
        return
    
    print("📦 Modo produção: Imóveis + Veículos\n")
    
    scraper = BradescoScraper()
    
    # Cookies
    cookie_dict, cookies_raw = scraper.get_cookies()
    time.sleep(3)
    
    start_time = time.time()
    todos_items = scraper.scrape_all(cookies_raw)
    elapsed = time.time() - start_time
    
    if todos_items:
        # Organiza por estado
        resultado_por_estado = scraper.organizar_por_estado(todos_items)
        
        # Salva JSON local
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"bradesco_completo_{timestamp}.json"
        filepath = OUTPUT_DIR / filename
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(resultado_por_estado, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Salvo: {filepath}")
        
        imoveis_count = sum(1 for i in todos_items if i.get('tipo') == 'imovel')
        veiculos_count = sum(1 for i in todos_items if i.get('tipo') == 'veiculo')
        
        # Estatísticas
        print(f"\n{'='*80}")
        print("📈 ESTATÍSTICAS")
        print(f"{'='*80}")
        print(f"Imóveis: {imoveis_count}")
        print(f"Veículos: {veiculos_count}")
        print(f"Total: {len(todos_items)}")
        print(f"Tempo: {elapsed/60:.1f} min")
        
        # Envia pro Supabase
        upload_to_supabase(resultado_por_estado)
    
    print(f"\n⏰ Fim: {datetime.now().strftime('%H:%M:%S')}")
    print("="*80)


if __name__ == "__main__":
    main()