#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CAIXA ECONÔMICA FEDERAL SCRAPER - GitHub Actions Ready
Timeout: 2h (7200 segundos)
Delays: 1-2s entre cidades
"""

import json
import time
import re
import requests
import os
import signal
import random
from playwright.sync_api import sync_playwright
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path

API_BASE = "https://venda-imoveis.caixa.gov.br/sistema"
OUTPUT_DIR = Path("caixa_data")
OUTPUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    "accept": "*/*",
    "accept-language": "pt-BR,pt;q=0.9",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://venda-imoveis.caixa.gov.br",
    "x-requested-with": "XMLHttpRequest",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Configurações
MAX_EXECUTION_TIME = 7200  # 2h
MAX_RETRIES = 3


class TimeoutException(Exception):
    pass


class CaixaScraper:
    def __init__(self):
        self.session = requests.Session()
        self.start_time = time.time()
        self.should_stop = False
        self.cookies = {}
        
        # Setup signal handler para timeout (só se não for Windows)
        if os.name != 'nt':
            signal.signal(signal.SIGALRM, self.timeout_handler)
            signal.alarm(MAX_EXECUTION_TIME)
    
    def timeout_handler(self, signum, frame):
        self.should_stop = True
        print(f"\⚠️ TIMEOUT: 2h alcançado, salvando...")
    
    def check_timeout(self) -> bool:
        elapsed = time.time() - self.start_time
        if elapsed > MAX_EXECUTION_TIME or self.should_stop:
            return True
        return False
    
    def carregar_cidades_txt(self, arquivo='cidades.txt'):
        """Carrega cidades do arquivo"""
        print("📖 Carregando cidades...")
        
        cidades_por_estado = {}
        estado_atual = None
        
        try:
            with open(arquivo, 'r', encoding='utf-8') as f:
                for linha in f:
                    linha = linha.strip()
                    
                    if not linha or linha.startswith('#'):
                        continue
                    
                    # Estado (2 letras)
                    if len(linha) == 2 and linha.isupper():
                        estado_atual = linha
                        cidades_por_estado[estado_atual] = {}
                        continue
                    
                    # Cidade (codigo - nome)
                    if estado_atual and ' - ' in linha:
                        try:
                            codigo, nome = linha.split(' - ', 1)
                            cidades_por_estado[estado_atual][nome.strip()] = codigo.strip()
                        except:
                            continue
            
            total = sum(len(c) for c in cidades_por_estado.values())
            print(f"✅ {len(cidades_por_estado)} estados, {total} cidades")
            return dict(sorted(cidades_por_estado.items()))
            
        except FileNotFoundError:
            print(f"❌ Arquivo {arquivo} não encontrado!")
            return {}
        except Exception as e:
            print(f"❌ Erro: {e}")
            return {}
    
    def get_cookies(self):
        """Captura cookies"""
        print("🍪 Capturando cookies...")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                page.goto("https://venda-imoveis.caixa.gov.br/sistema/busca-imovel.asp?sltTipoBusca=imoveis",
                         wait_until="networkidle", timeout=60000)
                time.sleep(3)
                
                cookies = context.cookies()
                browser.close()
                
                cookie_dict = {c["name"]: c["value"] for c in cookies}
                print(f"✅ {len(cookie_dict)} cookies")
                return cookie_dict
                
        except Exception as e:
            print(f"❌ Erro: {e}")
            return {}
    
    def pesquisar_imoveis(self, estado, cidade_codigo, tipo_venda='34', tipo_imovel='4'):
        """Busca IDs de imóveis"""
        url = f"{API_BASE}/carregaPesquisaImoveis.asp"
        
        payload = {
            'hdn_estado': estado,
            'hdn_cidade': cidade_codigo,
            'hdn_bairro': '',
            'hdn_tp_venda': tipo_venda,
            'hdn_tp_imovel': tipo_imovel,
            'hdn_area_util': '0',
            'hdn_faixa_vlr': '0',
            'hdn_quartos': '0',
            'hdn_vg_garagem': '0',
            'strValorSimulador': '',
            'strAceitaFGTS': '',
            'strAceitaFinanciamento': ''
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                headers = HEADERS.copy()
                headers['referer'] = f"https://venda-imoveis.caixa.gov.br/sistema/busca-imovel.asp?estado={estado}"
                
                time.sleep(random.uniform(1, 2))
                
                r = self.session.post(url, headers=headers, data=payload, cookies=self.cookies, timeout=30)
                
                if r.status_code == 200:
                    return r.text, None
                    
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(3)
                else:
                    return None, str(e)
        
        return None, "Falha após tentativas"
    
    def extrair_ids_imoveis(self, html):
        """Extrai códigos de 14 dígitos"""
        if "nenhum imóvel encontrado" in html.lower():
            return []
        
        pattern = r'\b(\d{14})\b'
        matches = re.findall(pattern, html)
        
        seen = set()
        ids = []
        for id_imovel in matches:
            if id_imovel not in seen:
                ids.append(id_imovel)
                seen.add(id_imovel)
        
        return ids
    
    def carregar_detalhes_imoveis(self, ids):
        """Carrega detalhes dos imóveis"""
        url = f"{API_BASE}/carregaListaImoveis.asp"
        
        hdn_imov = '||'.join(ids)
        data = {'hdnImov': hdn_imov}
        
        try:
            time.sleep(random.uniform(1, 2))
            r = self.session.post(url, headers=HEADERS, data=data, cookies=self.cookies, timeout=30)
            
            if r.status_code == 200:
                return r.text, None
            return None, f"Status {r.status_code}"
            
        except Exception as e:
            return None, str(e)
    
    def parse_imoveis(self, html, ids):
        """Parse HTML dos detalhes"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            imoveis = []
            
            selectors = ['div.imovel-card', 'li.imovel-item', 'div.imovel', 'article.imovel', 'li']
            
            cards = None
            for selector in selectors:
                cards = soup.select(selector)
                if cards:
                    break
            
            if not cards:
                return [{
                    'id': id_imovel,
                    'link': f"https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnIdImovel={id_imovel}",
                    'status': 'sem_detalhes'
                } for id_imovel in ids]
            
            for idx, card in enumerate(cards):
                try:
                    texto = card.get_text(separator=' ', strip=True)
                    
                    if "corretores credenciados" in texto.lower():
                        continue
                    
                    imovel_id = None
                    for attr in ['data-imovel-id', 'data-id', 'id']:
                        if card.get(attr):
                            imovel_id = card.get(attr)
                            break
                    
                    if not imovel_id and idx < len(ids):
                        imovel_id = ids[idx]
                    
                    if not imovel_id:
                        continue
                    
                    valor = None
                    valor_match = re.search(r'R\$\s*([\d.]+,\d{2})', texto)
                    if valor_match:
                        valor = valor_match.group(0)
                    
                    endereco = None
                    end_patterns = [
                        r'(?:Endereço|End\.|Localização):\s*([^|]+?)(?=\||R\$|Tipo|$)',
                        r'(?:Rua|Av\.|Avenida)[^|]{5,100}',
                    ]
                    for pattern in end_patterns:
                        match = re.search(pattern, texto, re.IGNORECASE)
                        if match:
                            endereco = match.group(0).strip()
                            break
                    
                    tipo = None
                    tipo_match = re.search(r'Tipo:\s*([^|]+)', texto)
                    if tipo_match:
                        tipo = tipo_match.group(1).strip()
                    
                    imoveis.append({
                        'id': imovel_id,
                        'valor': valor,
                        'endereco': endereco,
                        'tipo': tipo,
                        'descricao': texto[:500],
                        'link': f"https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnIdImovel={imovel_id}",
                        'data_coleta': datetime.now().isoformat()
                    })
                    
                except:
                    continue
            
            return imoveis
            
        except:
            return []
    
    def consolidar_pares(self, imoveis):
        """Consolida pares de cards"""
        resultado = []
        i = 0
        
        while i < len(imoveis):
            atual = imoveis[i].copy()
            
            if i + 1 < len(imoveis):
                prox = imoveis[i + 1].copy()
                
                if atual.get('valor') and not prox.get('valor'):
                    principal, secundario = atual, prox
                elif prox.get('valor') and not atual.get('valor'):
                    principal, secundario = prox, atual
                else:
                    principal, secundario = atual, prox
                
                merged = {}
                merged.update(secundario)
                merged.update(principal)
                merged["id"] = principal["id"]
                resultado.append(merged)
                i += 2
            else:
                resultado.append(atual)
                i += 1
        
        return resultado
    
    def coletar_imoveis(self, estado, cidade_codigo, cidade_nome, tipo_venda='34', tipo_imovel='4'):
        """Coleta imóveis de uma cidade"""
        
        html_pesquisa, error = self.pesquisar_imoveis(estado, cidade_codigo, tipo_venda, tipo_imovel)
        if error:
            return []
        
        ids = self.extrair_ids_imoveis(html_pesquisa)
        if not ids:
            return []
        
        if len(ids) > 100:
            ids = ids[:100]
        
        html_detalhes, error = self.carregar_detalhes_imoveis(ids)
        if error:
            return []
        
        imoveis = self.parse_imoveis(html_detalhes, ids)
        imoveis = self.consolidar_pares(imoveis)
        
        for imovel in imoveis:
            imovel['estado'] = estado
            imovel['cidade'] = cidade_nome
            imovel['cidade_codigo'] = cidade_codigo
            imovel['tipo_venda_codigo'] = tipo_venda
            imovel['tipo_imovel_codigo'] = tipo_imovel
        
        return imoveis
    
    def scrape_all(self, cidades_dict):
        """Scrape todas as cidades"""
        print("\n" + "="*80)
        print("🏠 CAIXA ECONÔMICA FEDERAL - SCRAPING COMPLETO")
        print("="*80)
        print(f"⏰ Início: {datetime.now().strftime('%H:%M:%S')}")
        print(f"⏱️ Timeout: 2h\n")
        
        self.cookies = self.get_cookies()
        if not self.cookies:
            print("❌ Sem cookies!")
            return {}
        
        resultado_por_estado = {}
        total_imoveis = 0
        total_cidades_processadas = 0
        primeiro_lote = True
        
        for estado, cidades in cidades_dict.items():
            if self.check_timeout():
                print("\n⏰ Timeout global alcançado")
                break
            
            print(f"\n{'='*80}")
            print(f"🗺️ {estado}")
            print(f"{'='*80}")
            
            resultado_por_estado[estado] = []
            
            for cidade_nome, cidade_codigo in cidades.items():
                if self.check_timeout():
                    break
                
                print(f"🏙️  {cidade_nome}...", end=" ", flush=True)
                
                imoveis = self.coletar_imoveis(estado, cidade_codigo, cidade_nome, '34', '4')
                
                if imoveis:
                    print(f"✅ {len(imoveis)}")
                    resultado_por_estado[estado].extend(imoveis)
                    total_imoveis += len(imoveis)
                    
                    # Log detalhado no primeiro lote
                    if primeiro_lote:
                        print(f"\n🎯 PRIMEIRO LOTE ENCONTRADO!")
                        print(f"   Estado: {estado}")
                        print(f"   Cidade: {cidade_nome}")
                        print(f"   Imóveis: {len(imoveis)}")
                        print(f"   Exemplo ID: {imoveis[0]['id']}")
                        print(f"   Link: {imoveis[0]['link']}\n")
                        primeiro_lote = False
                    
                    # Log a cada 200 imóveis
                    if total_imoveis % 200 <= len(imoveis) and total_imoveis >= 200:
                        elapsed = time.time() - self.start_time
                        print(f"\n📊 PROGRESSO: {total_imoveis} imóveis | {total_cidades_processadas} cidades | {elapsed/60:.1f}min\n")
                else:
                    print("⚪")
                
                total_cidades_processadas += 1
            
            print(f"✅ {estado}: {len(resultado_por_estado[estado])} total")
        
        return resultado_por_estado


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
        client.insert_raw('caixa', data)
        
        # 2. Normaliza
        print("\n🧹 Normalizando dados...")
        normalized = normalize('caixa', data)
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
    
    parser = argparse.ArgumentParser(description='Caixa Scraper - GitHub Actions')
    parser.add_argument('--full-update', action='store_true', 
                       help='Todas as cidades (modo produção)')
    
    args = parser.parse_args()
    
    if not args.full_update:
        print("❌ Use --full-update para modo produção")
        return
    
    scraper = CaixaScraper()
    
    # Carrega cidades
    cidades_dict = scraper.carregar_cidades_txt('cidades.txt')
    if not cidades_dict:
        print("❌ Sem cidades!")
        return
    
    print(f"📦 Modo produção: {sum(len(c) for c in cidades_dict.values())} cidades\n")
    
    start_time = time.time()
    resultado = scraper.scrape_all(cidades_dict)
    elapsed = time.time() - start_time
    
    if resultado:
        # Salva JSON local
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"caixa_completo_{timestamp}.json"
        filepath = OUTPUT_DIR / filename
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Salvo: {filepath}")
        
        total_imoveis = sum(len(items) for items in resultado.values())
        
        # Estatísticas
        print(f"\n{'='*80}")
        print("📈 ESTATÍSTICAS")
        print(f"{'='*80}")
        print(f"Estados: {len(resultado)}")
        print(f"Imóveis: {total_imoveis}")
        print(f"Tempo: {elapsed/60:.1f} min\n")
        
        for estado, imoveis in resultado.items():
            if imoveis:
                print(f"   {estado}: {len(imoveis)}")
        
        # Envia pro Supabase
        upload_to_supabase(resultado)
    
    print(f"\n⏰ Fim: {datetime.now().strftime('%H:%M:%S')}")
    print("="*80)


if __name__ == "__main__":
    main()