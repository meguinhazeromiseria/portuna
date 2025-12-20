#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO SCRAPER - GitHub Actions Ready
Timeout: 2h58min (10680 segundos)
Delays: 20-40s entre categorias, 1.5-3s entre páginas
Checkpoint automático a cada 1000 itens
✅ FILTRO: Apenas lotes ATIVOS (lot_status_id: 1, 2, 3)
✅ CORREÇÃO: Valores sempre divididos por 100 (API retorna centavos)
"""

import json
import time
import requests
import os
import signal
import random
from playwright.sync_api import sync_playwright
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

API_BASE = "https://www.sodresantoro.com.br"
SEARCH_ENDPOINT = "/api/search-lots"
OUTPUT_DIR = Path("sodre_data")
OUTPUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    "accept": "application/json",
    "accept-language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "content-type": "application/json",
    "origin": "https://www.sodresantoro.com.br",
    "referer": "https://www.sodresantoro.com.br/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# 🔥 TODAS AS 5 CATEGORIAS DO SODRÉ
INDICES = {
    "veiculos": ["veiculos", "judiciais-veiculos"],
    "imoveis": ["imoveis", "judiciais-imoveis"],
    "materiais": ["materiais"],
    "sucatas": ["sucatas"],
    "judiciais": ["judiciais"],
}

# ✅ STATUS DOS LOTES (baseado no site)
# 1 = Aberto para lances
# 2 = Em andamento
# 3 = Aguardando início
# 4 = Encerrado ❌
# 5 = Cancelado ❌
ACTIVE_STATUS = [1, 2, 3]  # Apenas lotes ativos

# Configurações
MAX_EXECUTION_TIME = 10680  # 2h58min
SAVE_CHECKPOINT_EVERY = 1000
MAX_RETRIES = 3
REQUEST_TIMEOUT = 45
REQUEST_DELAY_MIN = 1.5
REQUEST_DELAY_MAX = 3.0
CATEGORY_DELAY_MIN = 20  # 20-40s entre categorias
CATEGORY_DELAY_MAX = 40


class TimeoutException(Exception):
    pass


class SodreScraper:
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
        print(f"\n⚠️ TIMEOUT: 2h58min alcançado, salvando...")
    
    def check_timeout(self) -> bool:
        elapsed = time.time() - self.start_time
        if elapsed > MAX_EXECUTION_TIME or self.should_stop:
            return True
        return False
    
    def random_delay(self, min_sec: float, max_sec: float, reason: str = ""):
        """Delay aleatório para parecer humano"""
        delay = random.uniform(min_sec, max_sec)
        if reason:
            print(f"   ⏳ {reason} ({delay:.1f}s)...", flush=True)
        time.sleep(delay)
    
    def get_cookies(self) -> dict:
        """Captura cookies com anti-detection"""
        print("🍪 Capturando cookies...")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox',
                    ]
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='pt-BR',
                    timezone_id='America/Sao_Paulo',
                )
                
                page = context.new_page()
                
                page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.chrome = {runtime: {}};
                """)
                
                print("   Acessando site...")
                page.goto("https://www.sodresantoro.com.br", wait_until="networkidle", timeout=60000)
                time.sleep(5)

                cookies = context.cookies()
                if not cookies:
                    print("   ⚠️ Tentando página de veículos...")
                    page.goto("https://www.sodresantoro.com.br/veiculos/lotes", wait_until="networkidle")
                    time.sleep(3)
                    cookies = context.cookies()

                browser.close()
                cookie_dict = {c["name"]: c["value"] for c in cookies}
                
                if cookie_dict:
                    print(f"   ✅ Cookies capturados: {len(cookie_dict)} cookies\n")
                else:
                    print("   ⚠️ Nenhum cookie capturado\n")
                    
                return cookie_dict

        except Exception as e:
            print(f"   ❌ Erro ao capturar cookies: {e}")
            return {}
    
    def fetch_page(self, indices: List[str], from_offset: int, page_size: int = 100) -> Tuple[Optional[dict], Optional[str]]:
        """Busca página com retry automático + FILTRO DE LOTES ATIVOS"""
        payload = {
            "indices": indices,
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        # ✅ FILTRO CRÍTICO: Apenas lotes ativos (status 1, 2, 3)
                        {
                            "terms": {
                                "lot_status_id": ACTIVE_STATUS
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            },
            "from": from_offset,
            "size": page_size,
            "sort": [
                {"lot_status_id_order": {"order": "asc"}},
                {"auction_date_init": {"order": "asc"}}
            ]
        }

        url = API_BASE + SEARCH_ENDPOINT
        
        for attempt in range(MAX_RETRIES):
            try:
                r = self.session.post(
                    url, 
                    headers=HEADERS, 
                    json=payload, 
                    cookies=self.cookies, 
                    timeout=REQUEST_TIMEOUT
                )
                
                if r.status_code == 200:
                    return r.json(), None
                elif r.status_code == 429:
                    wait_time = random.randint(15, 30) * (attempt + 1)
                    print(f"   ⚠️ Rate limit (429), aguardando {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return None, f"Status {r.status_code}"
                    
            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    wait_time = random.randint(10, 20)
                    print(f"   ⚠️ Timeout (tentativa {attempt + 1}/{MAX_RETRIES}), aguardando {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return None, "Timeout após 3 tentativas"
                    
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = random.randint(10, 20)
                    print(f"   ⚠️ Erro (tentativa {attempt + 1}/{MAX_RETRIES}): {e}")
                    time.sleep(wait_time)
                else:
                    return None, str(e)
        
        return None, "Falha após múltiplas tentativas"
    
    def collect_category(self, category_key: str, primeiro_lote_global: dict) -> List[dict]:
        """Coleta todos os lotes de uma categoria com checkpoints"""
        indices = INDICES[category_key]
        category_name = category_key.upper()
        
        print(f"\n📊 Coletando {category_name}...")
        
        all_lots = []
        from_offset = 0
        page_num = 1
        checkpoint_counter = 0
        consecutive_errors = 0
        
        while not self.check_timeout():
            if consecutive_errors >= MAX_RETRIES:
                print(f"   ❌ {MAX_RETRIES} erros consecutivos - parando categoria")
                break
            
            data, error = self.fetch_page(indices, from_offset, 100)
            
            if error:
                print(f"   ❌ Página {page_num}: {error}")
                consecutive_errors += 1
                self.random_delay(3, 6, "Aguardando antes de retry")
                continue
            
            consecutive_errors = 0
            lots = data.get("results", [])
            total = data.get("total", 0)
            
            if not lots:
                print(f"   ✅ Fim da paginação (página {page_num})")
                break
            
            all_lots.extend(lots)
            print(f"   📦 Pág {page_num}: +{len(lots)} lotes | Total: {len(all_lots)}/{total}", flush=True)
            
            # Log detalhado no primeiro lote
            if primeiro_lote_global.get('mostrado') == False and lots:
                normalized = self.normalize_to_schema(lots[0], category_key)
                if normalized:
                    print(f"\n🎯 PRIMEIRO LOTE ENCONTRADO!")
                    print(f"   Categoria: {category_name}")
                    print(f"   Título: {normalized['title']}")
                    print(f"   Valor: {normalized.get('value_text', 'N/A')}")
                    print(f"   Status: {lots[0].get('lot_status', 'N/A')} (ID: {lots[0].get('lot_status_id')})")
                    print(f"   ID: {normalized['external_id']}")
                    print(f"   Link: {normalized['link']}\n")
                    primeiro_lote_global['mostrado'] = True
            
            # Checkpoint automático a cada 1000
            if len(all_lots) >= (checkpoint_counter + 1) * SAVE_CHECKPOINT_EVERY:
                checkpoint_counter += 1
                self.save_checkpoint(all_lots, category_key, checkpoint_counter)
            
            if len(all_lots) >= total:
                break
            
            from_offset += 100
            page_num += 1
            
            # Delay entre páginas
            self.random_delay(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX, "Próxima página")
        
        if self.check_timeout():
            print(f"\n⏰ Timeout alcançado na página {page_num}")
        
        return all_lots
    
    def normalize_to_schema(self, lot: dict, category_key: str) -> Optional[Dict]:
        """Normaliza para o schema do banco (compatível com Supabase)"""
        
        # ✅ PROTEÇÃO: Ignora lotes None ou inválidos
        if lot is None or not isinstance(lot, dict):
            print(f"⚠️ Lote inválido ignorado: {type(lot)}")
            return None
        
        # ✅ VALIDAÇÃO EXTRA: Verifica se o lote está ativo
        lot_status_id = lot.get("lot_status_id")
        if lot_status_id not in ACTIVE_STATUS:
            print(f"⚠️ Lote encerrado ignorado: status_id={lot_status_id}")
            return None
        
        auction_id = lot.get("auction_id")
        lot_id = lot.get("lot_id") or lot.get("id")
        
        if not lot_id:
            print(f"⚠️ Lote sem ID ignorado")
            return None
        
        external_id = f"sodre_{lot_id}"
        
        # URL correta
        link = f"https://leilao.sodresantoro.com.br/leilao/{auction_id}/lote/{lot_id}/"
        
        # Título limpo
        title = (lot.get("lot_title") or "").strip()
        if not title:
            marca = lot.get("lot_brand", "")
            modelo = lot.get("lot_model", "")
            title = f"{marca} {modelo}".strip() or "Sem título"
        
        # Categoria inteligente
        categoria_raw = (lot.get("lot_category") or "").lower()
        segmento = (lot.get("segment_label") or "").lower()
        
        if category_key == "veiculos" or "carro" in categoria_raw or "moto" in categoria_raw or "veículo" in categoria_raw:
            category = "Carros & Motos"
        elif category_key == "imoveis" or "imóvel" in categoria_raw or "imovel" in categoria_raw:
            category = "Imóveis"
        elif category_key == "materiais":
            category = "Materiais"
        elif category_key == "sucatas":
            category = "Sucatas"
        elif category_key == "judiciais":
            category = "Judiciais"
        elif segmento:
            category = segmento.title()
        else:
            category = "outros"
        
        # ✅ CORREÇÃO CRÍTICA: Valor - API retorna SEMPRE em centavos!
        value_raw = lot.get("bid_actual") or lot.get("bid_initial")
        
        if isinstance(value_raw, str):
            # Remove formatação brasileira
            value_raw = value_raw.replace("R$", "").replace(".", "").replace(",", ".").strip()
            try:
                value = float(value_raw)
            except:
                value = None
        elif isinstance(value_raw, (int, float)):
            value = float(value_raw)
        else:
            value = None
        
        # ✅ SEMPRE divide por 100 (API sempre retorna centavos)
        if value is not None and value > 0:
            value = value / 100
        
        # Formata texto em formato brasileiro
        if value:
            value_text = f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        else:
            value_text = None
        
        # Localização
        location = lot.get("lot_location", "") or ""
        city = None
        state = None
        
        if "/" in location:
            parts = location.split("/")
            city = parts[0].strip() if len(parts) > 0 else None
            state = parts[1].strip() if len(parts) > 1 else None
        elif " - " in location:
            parts = location.split(" - ")
            city = parts[0].strip() if len(parts) > 0 else None
            state = parts[1].strip() if len(parts) > 1 else None
        
        # Valida UF (2 caracteres maiúsculos)
        if state and (len(state) != 2 or not state.isupper()):
            state = None
        
        # Descrição
        description = lot.get("lot_description", "")
        description_preview = description[:200] if description else title[:200]
        
        # Data do leilão
        auction_date = None
        days_remaining = None
        
        date_str = lot.get("lot_date_end") or lot.get("auction_date_init")
        if date_str:
            try:
                auction_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                days_remaining = max(0, (auction_date - datetime.now(auction_date.tzinfo)).days)
            except:
                pass
        
        # Metadata completo
        metadata = {
            "leilao": {
                "id": auction_id,
                "nome": lot.get("auction_name"),
                "leiloeiro": lot.get("auctioneer_name"),
                "data_inicio": lot.get("auction_date_init"),
            },
            "lote": {
                "numero": lot.get("lot_number"),
                "status": lot.get("lot_status"),
                "status_id": lot.get("lot_status_id"),
                "origem": lot.get("lot_origin"),
                "cliente": lot.get("client_name"),
            },
            "veiculo": {
                "marca": lot.get("lot_brand"),
                "modelo": lot.get("lot_model"),
                "placa": lot.get("lot_plate"),
                "cor": lot.get("lot_color"),
                "km": lot.get("lot_km", 0),
                "combustivel": lot.get("lot_fuel"),
                "cambio": lot.get("lot_transmission"),
                "ano_fabricacao": lot.get("lot_year_manufacture"),
                "ano_modelo": lot.get("lot_year_model"),
                "sinistro": lot.get("lot_sinister"),
                "opcionais": lot.get("lot_optionals", []),
            },
            "lances": {
                "inicial": lot.get("bid_initial"),
                "atual": lot.get("bid_actual"),
                "tem_lance": lot.get("bid_has_bid", False),
                "total_lances": lot.get("bid_count", 0),
            },
            "midia": {
                "imagens": lot.get("lot_pictures", []),
                "total_fotos": len(lot.get("lot_pictures", [])),
            },
            "outros": {
                "visitas": lot.get("lot_visits", 0),
                "financiavel": lot.get("lot_financeable"),
                "destaque": lot.get("is_highlight", False),
            }
        }
        
        return {
            "source": "sodre",
            "external_id": external_id,
            "title": title,
            "category": category,
            "value": value,
            "value_text": value_text,
            "city": city,
            "state": state,
            "description_preview": description_preview,
            "auction_date": auction_date.isoformat() if auction_date else None,
            "days_remaining": days_remaining,
            "auction_type": "Leilão",
            "auction_name": lot.get("auction_name"),
            "store_name": lot.get("auctioneer_name"),
            "lot_number": lot.get("lot_number"),
            "total_visits": lot.get("lot_visits", 0),
            "total_bids": lot.get("bid_count", 0),
            "total_bidders": 0,
            "description": description,
            "address": location,
            "link": link,
            "metadata": metadata,
        }
    
    def save_checkpoint(self, lots: List[dict], category_key: str, checkpoint_num: int):
        """Salva checkpoint e envia pro Supabase"""
        
        # ✅ Filtra None ANTES de normalizar
        lots = [lot for lot in lots if lot is not None]
        
        # Normaliza e filtra resultados None
        normalized = []
        for lot in lots:
            result = self.normalize_to_schema(lot, category_key)
            if result is not None:
                normalized.append(result)
        
        if not normalized:
            print(f"   ⚠️ Nenhum lote válido para checkpoint {checkpoint_num}")
            return
        
        # Remove duplicatas
        unique = {item["external_id"]: item for item in normalized}
        normalized = list(unique.values())
        
        # Salva JSON local
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"sodre_{category_key}_ckpt{checkpoint_num}_{timestamp}.json"
        filepath = OUTPUT_DIR / filename
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)
        
        print(f"   💾 Checkpoint {checkpoint_num}: {len(normalized)} itens salvos")
        
        # Envia pro Supabase
        upload_to_supabase(normalized)
    
    def scrape_all(self) -> List[Dict]:
        """Scrape todas as categorias"""
        print("\n" + "="*80)
        print("🚀 SODRÉ SANTORO - SCRAPING COMPLETO (5 CATEGORIAS)")
        print("="*80)
        print(f"⏰ Início: {datetime.now().strftime('%H:%M:%S')}")
        print(f"⏱️ Timeout: 2h58min")
        print(f"✅ Filtro: Apenas lotes ATIVOS (status 1, 2, 3)")
        print(f"✅ Correção: Valores sempre divididos por 100 (centavos → reais)")
        print(f"⏳ Delay entre categorias: {CATEGORY_DELAY_MIN}-{CATEGORY_DELAY_MAX}s\n")
        
        # Captura cookies uma vez
        self.cookies = self.get_cookies()
        
        all_lots = []
        category_count = 0
        total_items = 0
        primeiro_lote_global = {'mostrado': False}
        
        for category_key in INDICES.keys():
            if self.check_timeout():
                print("\n⏰ Timeout global alcançado")
                break
            
            category_count += 1
            lots = self.collect_category(category_key, primeiro_lote_global)
            
            if lots:
                # Filtra None e normaliza
                lots = [lot for lot in lots if lot is not None]
                normalized = []
                for lot in lots:
                    result = self.normalize_to_schema(lot, category_key)
                    if result is not None:
                        normalized.append(result)
                
                all_lots.extend(normalized)
                total_items += len(normalized)
                
                # Log a cada 200 itens
                if total_items % 200 <= len(normalized) and total_items >= 200:
                    elapsed = time.time() - self.start_time
                    print(f"\n📊 PROGRESSO: {total_items} lotes | {category_count} categorias | {elapsed/60:.1f}min\n")
            
            # Delay entre categorias (não aplica na última)
            if category_count < len(INDICES):
                self.random_delay(CATEGORY_DELAY_MIN, CATEGORY_DELAY_MAX, 
                                f"Próxima categoria ({category_count}/{len(INDICES)})")
        
        # Remove duplicatas finais
        unique = {item["external_id"]: item for item in all_lots}
        all_lots = list(unique.values())
        
        print(f"\n✅ Total único: {len(all_lots)} lotes ATIVOS")
        
        return all_lots


# ============================================================
# INTEGRAÇÃO SUPABASE
# ============================================================

def upload_to_supabase(items: List[Dict]) -> bool:
    """Envia para Supabase"""
    try:
        from supabase_client import SupabaseClient
        
        client = SupabaseClient()
        
        print(f"\n{'='*60}")
        print("📤 ENVIANDO PARA SUPABASE")
        print(f"{'='*60}\n")
        
        # 1. Salva RAW
        print("💾 Salvando dados RAW...")
        client.insert_raw('sodre', items)
        
        # 2. Insere normalizado (já normalizado)
        print(f"\n💾 Inserindo {len(items)} itens normalizados...")
        inserted = client.insert_normalized(items)
        
        if inserted > 0:
            print(f"✅ {inserted} itens processados no banco")
            return True
        else:
            print("⚠️ Nenhum item novo inserido (duplicados)")
            return False
        
    except ImportError:
        print("\n❌ supabase_client.py não encontrado")
        return False
    except Exception as e:
        print(f"\n❌ Erro ao enviar para Supabase: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================
# MAIN
# ============================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Sodré Santoro Scraper - GitHub Actions')
    parser.add_argument('--full-update', action='store_true', 
                       help='Todas as categorias (modo produção)')
    
    args = parser.parse_args()
    
    scraper = SodreScraper()
    
    if args.full_update:
        print(f"📦 Modo produção: {len(INDICES)} categorias\n")
        
        items = scraper.scrape_all()
        
        if items:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"sodre_full_{timestamp}.json"
            filepath = OUTPUT_DIR / filename
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            
            print(f"\n💾 Salvo: {filepath}")
            upload_to_supabase(items)
            
            # Estatísticas
            print(f"\n{'='*80}")
            print("📈 ESTATÍSTICAS")
            print(f"{'='*80}")
            print(f"Total: {len(items)} lotes ATIVOS")
            
            # Por categoria
            cats = {}
            for item in items:
                cat = item.get("category", "outros")
                cats[cat] = cats.get(cat, 0) + 1
            
            print("\nPor categoria:")
            for cat, count in sorted(cats.items(), key=lambda x: x[1], reverse=True):
                print(f"  • {cat}: {count}")
    
    else:
        print("❌ Use --full-update para modo produção")
        return
    
    print(f"\n⏰ Fim: {datetime.now().strftime('%H:%M:%S')}")
    print("="*80)


if __name__ == "__main__":
    main()
