#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT v3.0 - Bradesco, Caixa, Sodré
Performance máxima com RPC nativo
"""

import os
import re
import time
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class SupabaseClient:
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        
        self.session = self._create_session()
        self._rpc_available = None
        self._check_rpc_availability()
    
    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET", "PATCH"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
    
    def _check_rpc_availability(self) -> bool:
        if self._rpc_available is not None:
            return self._rpc_available
        
        try:
            url = f"{self.url}/rest/v1/rpc/upsert_auctions_v2"
            r = self.session.post(url, headers=self.headers, json={'items': []}, timeout=5)
            self._rpc_available = r.status_code in (200, 201)
            
            if self._rpc_available:
                print("✅ RPC upsert_auctions_v2 disponível")
            else:
                print("⚠️ RPC não disponível - execute install.sql")
        except Exception as e:
            print(f"⚠️ Erro ao verificar RPC: {e}")
            self._rpc_available = False
        
        return self._rpc_available
    
    def insert_raw(self, source: str, data: Any) -> bool:
        """Compatibilidade - não faz nada"""
        return True
    
    def upsert_normalized(self, items: List[Dict]) -> Dict[str, int]:
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0, 'time_ms': 0}
        
        start_time = time.time()
        
        if self._rpc_available:
            stats = self._upsert_via_rpc(items)
        else:
            print("⚠️ Execute install.sql para melhor performance!")
            stats = self._upsert_fallback(items)
        
        stats['time_ms'] = int((time.time() - start_time) * 1000)
        return stats
    
    def _upsert_via_rpc(self, items: List[Dict]) -> Dict[str, int]:
        url = f"{self.url}/rest/v1/rpc/upsert_auctions_v2"
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 500
        
        print(f"📦 Processando {len(items)} itens")
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            
            try:
                r = self.session.post(url, headers=self.headers, json={'items': batch}, timeout=120)
                
                if r.status_code == 200:
                    result = r.json()
                    stats['inserted'] += result.get('inserted', 0)
                    stats['updated'] += result.get('updated', 0)
                    stats['errors'] += result.get('errors', 0)
                    print(f"   ✅ Batch {i//batch_size + 1}: +{result.get('inserted', 0)} novos, ~{result.get('updated', 0)} atualizados")
                else:
                    print(f"   ❌ Batch {i//batch_size + 1}: HTTP {r.status_code}")
                    stats['errors'] += len(batch)
            except Exception as e:
                print(f"   ❌ Batch {i//batch_size + 1}: {str(e)[:100]}")
                stats['errors'] += len(batch)
        
        print(f"\n📊 RESULTADO: {stats['inserted']} novos | {stats['updated']} atualizados | {stats['errors']} erros")
        return stats
    
    def _upsert_fallback(self, items: List[Dict]) -> Dict[str, int]:
        url = f"{self.url}/rest/v1/auctions"
        headers = self.headers.copy()
        headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        
        for i in range(0, len(items), 200):
            batch = items[i:i+200]
            try:
                r = self.session.post(url, headers=headers, json=batch, timeout=30)
                if r.status_code in (200, 201):
                    stats['inserted'] += len(batch)
                    print(f"   ✅ Batch {i//200 + 1}: {len(batch)} processados")
                else:
                    stats['errors'] += len(batch)
            except Exception as e:
                print(f"   ❌ Erro: {str(e)[:100]}")
                stats['errors'] += len(batch)
        
        return stats
    
    def insert_normalized(self, items: List[Dict]) -> int:
        result = self.upsert_normalized(items)
        return result['inserted'] + result['updated']
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


# ============================================================
# HELPERS
# ============================================================

_REGEX_CACHE = {
    'whitespace': re.compile(r'\s+'),
    'control_chars': re.compile(r'[\x00-\x1f\x7f-\x9f]'),
    'state_end': re.compile(r'[-/]\s*([A-Z]{2})\s*$'),
}

_UFS = {'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
        'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'}


def clean_text(text: Optional[str], max_len: Optional[int] = None) -> Optional[str]:
    if not text:
        return None
    text = _REGEX_CACHE['whitespace'].sub(' ', str(text)).strip()
    text = _REGEX_CACHE['control_chars'].sub('', text)
    if max_len and len(text) > max_len:
        text = text[:max_len].rsplit(' ', 1)[0] + '...'
    return text if text else None


def extract_state(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    
    text_upper = text.upper()
    match = _REGEX_CACHE['state_end'].search(text_upper)
    if match:
        uf = match.group(1)
        return uf if uf in _UFS else None
    
    for word in text_upper.split():
        if word in _UFS:
            return word
    
    return None


def parse_address(text: Optional[str]) -> Dict[str, Optional[str]]:
    result = {'address': None, 'city': None, 'state': None}
    
    if not text:
        return result
    
    text = clean_text(text)
    
    match = re.match(r'(.+?)[-,]\s*([^-,]+)\s*[-,]\s*([A-Z]{2})\s*$', text)
    if match:
        result['address'] = clean_text(match.group(1))
        result['city'] = clean_text(match.group(2))
        result['state'] = match.group(3)
        return result
    
    match = re.search(r'([^-/]+)\s*[-/]\s*([A-Z]{2})\s*$', text)
    if match:
        result['city'] = clean_text(match.group(1))
        result['state'] = match.group(2)
        result['address'] = text
        return result
    
    result['state'] = extract_state(text)
    result['address'] = text
    
    return result


def parse_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        clean = re.sub(r'[^\d,]', '', value).replace(',', '.')
        try:
            return float(clean) if clean else None
        except:
            return None
    return None


def parse_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    
    match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
    if match:
        parsed = match.group(1)
    else:
        match = re.search(r'(\d{2})/(\d{2})/(\d{4})', date_str)
        if match:
            day, month, year = match.groups()
            parsed = f"{year}-{month}-{day}"
        else:
            return None
    
    try:
        date_obj = datetime.strptime(parsed, '%Y-%m-%d')
        dois_anos_atras = datetime.now() - timedelta(days=730)
        if date_obj < dois_anos_atras:
            return None
        return parsed
    except:
        return None


def generate_clean_external_id(source: str, raw_id: Any) -> str:
    if not raw_id:
        return f"{source}_unknown_{int(time.time() * 1000)}"
    clean = re.sub(r'[^a-zA-Z0-9-]', '_', str(raw_id).lower())
    clean = re.sub(r'_+', '_', clean).strip('_')
    return f"{source}_{clean}"


def extract_category(text: Optional[str], tipo: Optional[str]) -> Optional[str]:
    if not text:
        return None
    
    text = text.lower()
    
    if 'apartamento' in text or 'apto' in text:
        return 'Apartamento'
    if 'casa' in text:
        return 'Casa'
    if 'terreno' in text or 'lote' in text:
        return 'Terreno'
    if 'sala comercial' in text or 'loja' in text:
        return 'Comercial'
    if 'carro' in text or 'sedan' in text or 'hatch' in text:
        return 'Carro'
    if 'moto' in text or 'motocicleta' in text:
        return 'Moto'
    if 'caminhão' in text or 'truck' in text:
        return 'Caminhão'
    if 'van' in text or 'kombi' in text:
        return 'Van'
    
    if tipo:
        return tipo.capitalize()
    
    return None


def extract_title_from_description(desc: Optional[str], max_len: int = 100) -> Optional[str]:
    if not desc:
        return None
    
    desc = clean_text(desc)
    desc = re.sub(r'^(Data do leilão:\s*\d{2}/\d{2}/\d{4}\s*\|\s*)', '', desc, flags=re.IGNORECASE)
    
    sentences = re.split(r'[.!?|]', desc)
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) > 20:
            return clean_text(sentence, max_len)
    
    return clean_text(desc, max_len)


# ============================================================
# NORMALIZERS
# ============================================================

def normalize_bradesco(data: Dict) -> List[Dict]:
    results = []
    
    for estado, items in data.items():
        for item in items:
            addr_parsed = parse_address(item.get('descricao'))
            
            title = extract_title_from_description(item.get('descricao'), 120)
            if not title:
                title = f"{item.get('tipo', 'Imóvel').capitalize()} em {item.get('cidade', 'N/A')}"
            
            external_id = generate_clean_external_id('bradesco', item.get('id'))
            category = extract_category(item.get('descricao'), item.get('tipo'))
            
            results.append({
                'source': 'bradesco',
                'external_id': external_id,
                'category': category or 'Imóveis',
                'title': title,
                'value': parse_value(item.get('valor')),
                'value_text': item.get('valor'),
                'city': item.get('cidade'),
                'state': estado if len(estado) == 2 else addr_parsed['state'],
                'address': addr_parsed['address'] or item.get('endereco'),
                'auction_date': parse_date(item.get('leilao_data')),
                'link': item.get('link'),
                'description': item.get('descricao'),
                'description_preview': clean_text(item.get('descricao'), 200),
                'metadata': {
                    'ano': item.get('ano'),
                    'original_id': item.get('id'),
                    'tipo': item.get('tipo'),
                }
            })
    
    return results


def normalize_caixa(data: Dict) -> List[Dict]:
    results = []
    
    for estado, items in data.items():
        for item in items:
            addr_parsed = parse_address(item.get('endereco'))
            
            title = clean_text(item.get('endereco'), 120)
            if not title:
                title = f"Imóvel em {item.get('cidade', 'N/A')} - {estado}"
            
            external_id = generate_clean_external_id('caixa', item.get('id'))
            category = extract_category(item.get('tipo') or item.get('descricao'), 'imovel')
            
            results.append({
                'source': 'caixa',
                'external_id': external_id,
                'category': category or 'Imóveis',
                'title': title,
                'value': parse_value(item.get('valor')),
                'value_text': item.get('valor'),
                'city': item.get('cidade'),
                'state': estado if len(estado) == 2 else addr_parsed['state'],
                'address': addr_parsed['address'],
                'auction_date': None,
                'link': item.get('link'),
                'description': item.get('descricao'),
                'description_preview': clean_text(item.get('descricao'), 200),
                'metadata': {
                    'tipo': item.get('tipo'),
                    'venda_direta': True,
                    'cidade_codigo': item.get('cidade_codigo'),
                    'original_id': item.get('id')
                }
            })
    
    return results


def normalize_sodre(data: List[Dict]) -> List[Dict]:
    results = []
    
    for item in data:
        external_id = item.get('external_id')
        link = item.get('link')
        
        if not external_id or not link:
            continue
        
        state = item.get('state')
        if state and len(state) != 2:
            state = None
        
        auction_date = item.get('auction_date')
        if auction_date and isinstance(auction_date, str):
            try:
                auction_date = auction_date.replace('Z', '+00:00')
                dt = datetime.fromisoformat(auction_date)
                auction_date = dt.strftime('%Y-%m-%d %H:%M:%S%z')
            except:
                auction_date = None
        
        category = item.get('category', 'outros')
        if not category or category == 'Outros':
            category = 'outros'
        
        results.append({
            'source': 'sodre',
            'external_id': external_id,
            'category': category,
            'title': clean_text(item.get('title'), 200),
            'description': item.get('description'),
            'description_preview': item.get('description_preview'),
            'value': item.get('value'),
            'value_text': item.get('value_text'),
            'city': item.get('city'),
            'state': state,
            'address': item.get('address'),
            'auction_date': auction_date,
            'days_remaining': item.get('days_remaining'),
            'link': link,
            'metadata': item.get('metadata', {}),
            'auction_type': item.get('auction_type'),
            'auction_name': item.get('auction_name'),
            'store_name': item.get('store_name'),
            'lot_number': item.get('lot_number'),
            'total_visits': item.get('total_visits', 0),
            'total_bids': item.get('total_bids', 0),
            'total_bidders': item.get('total_bidders', 0),
        })
    
    return results


NORMALIZERS = {
    'bradesco': normalize_bradesco,
    'caixa': normalize_caixa,
    'sodre': normalize_sodre,
}


def normalize(source: str, data: Any) -> List[Dict]:
    normalizer = NORMALIZERS.get(source.lower())
    if not normalizer:
        raise ValueError(f"Fonte desconhecida: {source}. Suportadas: {', '.join(NORMALIZERS.keys())}")
    
    normalized = normalizer(data)
    
    valid_items = []
    for item in normalized:
        if not item.get('external_id') or not item.get('link'):
            continue
        if item.get('state') and len(item['state']) != 2:
            item['state'] = None
        valid_items.append(item)
    
    return valid_items
