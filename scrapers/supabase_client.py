#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - NORMALIZAÇÃO ROBUSTA
Suporta: Bradesco, Caixa, Sodré
"""

import os
import re
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta


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
            'Prefer': 'return=representation'
        }
    
    def insert_raw(self, source: str, data: Any) -> bool:
        """Insere dados RAW (backup completo)"""
        url = f"{self.url}/rest/v1/raw_auctions"
        payload = {'source': source, 'data': data}
        
        try:
            r = requests.post(url, headers=self.headers, json=payload, timeout=30)
            r.raise_for_status()
            print(f"✅ RAW salvo: {source}")
            return True
        except Exception as e:
            print(f"❌ Erro RAW: {e}")
            return False
    
    def insert_normalized(self, items: List[Dict]) -> int:
        """INSERT com detecção de duplicatas via constraint do banco"""
        if not items:
            return 0
        
        url = f"{self.url}/rest/v1/auctions"
        total_inserted = 0
        total_duplicated = 0
        
        # Batch de 500
        for i in range(0, len(items), 500):
            batch = items[i:i+500]
            try:
                r = requests.post(url, headers=self.headers, json=batch, timeout=60)
                
                if r.status_code == 409:
                    # Duplicatas pela constraint
                    print(f"   ⚪ Batch {i//500 + 1}: {len(batch)} duplicados")
                    total_duplicated += len(batch)
                elif r.status_code in (200, 201):
                    # Inseridos com sucesso
                    print(f"   ✅ Batch {i//500 + 1}: {len(batch)} novos")
                    total_inserted += len(batch)
                else:
                    r.raise_for_status()
                    
            except Exception as e:
                if "409" in str(e) or "duplicate key" in str(e).lower():
                    print(f"   ⚪ Batch {i//500 + 1}: {len(batch)} duplicados")
                    total_duplicated += len(batch)
                else:
                    print(f"   ❌ Erro batch {i//500 + 1}: {e}")
        
        print(f"\n📊 Inseridos: {total_inserted} | Duplicados: {total_duplicated}")
        return total_inserted


# ============================================================
# 🧹 FUNÇÕES DE LIMPEZA E PARSING
# ============================================================

def clean_text(text: Optional[str], max_len: Optional[int] = None) -> Optional[str]:
    """Limpa texto: remove espaços extras, quebras, caracteres especiais"""
    if not text:
        return None
    
    text = re.sub(r'\s+', ' ', str(text)).strip()
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
    
    if max_len and len(text) > max_len:
        text = text[:max_len].rsplit(' ', 1)[0] + '...'
    
    return text if text else None


def extract_state(text: Optional[str]) -> Optional[str]:
    """Extrai UF de texto livre"""
    if not text:
        return None
    
    # Padrão: "CIDADE - UF" ou "CIDADE/UF"
    match = re.search(r'[-/]\s*([A-Z]{2})\s*$', text.upper())
    if match:
        return match.group(1)
    
    # Lista completa de UFs
    ufs = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
           'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']
    
    for uf in ufs:
        if re.search(rf'\b{uf}\b', text.upper()):
            return uf
    
    return None


def parse_address(text: Optional[str]) -> Dict[str, Optional[str]]:
    """Parse inteligente de endereço"""
    result = {'address': None, 'city': None, 'state': None}
    
    if not text:
        return result
    
    text = clean_text(text)
    
    # "RUA X, N - BAIRRO, CIDADE - UF"
    match = re.match(r'(.+?)[-,]\s*([^-,]+)\s*[-,]\s*([A-Z]{2})\s*$', text)
    if match:
        result['address'] = clean_text(match.group(1))
        result['city'] = clean_text(match.group(2))
        result['state'] = match.group(3)
        return result
    
    # "CIDADE - UF"
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
    """Converte valor para float"""
    if value is None:
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # Remove tudo exceto números e vírgula
        value = re.sub(r'[^\d,]', '', value).replace(',', '.')
        try:
            return float(value) if value else None
        except:
            return None
    
    return None


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Converte data para ISO (YYYY-MM-DD) com validação"""
    if not date_str:
        return None
    
    parsed_date = None
    
    # ISO 8601 completo (2024-12-31T23:59:59Z)
    match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
    if match:
        parsed_date = match.group(1)
    
    # DD/MM/YYYY
    if not parsed_date:
        match = re.search(r'(\d{2})/(\d{2})/(\d{4})', date_str)
        if match:
            day, month, year = match.groups()
            parsed_date = f"{year}-{month}-{day}"
    
    # Validação: data futura ou até 2 anos atrás
    if parsed_date:
        try:
            date_obj = datetime.strptime(parsed_date, '%Y-%m-%d')
            hoje = datetime.now()
            dois_anos_atras = hoje - timedelta(days=730)
            
            if date_obj < dois_anos_atras:
                return None
            
            return parsed_date
        except:
            return None
    
    return None


def generate_clean_external_id(source: str, raw_id: Any) -> str:
    """Gera external_id limpo: {source}_{id}"""
    if not raw_id:
        return f"{source}_unknown_{int(datetime.now().timestamp())}"
    
    clean_id = re.sub(r'[^a-zA-Z0-9-]', '_', str(raw_id).lower())
    clean_id = re.sub(r'_+', '_', clean_id).strip('_')
    
    return f"{source}_{clean_id}"


def extract_category(text: Optional[str], tipo: Optional[str]) -> Optional[str]:
    """Extrai categoria específica"""
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
    if 'caminhão' in text or 'caminhao' in text or 'truck' in text:
        return 'Caminhão'
    if 'van' in text or 'kombi' in text:
        return 'Van'
    
    if tipo:
        return tipo.capitalize()
    
    return None


def extract_title_from_description(desc: Optional[str], max_len: int = 100) -> Optional[str]:
    """Extrai título inteligente da descrição"""
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
# 🎯 NORMALIZADORES POR FONTE
# ============================================================

def normalize_bradesco(data: Dict) -> List[Dict]:
    """Normaliza Bradesco"""
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
    """Normaliza Caixa"""
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
    """Normaliza Sodré - dados já vêm normalizados do scraper"""
    results = []
    
    for item in data:
        external_id = item.get('external_id')
        link = item.get('link')
        
        if not external_id or not link:
            continue
        
        # Valida estado (2 caracteres)
        state = item.get('state')
        if state and len(state) != 2:
            state = None
        
        # Converte auction_date se for string ISO
        auction_date = item.get('auction_date')
        if auction_date and isinstance(auction_date, str):
            try:
                auction_date = auction_date.replace('Z', '+00:00')
                dt = datetime.fromisoformat(auction_date)
                auction_date = dt.strftime('%Y-%m-%d %H:%M:%S%z')
            except:
                auction_date = None
        
        # Garante que category seja lowercase se for 'outros'
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


# ============================================================
# 🎯 DISPATCHER
# ============================================================

NORMALIZERS = {
    'bradesco': normalize_bradesco,
    'caixa': normalize_caixa,
    'sodre': normalize_sodre,
}


def normalize(source: str, data: Any) -> List[Dict]:
    """Normalização principal - despacha para o normalizer correto"""
    normalizer = NORMALIZERS.get(source.lower())
    if not normalizer:
        raise ValueError(f"Fonte desconhecida: {source}. Suportadas: {', '.join(NORMALIZERS.keys())}")
    
    normalized = normalizer(data)
    
    # Validação final
    valid_items = []
    for item in normalized:
        # Obrigatórios: external_id e link
        if not item.get('external_id') or not item.get('link'):
            continue
        
        # Valida UF (2 caracteres maiúsculos ou NULL)
        if item.get('state') and len(item['state']) != 2:
            item['state'] = None
        
        valid_items.append(item)
    
    return valid_items