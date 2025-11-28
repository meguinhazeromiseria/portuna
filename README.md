"# portuna"

Meguinha aqui, se tu achou isso aproveite - uso engenharia reversa em alguns scrapers e outros automação com playwright <3!

Deus abençoe.

# Na pasta do projeto

# 1. Remove o .git (reseta tudo)
rmdir /s /q .git

# 2. Inicia do zero
git init

# 3. Adiciona TUDO de novo
git add .

# 4. Commit
git commit -m "feat: scrapers completos - todos corrigidos e funcionais"

# 5. Conecta ao repo (força push)
git branch -M main
git remote add origin https://github.com/(user)/portuna.git

# 6. FORÇA o push (sobrescreve tudo no GitHub)
git push -f origin main

''''''

\## 🎯 O que vai acontecer:



\*\*Todo dia automaticamente:\*\*

\- 2h → Bradesco roda

\- 3h → Caixa roda

\- 4h → Santander roda

\- 5h → Mega Leilões roda

\- 6h → Sodré roda

\- 7h → Superbid roda



\*\*Dados vão pro Supabase:\*\*

\- `raw\_auctions` → JSON bruto

\- `auctions` → Dados limpos, prontos pro front



---



\## 💰 Depois disso:



\### \*\*Você tem um negócio rodando\*\*



1\. \*\*Front-end Next.js\*\* (consumir `/api/leiloes`)

2\. \*\*Filtros\*\* (estado, tipo, preço)

3\. \*\*Busca\*\* (depois)

4\. \*\*Deploy\*\* (Vercel)

5\. \*\*VENDER\*\* 💸



---



\## 📊 Status Atual:

```

Backend: ████████████████████ 100% ✅

Scrapers: ████████████████████ 100% ✅

Database: ████████████████████ 100% ✅

Automação: ████████████████████ 100% ✅

Front-end: ░░░░░░░░░░░░░░░░░░░░ 0%

