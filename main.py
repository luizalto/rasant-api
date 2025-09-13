import hashlib
import os
import re
import sqlite3
import time
import urllib.parse
from typing import Dict, List, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, PlainTextResponse

DB_PATH = os.getenv("COUNTERS_DB", "counters.sqlite")

app = FastAPI(title="Shopee shortlink + UTM dinâmica (todas as UTMs)")

# --------- DB (contador) ----------
def _ensure_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS counters(
            k TEXT PRIMARY KEY,
            n INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    return conn

DB = _ensure_db()

def _db_next(k: str) -> int:
    cur = DB.cursor()
    row = cur.execute("SELECT n FROM counters WHERE k=?", (k,)).fetchone()
    if row is None:
        n = 0
        cur.execute("INSERT INTO counters(k,n,updated_at) VALUES(?,?,?)", (k, n, int(time.time())))
    else:
        n = int(row[0]) + 1
        cur.execute("UPDATE counters SET n=?, updated_at=? WHERE k=?", (n, int(time.time()), k))
    DB.commit()
    return n

# --------- Helpers ----------
RE_TRAIL_N = re.compile(r"(.*?)(?:N\d+)$", re.IGNORECASE)

def parse_query(url: str) -> Tuple[urllib.parse.SplitResult, List[Tuple[str, str]]]:
    p = urllib.parse.urlsplit(url)
    pairs = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
    return p, pairs

def strip_trailing_N(value: str) -> str:
    """
    Remove um sufixo N<numero> no final do valor, se existir.
    """
    m = RE_TRAIL_N.match(value)
    return m.group(1) if m else value

def normalized_key(url: str) -> str:
    """
    Gera chave estável para o contador:
    - ordena parâmetros da query;
    - remove sufixos N<dígitos> do final de **valores de UTMs**.
    """
    p, pairs = parse_query(url)
    norm_pairs: List[Tuple[str, str]] = []
    for k, v in pairs:
        if k.lower().startswith("utm_"):
            v = strip_trailing_N(v)
        norm_pairs.append((k, v))
    norm_pairs.sort()
    norm_q = urllib.parse.urlencode(norm_pairs, doseq=True)
    key_url = urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, norm_q, p.fragment))
    return hashlib.sha1(key_url.encode("utf-8")).hexdigest()

def add_N_to_all_utms(url: str, n_value: int) -> str:
    """
    Para cada parâmetro 'utm_*' no URL, acrescenta (ou atualiza) o sufixo N<n_value>.
    - Se o valor já termina com N<algo>, substitui pelo N<n_value> atual.
    - Se não termina, concatena N<n_value>.
    - Se não houver nenhuma UTM, cria utm_content=N<n_value>.
    """
    p, pairs = parse_query(url)
    q: Dict[str, str] = {}
    for k, v in pairs:
        q[k] = v

    touched_any = False
    for k in list(q.keys()):
        if k.lower().startswith("utm_"):
            old = q[k] or ""
            base = strip_trailing_N(old)
            # Se já havia um N e depois do strip ficou vazio, base = "" (ok).
            new_val = (base + f"N{n_value}") if base else f"N{n_value}"
            q[k] = new_val
            touched_any = True

    if not touched_any:
        # Nenhuma UTM presente -> cria utm_content=N<n>
        q["utm_content"] = f"N{n_value}"

    new_query = urllib.parse.urlencode(q, doseq=True)
    return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, new_query, p.fragment))

# --------- Rotas ----------
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

# padrão de shortlink: /{url_codificada}
@app.get("/{encoded:path}")
def go(encoded: str, request: Request):
    try:
        target = urllib.parse.unquote(encoded)
    except Exception:
        return PlainTextResponse("Bad encoded URL", status_code=400)

    if not (target.startswith("http://") or target.startswith("https://")):
        return PlainTextResponse("Expected an URL after '/'", status_code=400)

    # chave normalizada (ignora N já existente nas UTMs)
    k = normalized_key(target)
    n = _db_next(k)  # começa em 0

    final_url = add_N_to_all_utms(target, n)
    return RedirectResponse(final_url, status_code=302)
