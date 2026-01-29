from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


"""
CSV/Excel -> JSON/JSONL converter (large-file friendly)

Pilares (CTO):
- Streaming: processa registro a registro (baixo uso de RAM)
- Robustez: suporta campos grandes, encodings variados, headers duplicados
- Observabilidade: tqdm (opcional) + progress.json para UI/painel
- Entrega: split do output por N partes (garantidas) ou por X registros
- DX: gera exemplos reais + template + keys antes do processamento completo
"""


# -------------------------------------------------------------------
# CSV hardening: suporta campos gigantes (evita _csv.Error field limit)
# -------------------------------------------------------------------
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)


# -------------------------------------------------------------------
# Dependências opcionais
# -------------------------------------------------------------------
try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None

try:
    import openpyxl  # type: ignore
except ImportError:
    openpyxl = None


# -------------------------------------------------------------------
# Progresso em arquivo para polling (UI) + informações operacionais
# -------------------------------------------------------------------
class ProgressReporter:
    def __init__(self, total: int, progress_path: str = "progress.json", flush_every: int = 2000):
        self.total = max(0, int(total))
        self.path = Path(progress_path)
        self.flush_every = max(1, int(flush_every))
        self.count = 0
        self.start = time.time()
        self._last_flush = 0
        self.write(status="started", percent=0, processed=0, total=self.total)

    def tick(self, n: int = 1):
        self.count += n
        if (self.count - self._last_flush) >= self.flush_every:
            self._last_flush = self.count
            percent = int((self.count / self.total) * 100) if self.total else 0
            self.write(status="processing", percent=percent, processed=self.count, total=self.total)

    def done(
        self,
        out_file: str,
        parts: Optional[List[str]] = None,
        examples: Optional[List[str]] = None,
    ):
        payload = {
            "status": "done",
            "percent": 100,
            "processed": self.count,
            "total": self.total if self.total else self.count,
            "output": out_file,
        }
        if parts:
            payload["parts"] = parts
        if examples:
            payload["examples"] = examples
        self.write(**payload)

    def fail(self, error: str):
        self.write(status="error", percent=None, processed=self.count, total=self.total, error=error)

    def write(self, **data):
        data["elapsed_s"] = round(time.time() - self.start, 2)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)


def count_csv_rows(path: str, encoding: str) -> int:
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        total = sum(1 for _ in f)
    return max(0, total - 1)


# -------------------------------------------------------------------
# Normalização de texto (remove invisíveis que quebram ingestão)
# -------------------------------------------------------------------
_REMOVE_CHARS = {"\ufeff", "\u200b", "\u200c", "\u200d", "\u2060"}
_REPLACE_WITH_SPACE = {"\u00a0", "\u202f", "\u2009"}


def clean_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    s = value
    for ch in _REMOVE_CHARS:
        s = s.replace(ch, "")
    for ch in _REPLACE_WITH_SPACE:
        s = s.replace(ch, " ")
    return s


# -------------------------------------------------------------------
# Tipagem opcional (safe-ish)
# -------------------------------------------------------------------
_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)$")


def infer_type(s: str, empty_as_null: bool = False) -> Any:
    if s == "":
        return None if empty_as_null else ""

    low = s.strip().lower()
    if low == "true":
        return True
    if low == "false":
        return False
    if low in {"null", "none"}:
        return None

    t = s.strip()
    if _INT_RE.match(t):
        digits = t.lstrip("+-")
        if len(digits) > 1 and digits.startswith("0"):
            return s
        if len(digits) > 15:
            return s
        try:
            return int(t)
        except ValueError:
            return s

    if _FLOAT_RE.match(t):
        try:
            return float(t)
        except ValueError:
            return s

    return s


def json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


# -------------------------------------------------------------------
# CSV: autodetecção de encoding e delimitador (sobrescrevível via CLI)
# -------------------------------------------------------------------
def detect_encoding(path: str, prefer: Optional[str] = None) -> str:
    if prefer:
        return prefer
    candidates = ["utf-8-sig", "utf-8", "utf-16", "cp1252", "latin-1"]
    for enc in candidates:
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                f.read(4096)
            return enc
        except UnicodeDecodeError:
            continue
    return "utf-8"


def detect_delimiter(path: str, encoding: str, prefer: Optional[str] = None) -> str:
    if prefer:
        return prefer
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        sample = f.read(65536)
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return ","


def make_unique_headers(headers: List[str]) -> Tuple[List[str], bool]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    had_dupes = False
    for h in headers:
        base = h if h is not None else ""
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            had_dupes = True
            seen[base] += 1
            out.append(f"{base}__{seen[base]}")
    return out, had_dupes


# -------------------------------------------------------------------
# Output: split (nomes e cálculo)
# -------------------------------------------------------------------
def build_part_name(out_path: str, part_idx: int) -> str:
    base, ext = os.path.splitext(out_path)
    return f"{base}.part{part_idx:03d}{ext}"


def ceil_div(a: int, b: int) -> int:
    if b <= 0:
        return 0
    return (a + b - 1) // b


def compute_split_every(total: int, split_parts: Optional[int], split_every: Optional[int]) -> Optional[int]:
    if split_every and split_every > 0:
        return int(split_every)
    if split_parts and split_parts > 0:
        if total <= 0:
            return None
        return max(1, ceil_div(total, split_parts))
    return None


# -------------------------------------------------------------------
# DX: exemplos (preview real + template + keys)
# -------------------------------------------------------------------
def examples_base(out_path: str, examples_prefix: Optional[str]) -> str:
    if examples_prefix:
        return examples_prefix
    base, _ext = os.path.splitext(out_path)
    return base


def make_template_record(keys: List[str], fill_mode: str) -> Dict[str, Any]:
    fill = None if fill_mode == "null" else ""
    return {k: fill for k in keys}


def take_n_items(items: Iterable[Dict[str, Any]], n: int) -> Tuple[List[Dict[str, Any]], Iterable[Dict[str, Any]]]:
    buf: List[Dict[str, Any]] = []
    it = iter(items)
    for _ in range(n):
        try:
            buf.append(next(it))
        except StopIteration:
            break
    return buf, it


def write_keys_file(keys: List[str], path: str, ensure_ascii: bool) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fp:
        fp.write(json.dumps(keys, ensure_ascii=ensure_ascii, indent=2))


# -------------------------------------------------------------------
# Writers: streaming + split (GARANTINDO N PARTES quando split-parts)
# -------------------------------------------------------------------
def write_json_array_stream(items: Iterable[Any], out_fp, ensure_ascii: bool, pretty: bool) -> None:
    indent = 2 if pretty else None
    out_fp.write("[")
    first = True
    for item in items:
        if first:
            first = False
        else:
            out_fp.write(",")
        out_fp.write("\n" if pretty else "")
        out_fp.write(json.dumps(item, ensure_ascii=ensure_ascii, indent=indent))
    out_fp.write("\n" if pretty else "")
    out_fp.write("]")


def write_jsonl_stream(items: Iterable[Any], out_fp, ensure_ascii: bool) -> None:
    for item in items:
        out_fp.write(json.dumps(item, ensure_ascii=ensure_ascii))
        out_fp.write("\n")


def write_jsonl_split_stream(
    items: Iterable[Any],
    out_path: str,
    ensure_ascii: bool,
    split_every: int,
    force_parts: Optional[int] = None,
) -> List[str]:
    part_paths: List[str] = []
    part = 1
    written = 0

    def open_part(p: int):
        path = build_part_name(out_path, p)
        fp = open(path, "w", encoding="utf-8", newline="")
        part_paths.append(path)
        return fp

    out_fp = open_part(part)

    try:
        for item in items:
            out_fp.write(json.dumps(item, ensure_ascii=ensure_ascii))
            out_fp.write("\n")
            written += 1

            if written >= split_every:
                if force_parts is not None and part >= force_parts:
                    continue

                out_fp.close()
                part += 1
                written = 0
                out_fp = open_part(part)
    finally:
        try:
            out_fp.close()
        except Exception:
            pass

    if force_parts is not None:
        while len(part_paths) < force_parts:
            p = len(part_paths) + 1
            path = build_part_name(out_path, p)
            open(path, "w", encoding="utf-8", newline="").close()
            part_paths.append(path)
    else:
        if part_paths:
            last = part_paths[-1]
            if os.path.exists(last) and os.path.getsize(last) == 0:
                os.remove(last)
                part_paths.pop()

    return part_paths


def write_json_array_split_stream(
    items: Iterable[Any],
    out_path: str,
    ensure_ascii: bool,
    pretty: bool,
    split_every: int,
    force_parts: Optional[int] = None,
) -> List[str]:
    indent = 2 if pretty else None
    part_paths: List[str] = []
    part = 1
    written = 0

    def open_part(p: int):
        path = build_part_name(out_path, p)
        fp = open(path, "w", encoding="utf-8", newline="")
        fp.write("[")
        part_paths.append(path)
        return fp

    out_fp = open_part(part)
    first_in_part = True

    try:
        for item in items:
            if not first_in_part:
                out_fp.write(",")
            out_fp.write("\n" if pretty else "")
            out_fp.write(json.dumps(item, ensure_ascii=ensure_ascii, indent=indent))
            first_in_part = False
            written += 1

            if written >= split_every:
                if force_parts is not None and part >= force_parts:
                    continue

                out_fp.write("\n" if pretty else "")
                out_fp.write("]")
                out_fp.close()

                part += 1
                written = 0
                first_in_part = True
                out_fp = open_part(part)
    finally:
        try:
            out_fp.write("\n" if pretty else "")
            out_fp.write("]")
            out_fp.close()
        except Exception:
            pass

    if force_parts is not None:
        while len(part_paths) < force_parts:
            p = len(part_paths) + 1
            path = build_part_name(out_path, p)
            with open(path, "w", encoding="utf-8", newline="") as fp:
                fp.write("[]")
            part_paths.append(path)
    else:
        if part_paths:
            last = part_paths[-1]
            if os.path.exists(last) and os.path.getsize(last) <= 3:
                os.remove(last)
                part_paths.pop()

    return part_paths


# -------------------------------------------------------------------
# Readers: streaming de origem (CSV e XLSX)
# -------------------------------------------------------------------
def iter_csv_records(
    path: str,
    encoding: str,
    delimiter: str,
    clean: bool,
    clean_headers: bool,
    infer_types_flag: bool,
    empty_as_null: bool,
    progress: Optional[ProgressReporter] = None,
    pbar: Any = None,
) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        try:
            raw_headers = next(reader)
        except StopIteration:
            return

        headers = [str(h) for h in raw_headers]
        if clean_headers:
            headers = [clean_text(h) for h in headers]
        headers, _ = make_unique_headers(headers)

        for row in reader:
            obj: Dict[str, Any] = {}
            for i, val in enumerate(row):
                key = headers[i] if i < len(headers) else f"__extra_{i - len(headers) + 1}"
                v: Any = val
                if clean:
                    v = clean_text(v)
                if infer_types_flag and isinstance(v, str):
                    v = infer_type(v, empty_as_null=empty_as_null)
                obj[key] = v

            if len(row) < len(headers):
                fill = None if empty_as_null else ""
                for j in range(len(row), len(headers)):
                    obj[headers[j]] = fill

            if progress:
                progress.tick(1)
            if pbar is not None:
                pbar.update(1)

            yield obj


def iter_xlsx_records(
    path: str,
    sheet: Optional[str],
    header_row: int,
    clean: bool,
    clean_headers: bool,
    infer_types_flag: bool,
    empty_as_null: bool,
    progress: Optional[ProgressReporter] = None,
    pbar: Any = None,
) -> Iterable[Dict[str, Any]]:
    if openpyxl is None:
        raise RuntimeError("openpyxl não está instalado. Rode: pip install openpyxl")

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active if sheet is None else (wb.worksheets[int(sheet)] if sheet.isdigit() else wb[sheet])

    rows = ws.iter_rows(values_only=True)

    for _ in range(max(0, header_row - 1)):
        next(rows, None)

    header = next(rows, None)
    if header is None:
        return

    headers = ["" if h is None else str(h) for h in header]
    if clean_headers:
        headers = [clean_text(h) for h in headers]
    headers, _ = make_unique_headers(headers)

    for row in rows:
        obj: Dict[str, Any] = {}
        row_vals = list(row) if row is not None else []
        for i, val in enumerate(row_vals):
            key = headers[i] if i < len(headers) else f"__extra_{i - len(headers) + 1}"
            v: Any = json_safe(val)
            if clean and isinstance(v, str):
                v = clean_text(v)
            if infer_types_flag and isinstance(v, str):
                v = infer_type(v, empty_as_null=empty_as_null)
            obj[key] = v

        if len(row_vals) < len(headers):
            fill = None if empty_as_null else ""
            for j in range(len(row_vals), len(headers)):
                obj[headers[j]] = fill

        if progress:
            progress.tick(1)
        if pbar is not None:
            pbar.update(1)

        yield obj


# -------------------------------------------------------------------
# CLI Entrypoint
# -------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(
        description="Converte CSV ou Excel (.xlsx) para JSON/JSONL com streaming, progresso, split e exemplos."
    )
    p.add_argument("input", help="Caminho do arquivo .csv/.tsv/.xlsx")
    p.add_argument("-o", "--output", help="Arquivo de saída .json/.jsonl (default: mesmo nome com .json)")
    p.add_argument("--format", choices=["auto", "csv", "xlsx"], default="auto")
    p.add_argument("--sheet", default=None, help="Aba do Excel por nome OU índice (0,1,2...). Default: aba ativa")
    p.add_argument("--header-row", type=int, default=1)

    p.add_argument("--jsonl", action="store_true", help="Saída em JSON Lines (1 objeto por linha)")
    p.add_argument("--pretty", action="store_true", help="JSON identado (somente modo array)")

    p.add_argument("--encoding", default=None, help="Força encoding do CSV (ex: utf-8-sig, latin-1)")
    p.add_argument("--delimiter", default=None, help="Força delimitador do CSV (ex: ; , \\t |)")

    p.add_argument("--no-clean", action="store_true", help="Desativa limpeza de caracteres invisíveis")
    p.add_argument("--no-clean-headers", action="store_true", help="Não limpa cabeçalhos")
    p.add_argument("--infer-types", action="store_true", help="Tenta converter true/false/números/null")
    p.add_argument("--empty-as-null", action="store_true", help="Converte '' vazio para null (None)")

    p.add_argument("--ensure-ascii", action="store_true", help="Escapa UTF-8 no JSON (default: mantém UTF-8)")

    p.add_argument("--progress-file", default="progress.json", help="Arquivo para reportar progresso")
    p.add_argument("--flush-every", type=int, default=2000, help="Atualiza progress.json a cada N registros")
    p.add_argument("--no-terminal-progress", action="store_true", help="Desativa barra de progresso no terminal")

    split_group = p.add_mutually_exclusive_group()
    split_group.add_argument("--split-parts", type=int, default=None, help="Divide a saída em N partes (GARANTIDAS)")
    split_group.add_argument("--split-every", type=int, default=None, help="Divide a saída a cada X registros")

    p.add_argument(
        "--examples",
        type=int,
        default=10,
        help="Gera N exemplos reais + N templates antes da conversão (0 desativa). Default: 10",
    )
    p.add_argument("--examples-prefix", default=None, help="Prefixo para arquivos de exemplo (default: base do output)")
    p.add_argument("--template-fill", choices=["empty", "null"], default="empty", help="Template: empty ou null")
    p.add_argument("--no-keys-file", action="store_true", help="Não gera arquivo com as chaves/colunas detectadas")

    args = p.parse_args()

    in_path = args.input
    if not os.path.exists(in_path):
        print(f"Arquivo não encontrado: {in_path}", file=sys.stderr)
        return 2

    out_path = args.output if args.output else (os.path.splitext(in_path)[0] + (".jsonl" if args.jsonl else ".json"))

    ext = os.path.splitext(in_path.lower())[1]
    fmt = args.format
    if fmt == "auto":
        if ext in [".csv", ".tsv", ".txt"]:
            fmt = "csv"
        elif ext in [".xlsx"]:
            fmt = "xlsx"
        else:
            print("Não consegui detectar o formato. Use --format csv ou --format xlsx.", file=sys.stderr)
            return 2

    clean = not args.no_clean
    clean_headers = not args.no_clean_headers
    ensure_ascii = bool(args.ensure_ascii)
    pretty = bool(args.pretty)

    total_rows = 0
    enc_tmp = None
    if fmt == "csv":
        enc_tmp = detect_encoding(in_path, args.encoding)
        total_rows = count_csv_rows(in_path, enc_tmp)

    progress = ProgressReporter(total=total_rows, progress_path=args.progress_file, flush_every=args.flush_every)

    pbar = None
    if not args.no_terminal_progress and tqdm is not None:
        pbar = tqdm(total=total_rows if total_rows > 0 else None, unit="rows", desc="Convertendo")

    split_every_eff = compute_split_every(total_rows, args.split_parts, args.split_every)
    force_parts = args.split_parts if args.split_parts and args.split_parts > 0 else None

    example_files: List[str] = []

    try:
        if fmt == "csv":
            enc = enc_tmp or detect_encoding(in_path, args.encoding)
            delim = detect_delimiter(in_path, enc, args.delimiter)
            items = iter_csv_records(
                in_path,
                enc,
                delim,
                clean,
                clean_headers,
                args.infer_types,
                args.empty_as_null,
                progress=progress,
                pbar=pbar,
            )
        else:
            if openpyxl is None:
                raise RuntimeError("Para Excel (.xlsx), instale openpyxl: pip install openpyxl")
            items = iter_xlsx_records(
                in_path,
                args.sheet,
                args.header_row,
                clean,
                clean_headers,
                args.infer_types,
                args.empty_as_null,
                progress=progress,
                pbar=pbar,
            )

        if args.examples and args.examples > 0:
            ex_base = examples_base(out_path, args.examples_prefix)
            first_items, items = take_n_items(items, args.examples)

            if progress:
                progress.tick(len(first_items))
            if pbar is not None:
                pbar.update(len(first_items))

            keys = list(first_items[0].keys()) if first_items else []

            real_path = f"{ex_base}.examples.real.{len(first_items)}" + (".jsonl" if args.jsonl else ".json")
            tpl_path = f"{ex_base}.examples.template.{args.examples}" + (".jsonl" if args.jsonl else ".json")
            keys_path = f"{ex_base}.examples.keys.json"

            with open(real_path, "w", encoding="utf-8", newline="") as fp:
                if args.jsonl:
                    write_jsonl_stream(first_items, fp, ensure_ascii=ensure_ascii)
                else:
                    write_json_array_stream(first_items, fp, ensure_ascii=ensure_ascii, pretty=pretty)
            example_files.append(real_path)

            template_records = [make_template_record(keys, args.template_fill) for _ in range(args.examples)]
            with open(tpl_path, "w", encoding="utf-8", newline="") as fp:
                if args.jsonl:
                    write_jsonl_stream(template_records, fp, ensure_ascii=ensure_ascii)
                else:
                    write_json_array_stream(template_records, fp, ensure_ascii=ensure_ascii, pretty=pretty)
            example_files.append(tpl_path)

            if not args.no_keys_file:
                write_keys_file(keys, keys_path, ensure_ascii=ensure_ascii)
                example_files.append(keys_path)

        part_paths: Optional[List[str]] = None

        if split_every_eff is not None:
            if args.jsonl:
                part_paths = write_jsonl_split_stream(
                    items,
                    out_path,
                    ensure_ascii=ensure_ascii,
                    split_every=split_every_eff,
                    force_parts=force_parts,
                )
            else:
                part_paths = write_json_array_split_stream(
                    items,
                    out_path,
                    ensure_ascii=ensure_ascii,
                    pretty=pretty,
                    split_every=split_every_eff,
                    force_parts=force_parts,
                )
        else:
            with open(out_path, "w", encoding="utf-8", newline="") as out_fp:
                if args.jsonl:
                    write_jsonl_stream(items, out_fp, ensure_ascii=ensure_ascii)
                else:
                    write_json_array_stream(items, out_fp, ensure_ascii=ensure_ascii, pretty=pretty)

        if pbar is not None:
            pbar.close()

        progress.done(out_path, parts=part_paths, examples=example_files)

        if part_paths:
            print("OK! Gerado em partes:")
            for pth in part_paths:
                print(f" - {pth}")
        else:
            print(f"OK! Gerado: {out_path}")

        if example_files:
            print("Exemplos gerados:")
            for pth in example_files:
                print(f" - {pth}")

        print(f"Progresso: {args.progress_file}")
        return 0

    except Exception as e:
        if pbar is not None:
            pbar.close()
        progress.fail(str(e))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
