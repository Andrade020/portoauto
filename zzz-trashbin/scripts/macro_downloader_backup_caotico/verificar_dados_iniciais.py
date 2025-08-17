# -*- coding: utf-8 -*-
"""
inventariar_arquivos_dados.py
-----------------------------
Lista todos os arquivos em 'dados_adicionais_fidc' (ou --base),
imprimindo o caminho relativo e o tamanho (bytes e legível).
Mostra resumo por pasta raiz e um TOP N dos maiores arquivos.

Uso:
  python inventariar_arquivos_dados.py
  python inventariar_arquivos_dados.py --base "C:\...\dados_adicionais_fidc" --top 25 --sort size
"""

import os
import argparse

# ---------- Helpers ----------
def bytes_to_human(n):
    units = ["B","KB","MB","GB","TB"]
    i = 0
    x = float(n)
    while x >= 1024 and i < len(units) - 1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"

def collect_files(base_dir):
    files = []
    for root, dirs, filenames in os.walk(base_dir):
        for fn in filenames:
            p = os.path.join(root, fn)
            try:
                size = os.path.getsize(p)
            except OSError:
                size = None
            rel = os.path.relpath(p, base_dir)
            files.append((rel, size))
    return files

def group_by_root(files):
    groups = {}
    for rel, size in files:
        root = rel.split(os.sep, 1)[0] if os.sep in rel else rel
        groups.setdefault(root, []).append((rel, size))
    return groups

# ---------- Main ----------
def main():
    here = os.path.dirname(__file__)
    default_base = os.path.abspath(os.path.join(here, "..", "..", "dados_adicionais_fidc"))

    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=default_base, help="diretório base (padrão: ../../dados_adicionais_fidc)")
    ap.add_argument("--sort", choices=["name","size"], default="name", help="ordenação da listagem (padrão: name)")
    ap.add_argument("--top", type=int, default=20, help="quantos mostrar no TOP N maiores (padrão: 20)")
    args = ap.parse_args()

    base = os.path.abspath(args.base)
    print("="*60)
    print("INVENTÁRIO - DADOS ADICIONAIS (FIDC)")
    print("="*60)
    print(f"Base: {base}\n")

    if not os.path.isdir(base):
        print("ERRO: diretório base não encontrado.")
        return

    files = collect_files(base)
    total_size = sum(s or 0 for _, s in files)
    print(f"Total de arquivos: {len(files)}")
    print(f"Tamanho total: {bytes_to_human(total_size)} ({total_size:,} bytes)")

    groups = group_by_root(files)

    # Resumo por raiz
    print("\nRESUMO POR PASTA RAIZ")
    print("---------------------")
    for root in sorted(groups.keys()):
        g = groups[root]
        sz = sum(s or 0 for _, s in g)
        print(f"- {root:<20} arquivos: {len(g):>5} | tamanho: {bytes_to_human(sz):>10} ({sz:,} bytes)")

    # Listagem detalhada por raiz
    print("\nLISTAGEM DETALHADA")
    print("------------------")
    for root in sorted(groups.keys()):
        print(f"\n[{root}]")
        g = groups[root]
        if args.sort == "size":
            g = sorted(g, key=lambda x: (x[1] is None, x[1] or 0), reverse=True)
        else:
            g = sorted(g, key=lambda x: x[0].lower())
        for rel, size in g:
            sz_str = "?" if size is None else f"{bytes_to_human(size)} ({size:,} B)"
            print(f"  - {rel}  |  {sz_str}")

    # TOP N maiores
    print("\nTOP MAIORES ARQUIVOS")
    print("--------------------")
    only_sized = [(rel, s) for rel, s in files if s is not None]
    only_sized.sort(key=lambda x: x[1], reverse=True)
    topn = only_sized[:max(0, args.top)]
    for i, (rel, s) in enumerate(topn, start=1):
        print(f"{i:>2}. {rel}  |  {bytes_to_human(s)} ({s:,} B)")

if __name__ == "__main__":
    main()
