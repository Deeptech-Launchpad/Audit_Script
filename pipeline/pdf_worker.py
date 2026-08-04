"""
Isolated PDF-parsing worker, invoked as a subprocess so the caller can enforce
a hard wall-clock timeout (some malformed/complex PDFs make pdfplumber hang
well past any network timeout, since parsing happens after download).

Usage:
    python pdf_worker.py info <path>              -> {"num_pages": N, "full_text": "..." (if N<=15)}
    python pdf_worker.py pages <path> <lo> <hi>    -> {"num_pages": N, "text": "..."}
"""
import json
import sys

import pdfplumber


def main():
    mode = sys.argv[1]
    path = sys.argv[2]
    try:
        with pdfplumber.open(path) as pdf:
            num_pages = len(pdf.pages)
            if mode == "info":
                out = {"num_pages": num_pages}
                if num_pages <= 15:
                    texts = [p.extract_text() or "" for p in pdf.pages]
                    out["full_text"] = "\n\n----PAGE BREAK----\n\n".join(
                        f"[PAGE {i+1}]\n{t}" for i, t in enumerate(texts))
                print(json.dumps(out))
            elif mode == "pages":
                lo, hi = int(sys.argv[3]), int(sys.argv[4])
                lo = max(1, lo)
                hi = min(num_pages, hi)
                parts = []
                for i in range(lo - 1, hi):
                    t = pdf.pages[i].extract_text() or ""
                    parts.append(f"[PAGE {i+1}]\n{t}")
                print(json.dumps({"num_pages": num_pages, "text": "\n\n".join(parts)}))
            else:
                print(json.dumps({"error": f"unknown mode {mode}"}))
                sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
