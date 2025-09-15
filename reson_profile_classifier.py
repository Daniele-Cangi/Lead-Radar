
#!/usr/bin/env python3
"""
Reson Profile Classifier — uses your Reson 4.5 chat model as a lightweight scorer.
Modes:
  - CLI:  echo "text" | python reson_profile_classifier.py
  - API:  python reson_profile_classifier.py --serve --host 127.0.0.1 --port 8089
"""

import argparse
import json
import os
import re
import warnings
from typing import Tuple, Optional

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

# ---------- CONFIG ----------

MODEL_PATH = r"C:\Users\dacan\OneDrive\Desktop\Meta\Reson4.5\Reson4.5"
BASE_MODEL_NAME = "meta-llama/Llama-2-7b-chat-hf"

GEN_KWARGS = dict(
    max_new_tokens=256,
    temperature=0.4,
    do_sample=True,
    top_p=0.92,
    top_k=40,
    repetition_penalty=1.1,
    no_repeat_ngram_size=3,
    min_length=10,
)

# ---------- LOADER ----------

def load_reson_model(model_path: str = MODEL_PATH):
    warnings.filterwarnings("ignore", category=UserWarning)

    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_use_double_quant=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL_NAME, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
        tokenizer.pad_token_id = tokenizer.eos_token_id

    base_model = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL_NAME,
        quantization_config=bnb_config,
        torch_dtype=torch.float16,
        device_map="auto",
        trust_remote_code=True,
        use_cache=False,
        low_cpu_mem_usage=True
    )

    model = PeftModel.from_pretrained(base_model, model_path)
    return model, tokenizer

# ---------- PROMPTING ----------

SYSTEM_RULES = (
    "Sei un classificatore per profili tecnici JVL. "
    "Dato un testo (headline + about + activity di un profilo) devi restituire SOLO un JSON valido: "
    "{\"score\": <intero 0-100>, \"reason\": \"<frase breve>\"}. "
    "Il punteggio misura la rilevanza per JVL (Automation/Controls, EtherCAT/PROFINET/EtherNet-IP, ROS2, UR/cobot, AMR). "
    "Scrivi output in italiano semplice. Niente testo fuori dal JSON."
)

def build_prompt(text: str) -> str:
    user_q = (
        "Valuta questo profilo per JVL e restituisci SOLO JSON:\n"
        f"{text}\n\n"
        "Schema: {\"score\": 0-100, \"reason\": \"...\" }"
    )
    return f"[INST] {SYSTEM_RULES} [/INST] [INST] {user_q} [/INST]"

def generate_json(model, tokenizer, prompt: str) -> Tuple[int, str]:
    inputs = tokenizer(prompt, return_tensors="pt", padding=True, truncation=True, max_length=2048)
    inputs = {k: v.to(model.device) for k, v in inputs.items()}

    with torch.no_grad():
        out = model.generate(**inputs, **GEN_KWARGS)
    decoded = tokenizer.decode(out[0], skip_special_tokens=True)

    m = re.search(r"\{.*\}", decoded, flags=re.DOTALL)
    if not m:
        return 50, "Output non strutturato; fallback neutrale."
    raw = m.group(0).strip().strip("`").strip()
    try:
        obj = json.loads(raw)
        score = int(obj.get("score", 50))
        reason = str(obj.get("reason", "Motivo non disponibile")).strip()
    except Exception:
        score = 50
        reason = "Impossibile fare il parse JSON; fallback."
    score = max(0, min(100, score))
    return score, reason

# ---------- SERVICE ----------

_model = None
_tokenizer = None

def ensure_loaded(path: Optional[str] = None):
    global _model, _tokenizer
    if _model is None or _tokenizer is None:
        _model, _tokenizer = load_reson_model(path or MODEL_PATH)

def classify_text(text: str, model_path: Optional[str] = None) -> dict:
    ensure_loaded(model_path)
    prompt = build_prompt(text)
    score, reason = generate_json(_model, _tokenizer, prompt)
    return {"score": score, "reason": reason}

def run_cli(model_path: Optional[str] = None):
    ensure_loaded(model_path)
    import sys
    text = sys.stdin.read().strip()
    if not text:
        print(json.dumps({"error": "Nessun testo fornito su stdin"}, ensure_ascii=False))
        return
    result = classify_text(text, model_path=model_path)
    print(json.dumps(result, ensure_ascii=False))

def run_api(host: str, port: int, model_path: Optional[str] = None):
    ensure_loaded(model_path)
    from fastapi import FastAPI
    from pydantic import BaseModel
    import uvicorn

    app = FastAPI(title="Reson Profile Classifier")

    class Req(BaseModel):
        text: str

    @app.post("/classify")
    def classify(req: Req):
        res = classify_text(req.text, model_path=model_path)
        return res

    uvicorn.run(app, host=host, port=port)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true", help="Start HTTP API")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8089)
    ap.add_argument("--model_path", default=MODEL_PATH)
    args = ap.parse_args()

    if args.serve:
        run_api(args.host, args.port, model_path=args.model_path)
    else:
        run_cli(model_path=args.model_path)

if __name__ == "__main__":
    main()
