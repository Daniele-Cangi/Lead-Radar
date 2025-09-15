#!/usr/bin/env python3
"""
Reson Profile Classifier — STRICT v2 (English only, hybrid scoring)
- No examples in the prompt (prevents copying).
- Hybrid: rule-based prior + model score -> combined score.
- Enforces informative 'reason' with detected tags.
"""
import argparse, json, os, re, warnings, sys, math, time, random
from typing import Tuple, Optional, List, Dict
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

# ---------------- CONFIG ----------------
MODEL_PATH = r"C:\Users\dacan\OneDrive\Desktop\Meta\Reson4.5\Reson4.5"
BASE_MODEL_NAME = "meta-llama/Llama-2-7b-chat-hf"
# If 4-bit causes trouble on your machine, switch to CPU fallback in load_reson_model()

GEN_KWARGS = dict(
    max_new_tokens=120,
    temperature=0.0,      # fully deterministic
    do_sample=False,
    top_p=1.0,
    repetition_penalty=1.05,
    no_repeat_ngram_size=0,
)

KEYWORDS: Dict[str, List[str]] = {
    "EtherCAT":    ["ethercat", "beckhoff", "twincat"],
    "PROFINET":    ["profinet", "siemens tia", "gsdml", "tia portal", "plc siemens"],
    "EtherNet_IP": ["ethernet/ip", "ethernet ip", "rockwell", "allen bradley", "studio 5000", "aoi"],
    "ROS2":        ["ros2", "gazebo", "rclcpp", "ros industrial", "ros-industrial"],
    "UR_Cobot":    ["universal robots", " ur ", "cobot", "urcap", "amr", "mobile robot"],
    "Motion":      ["motion control", "mechatronics", "servo", "integrated motor", "stepper"],
    "Modbus":      ["modbus", "modbus tcp", "modbus rtu"],
}

# ---------------- RULE PRIOR ----------------
def detect_tags(text: str) -> List[str]:
    t = (text or "").lower()
    out = []
    for tag, kws in KEYWORDS.items():
        if any(k in t for k in kws):
            out.append(tag)
    # dedupe keep order
    seen=set(); res=[]
    for x in out:
        if x not in seen:
            seen.add(x); res.append(x)
    return res

def rb_prior(text: str) -> int:
    t = (text or "").lower()
    score = 0
    for _, kws in KEYWORDS.items():
        if any(k in t for k in kws):
            score += 15
    if "ethercat" in t and "ros2" in t:
        score += 10  # synergy bonus
    score = max(0, min(100, score))
    return score if score>0 else 35

# ---------------- LOADER ----------------
def load_reson_model(model_path: str = MODEL_PATH):
    warnings.filterwarnings("ignore", category=UserWarning)
    # 4-bit load (toggle to CPU fallback if needed)
    bnb = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_use_double_quant=True,
    )
    tok = AutoTokenizer.from_pretrained(BASE_MODEL_NAME, trust_remote_code=True)
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token
        tok.pad_token_id = tok.eos_token_id
    base = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL_NAME,
        quantization_config=bnb,
        torch_dtype=torch.float16,
        device_map="auto",
        trust_remote_code=True,
        use_cache=False,
        low_cpu_mem_usage=True,
    )
    model = PeftModel.from_pretrained(base, model_path)
    return model, tok

# CPU fallback (uncomment to force CPU)
# def load_reson_model(model_path: str = MODEL_PATH):
#     warnings.filterwarnings("ignore", category=UserWarning)
#     tok = AutoTokenizer.from_pretrained(BASE_MODEL_NAME, trust_remote_code=True)
#     if tok.pad_token is None:
#         tok.pad_token = tok.eos_token
#         tok.pad_token_id = tok.eos_token_id
#     base = AutoModelForCausalLM.from_pretrained(
#         BASE_MODEL_NAME,
#         torch_dtype=torch.float32,
#         device_map="cpu",
#         trust_remote_code=True,
#         low_cpu_mem_usage=True,
#     )
#     model = PeftModel.from_pretrained(base, model_path)
#     return model, tok

SYSTEM = (
    "You are a strict classifier for technical profiles related to JVL.\n"
    "- Return ONLY valid JSON (no extra text, no backticks).\n"
    '- Schema: {"score": <integer 0-100>, "reason": "<short English phrase>"}\n'
    "- Score = relevance to JVL (Automation/Controls, EtherCAT/PROFINET/EtherNet-IP, ROS2, UR/cobot, AMR).\n"
    "- The reason MUST mention at least one concrete technology if present (e.g., EtherCAT, PROFINET, ROS2, UR).\n"
    "- Do not copy templates or examples; compute from the provided profile only."
)

def build_prompt(text: str) -> str:
    user = (
        "Evaluate the following profile.\n"
        "Return ONLY a JSON object with fields 'score' and 'reason'.\n"
        "Profile text:\n"
        f"{text}\n"
        "JSON:"
    )
    return f"[INST] {SYSTEM} [/INST] [INST] {user} [/INST]"

def extract_json(s: str) -> Optional[dict]:
    m = re.search(r'\{\s*"score"\s*:\s*\d+\s*,\s*"reason"\s*:\s*".*?"\s*\}', s, flags=re.DOTALL)
    if not m: return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None

_model, _tok = None, None
def ensure_loaded(path: Optional[str] = None):
    global _model, _tok
    if _model is None or _tok is None:
        _model, _tok = load_reson_model(path or MODEL_PATH)

# ---------------- CLASSIFY ----------------
_last_signature = None

def classify(text: str, model_path: Optional[str] = None) -> dict:
    global _last_signature
    ensure_loaded(model_path)

    prior = rb_prior(text)
    tags = detect_tags(text)

    prompt = build_prompt(text)
    inputs = _tok(prompt, return_tensors="pt", padding=True, truncation=True, max_length=2048)
    inputs = {k: v.to(_model.device) for k, v in inputs.items()}
    with torch.no_grad():
        out = _model.generate(**inputs, **GEN_KWARGS)
    decoded = _tok.decode(out[0], skip_special_tokens=True)

    obj = extract_json(decoded) or {}
    # Normalize
    try:
        m_score = int(obj.get("score", 50))
    except Exception:
        m_score = 50
    m_score = max(0, min(100, m_score))
    m_reason = str(obj.get("reason", "")).strip()

    # Combine with prior (hybrid)
    if tags:
        combined = int(round(0.65*m_score + 0.35*prior))
        # enforce mention of at least one tag in reason
        if not any(t.lower() in m_reason.lower() for t in tags):
            m_reason = f"Mentions {', '.join(tags)}; profile appears relevant."
    else:
        # if no tags detected, cap optimism
        combined = min(m_score, 60)
        if not m_reason:
            m_reason = "No specific technologies detected."

    # Anti-copy: degrade identical generic reasons across calls
    sig = (combined, m_reason.lower().strip())
    if _last_signature == sig:
        # force slight variation using tags or generic rationale
        if tags:
            m_reason = f"Includes {', '.join(tags)}; experience aligns with JVL stack."
        else:
            m_reason = "Generic automation profile; limited evidence."
        combined = max(35, combined - 5)
    _last_signature = (combined, m_reason.lower().strip())

    return {"score": max(0, min(100, combined)), "reason": m_reason}

# ---------------- API / CLI ----------------
def run_api(host: str, port: int, model_path: Optional[str] = None):
    ensure_loaded(model_path)
    from fastapi import FastAPI
    from pydantic import BaseModel
    import uvicorn
    app = FastAPI(title="Reson Profile Classifier (STRICT v2)")

    class Req(BaseModel):
        text: str

    @app.post("/classify")
    def _classify(req: Req):
        return classify(req.text, model_path=model_path)

    uvicorn.run(app, host=host, port=port)

def run_cli(model_path: Optional[str] = None):
    ensure_loaded(model_path)
    text = sys.stdin.read().strip()
    if not text:
        print(json.dumps({"error": "No text on stdin"}, ensure_ascii=False)); return
    print(json.dumps(classify(text, model_path=model_path), ensure_ascii=False))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8089)
    ap.add_argument("--model_path", default=MODEL_PATH)
    args = ap.parse_args()
    if args.serve:
        run_api(args.host, args.port, args.model_path)
    else:
        run_cli(args.model_path)

if __name__ == "__main__":
    main()
