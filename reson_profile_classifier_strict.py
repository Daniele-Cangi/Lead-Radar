#!/usr/bin/env python3
"""
Reson Profile Classifier (STRICT, English only)

Usage:
  API:  python reson_profile_classifier_strict.py --serve --model_path "C:\\path\\to\\Reson4.5"
  CLI:  echo "text" | python reson_profile_classifier_strict.py --model_path "C:\\path\\to\\Reson4.5"
"""
import argparse, json, os, re, warnings, sys
from typing import Tuple, Optional
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel

MODEL_PATH = r"C:\Users\dacan\OneDrive\Desktop\Meta\Reson4.5\Reson4.5"
BASE_MODEL_NAME = "meta-llama/Llama-2-7b-chat-hf"

GEN_KWARGS = dict(
    max_new_tokens=128,
    temperature=0.1,
    do_sample=False,
    top_p=1.0,
    repetition_penalty=1.05,
    no_repeat_ngram_size=0,
    min_length=8,
)

KEYWORDS = {
    "EtherCAT": ["ethercat", "beckhoff", "twincat"],
    "PROFINET": ["profinet", "siemens tia", "gsdml", "plc siemens"],
    "EtherNet_IP": ["ethernet/ip", "ethernet ip", "rockwell", "allen bradley", "studio 5000", "aoi"],
    "ROS2": ["ros2", "gazebo", "rclcpp", "ros industrial"],
    "UR_Cobot": ["universal robots", "ur ", "cobot", "amr"],
    "Motion": ["motion control", "mechatronics", "servo", "integrated motor", "stepper"],
}

def rb_score(text: str) -> Tuple[int, str]:
    txt = text.lower()
    score = 0
    reasons = []
    for tag, kws in KEYWORDS.items():
        if any(k in txt for k in kws):
            score += 15
            reasons.append(tag)
    score = max(0, min(100, score))
    reason = "Detected tags: " + ", ".join(reasons) if reasons else "No technical tags detected"
    return score if score > 0 else 35, reason

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
        low_cpu_mem_usage=True,
    )
    model = PeftModel.from_pretrained(base_model, model_path)
    return model, tokenizer

SYSTEM = (
    "You are a classifier for technical profiles related to JVL. "
    "You must return ONLY valid JSON with the following schema: "
    '{"score": <integer 0-100>, "reason": "<short phrase in English>"} . '
    "Score = relevance to JVL (Automation/Controls, EtherCAT/PROFINET/EtherNet-IP, ROS2, UR/cobot, AMR)."
)
EXAMPLE = '{"score": 92, "reason": "Mentions EtherCAT/Beckhoff and ROS2; active technical profile."}'

def build_prompt(text: str) -> str:
    user = (
        "Evaluate the following profile and answer ONLY with valid JSON as in the example.\n"
        f"Example: {EXAMPLE}\n"
        f"Profile:\n{text}\n"
        "Output JSON:"
    )
    return f"[INST] {SYSTEM} [/INST] [INST] {user} [/INST]"

def extract_json(s: str) -> Optional[dict]:
    m = re.search(r"\{\s*\"score\"\s*:\s*\d+\s*,\s*\"reason\"\s*:\s*\".*?\"\s*\}", s, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return None
    return None

_model, _tok = None, None
def ensure_loaded(path: Optional[str] = None):
    global _model, _tok
    if _model is None or _tok is None:
        _model, _tok = load_reson_model(path or MODEL_PATH)

def classify(text: str, model_path: Optional[str] = None) -> dict:
    ensure_loaded(model_path)
    prompt = build_prompt(text)
    inputs = _tok(prompt, return_tensors="pt", padding=True, truncation=True, max_length=2048)
    inputs = {k: v.to(_model.device) for k, v in inputs.items()}
    with torch.no_grad():
        out = _model.generate(**inputs, **GEN_KWARGS)
    decoded = _tok.decode(out[0], skip_special_tokens=True)

    obj = extract_json(decoded)
    if obj is None:
        s, r = rb_score(text)
        obj = {"score": s, "reason": r}
    obj["score"] = max(0, min(100, int(obj.get("score", 50))))
    if not obj.get("reason"):
        obj["reason"] = "No reason provided"
    return obj

def run_api(host: str, port: int, model_path: Optional[str] = None):
    ensure_loaded(model_path)
    from fastapi import FastAPI
    from pydantic import BaseModel
    import uvicorn
    app = FastAPI(title="Reson Profile Classifier (STRICT)")

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
