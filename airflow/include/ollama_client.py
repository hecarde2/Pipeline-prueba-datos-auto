import os
import json
import requests

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct")


def generate(prompt: str, system: str | None = None, temperature: float = 0.2, max_tokens: int = 512):
    url = f"{OLLAMA_URL}/api/generate"
    payload = {"model": MODEL, "prompt": prompt, "stream": False, "options": {"temperature": temperature, "num_predict": max_tokens}}
    if system:
        payload["system"] = system
    try:
        r = requests.post(url, json=payload, timeout=300)
        r.raise_for_status()
        data = r.json()
        return data.get("response", "")
    except Exception as e:
        raise
