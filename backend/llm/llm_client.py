"""
llm_client.py
Groq LLM client wrapper with retry logic, rate-limit handling, and structured output parsing.
Used by the Architect agent for AI-driven topology design.

Model: llama-3.3-70b-versatile on Groq free tier
Limits: ~30 req/min, 14,400 req/day (more than enough for hackathon)
"""
import os
import json
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Lazy import — only fail when actually called, not at module load
_groq_client = None


def _get_client():
    """Lazy-init Groq client. Fails gracefully if not installed or no key."""
    global _groq_client
    if _groq_client is not None:
        return _groq_client
    try:
        # Load .env file if python-dotenv is installed
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass  # dotenv not installed — fall back to OS env vars

        from groq import Groq
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            logger.warning("GROQ_API_KEY not set — LLM calls will fail, falling back to rules")
            return None
        _groq_client = Groq(api_key=api_key)
        return _groq_client
    except ImportError:
        logger.warning("groq package not installed — pip install groq")
        return None
    except Exception as e:
        logger.warning(f"Failed to init Groq client: {e}")
        return None


def call_llm(
    system_prompt: str,
    user_prompt: str,
    max_retries: int = 2,
    temperature: float = 0.1,
    max_tokens: int = 4096,
    model: str = "llama-3.3-70b-versatile",
) -> Optional[dict]:
    """
    Call Groq LLM and parse JSON response.

    Returns parsed dict on success, None on failure (triggers fallback).
    Never raises — all errors are caught and logged.
    """
    client = _get_client()
    if client is None:
        logger.info("No Groq client available — returning None for fallback")
        return None

    for attempt in range(max_retries + 1):
        try:
            logger.info(f"LLM call attempt {attempt + 1}/{max_retries + 1} "
                        f"(model={model}, temp={temperature})")

            completion = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
                response_format={"type": "json_object"},
                max_tokens=max_tokens,
                timeout=45,
            )

            raw = completion.choices[0].message.content
            if not raw or not raw.strip():
                raise ValueError("Empty response from LLM")

            result = json.loads(raw)
            logger.info(f"LLM call succeeded on attempt {attempt + 1}")
            return result

        except json.JSONDecodeError as e:
            logger.warning(f"LLM returned invalid JSON (attempt {attempt + 1}): {e}")
            if attempt < max_retries:
                # Append error feedback so LLM can self-correct
                user_prompt += (
                    f"\n\nYour previous response was not valid JSON. "
                    f"Error: {e}. Please return ONLY valid JSON matching the schema."
                )
                time.sleep(1)
                continue
            else:
                logger.error("LLM JSON parsing failed after all retries")
                return None

        except Exception as e:
            error_str = str(e).lower()

            # Rate limit — back off and retry
            if "rate_limit" in error_str or "429" in error_str:
                wait = min(5 * (attempt + 1), 30)
                logger.warning(f"Rate limited — waiting {wait}s before retry")
                time.sleep(wait)
                if attempt < max_retries:
                    continue

            # Timeout — retry with shorter prompt or give up
            if "timeout" in error_str or "timed out" in error_str:
                logger.warning(f"LLM call timed out (attempt {attempt + 1})")
                if attempt < max_retries:
                    time.sleep(2)
                    continue

            logger.error(f"LLM call failed (attempt {attempt + 1}): {e}")
            if attempt >= max_retries:
                return None
            time.sleep(1)

    return None


def validate_architect_response(result: dict) -> list:
    """
    Validate that the LLM response has all required keys.
    Returns list of missing keys (empty = valid).
    """
    required = [
        "design_decisions",
        "adrs",
        "target_app_assignments",
        "qms_to_remove",
        "qms_to_keep",
        "required_connections",
    ]
    return [k for k in required if k not in result]
