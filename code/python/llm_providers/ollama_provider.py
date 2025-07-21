import asyncio
import json
import re
import aiohttp
import logging

from llm_providers.llm_provider import LLMProvider
from misc.logger.logging_config_helper import get_configured_logger
from core.config import CONFIG

logger = get_configured_logger("llm_ollama")


class OllamaProvider(LLMProvider):
    def get_client(self):
        return None
    async def get_completion(
        self,
        prompt: str,
        schema: dict,
        model: str = "llama2:13b",
        temperature: float = 0.7,
        max_tokens: int = 2048,
        timeout: float = 30.0,
        **kwargs
    ) -> dict:
        endpoint = CONFIG.get_llm_provider(CONFIG.preferred_llm_endpoint)
        url = f"{CONFIG.llm_endpoints['ollama_local'].get_endpoint()}/api/generate"



        payload = {
            "model": model,
            "prompt": prompt,
            "temperature": temperature,
            "num_predict": max_tokens,
            "stream": False
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=timeout) as resp:
                    data = await resp.json()
                    content = data.get("response", "")
                    logger.debug(f"Ollama response: {content}")
                    return self.clean_response(content, schema)
        except Exception as e:
            logger.exception("Ollama LLM call failed")
            raise RuntimeError(f"Ollama error: {e}")


    logger = logging.getLogger("llm_ollama")

    def clean_response(self, content: str, schema: dict) -> dict:
        try:
            # Remove Markdown code fences if present
            cleaned = re.sub(r"^```(?:json)?", "", content.strip(), flags=re.IGNORECASE)
            cleaned = re.sub(r"```$", "", cleaned.strip())

            # Extract JSON object
            match = re.search(r"(\{.*\})", cleaned, re.S)
            if not match:
                logger.error(f"No JSON found in content:\n{content}")
                raise ValueError("No JSON object found in LLM output")

            json_str = match.group(1)
            parsed = json.loads(json_str)
            return parsed
        except Exception as e:
            logger.error(f"Failed to parse LLM JSON response: {e}\nRaw content:\n{content}")
            raise

provider = OllamaProvider()
