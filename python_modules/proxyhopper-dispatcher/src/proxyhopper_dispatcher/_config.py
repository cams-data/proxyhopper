import yaml
from pathlib import Path
from typing import List, Dict, Union
from pydantic import BaseModel, Field

class TargetUrlConfig(BaseModel):
    min_request_interval: float = Field(default=0.0)

class ProxyhopperConfig(BaseModel):
    proxies: List[str]
    quarantine_time: int = Field(default=30)
    max_quarantine_strikes: int = Field(default=3)
    target_urls: Dict[str, TargetUrlConfig] = Field(default_factory=dict)
    max_retries: int = Field(default=3)
    health_check_interval: float = Field(default=30)

    @classmethod
    def from_yaml(cls, path: Union[str, Path] = "proxyhopper.yaml") -> "ProxyhopperConfig":
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, "r") as f:
            raw = yaml.safe_load(f)
        return cls.model_validate(raw)