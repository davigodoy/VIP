from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator

AgeBand = Literal["crianca", "junior", "adolescente", "jovem", "adulto", "idoso"]
GenderBand = Literal["homem", "mulher"]


class RetentionConfig(BaseModel):
    retencao_temp_id_horas: int = Field(ge=0, le=720)
    retencao_profile_dias: int = Field(ge=1, le=3650)
    retencao_eventos_dias: int = Field(ge=1, le=3650)
    retencao_agregados_meses: int = Field(ge=1, le=120)
    retencao_imagens_horas: int = Field(ge=0, le=720)
    janela_reentrada_min: int = Field(ge=1, le=240)
    limiar_match: float = Field(ge=0.0, le=1.0)
    auto_cleanup_enabled: bool
    auto_cleanup_hour: int = Field(ge=0, le=23)
    camera_device: str = Field(min_length=1, max_length=255)
    camera_label: str = Field(min_length=1, max_length=255)
    camera_enabled: bool
    camera_inference_width: int = Field(ge=160, le=1920)
    camera_inference_height: int = Field(ge=120, le=1080)
    camera_fps: int = Field(ge=1, le=60)
    contagem_continua_enabled: bool = True
    contagem_intervalo_min: int = Field(ge=5, le=240)
    culto_antecedencia_min: int = Field(ge=0, le=180)
    culto_duracao_min: int = Field(ge=30, le=360)
    estimar_faixa_etaria: bool
    estimar_genero: bool
    sync_google_sheets_enabled: bool
    sync_interval_sec: int = Field(ge=30, le=3600)
    sync_spreadsheet_id: str = Field(max_length=255)
    sync_worksheet_name: str = Field(min_length=1, max_length=100)
    sync_credentials_source: Literal["env", "file", "inline"] = "env"
    sync_credentials_env_var: str = Field(max_length=120)
    sync_credentials_file_path: str = Field(max_length=400)
    sync_credentials_json: str = Field(max_length=50000)
    idade_limite_crianca: int = Field(ge=1, le=30)
    idade_limite_junior: int = Field(ge=2, le=40)
    idade_limite_adolescente: int = Field(ge=5, le=60)
    idade_limite_jovem: int = Field(ge=10, le=80)
    idade_limite_adulto: int = Field(ge=10, le=100)

    @model_validator(mode="after")
    def validate_age_limits(self) -> "RetentionConfig":
        limits = [
            self.idade_limite_crianca,
            self.idade_limite_junior,
            self.idade_limite_adolescente,
            self.idade_limite_jovem,
            self.idade_limite_adulto,
        ]
        if limits != sorted(limits) or len(set(limits)) != len(limits):
            raise ValueError(
                "Limites de idade devem ser crescentes: crianca < junior < adolescente < jovem < adulto"
            )
        if (
            self.sync_credentials_source == "file"
            and not self.sync_credentials_file_path.strip()
        ):
            raise ValueError(
                "Quando a fonte de credencial for arquivo, informe o caminho do arquivo."
            )
        if (
            self.sync_credentials_source == "env"
            and not self.sync_credentials_env_var.strip()
        ):
            raise ValueError(
                "Quando a fonte de credencial for variavel de ambiente, informe o nome da variavel."
            )
        return self


class CleanupRequest(BaseModel):
    dry_run: bool = True


class ServiceScheduleCreate(BaseModel):
    service_name: str = Field(min_length=1, max_length=120)
    day_of_week: int = Field(ge=0, le=6)
    start_time: str = Field(pattern=r"^\d{2}:\d{2}$")
    is_active: bool = True


class ServiceScheduleUpdate(BaseModel):
    service_name: str = Field(min_length=1, max_length=120)
    day_of_week: int = Field(ge=0, le=6)
    start_time: str = Field(pattern=r"^\d{2}:\d{2}$")
    is_active: bool = True


class ServiceScheduleOut(BaseModel):
    id: int
    service_name: str
    day_of_week: int
    start_time: str
    is_active: bool


class EventIngestRequest(BaseModel):
    person_id: str = Field(min_length=1, max_length=120)
    direction: Literal["entrada", "saida"]
    event_ts: datetime | None = None
    age_band: AgeBand | None = None
    age_estimate: int | None = Field(default=None, ge=0, le=120)
    gender: GenderBand | None = None
