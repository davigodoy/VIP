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
    limiar_match: float = Field(
        ge=0.0,
        le=1.0,
        description="Rigor do match facial anonimo (Re-ID): mais alto = menos fusoes entre pessoas.",
    )
    auto_cleanup_enabled: bool
    auto_cleanup_hour: int = Field(ge=0, le=23)
    camera_device: str = Field(min_length=1, max_length=255)
    camera_label: str = Field(min_length=1, max_length=255)
    camera_enabled: bool
    camera_inference_width: int = Field(ge=160, le=1920)
    camera_inference_height: int = Field(ge=120, le=1080)
    camera_fps: int = Field(ge=1, le=60)
    live_detection_enabled: bool
    camera_entry_direction: Literal["down", "up", "left", "right"] = Field(
        default="down",
        description="Direcao do movimento de quem ENTRA no frame da camera (down=desce, up=sobe, left=esquerda, right=direita).",
    )
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
    envolvimento_janela_dias: int = Field(
        ge=7,
        le=120,
        description="Janela movel (ex.: 30): todos os niveis contam dias com entrada neste periodo.",
    )
    envolvimento_max_dias_visitante: int = Field(
        ge=1,
        le=120,
        description="Inclusive: ate este numero de dias distintos com entrada = visitante.",
    )
    envolvimento_max_dias_frequentador: int = Field(
        ge=2,
        le=120,
        description="Inclusive: acima do limite de visitante ate este valor = frequentador; acima = membro.",
    )

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
        if self.envolvimento_max_dias_frequentador <= self.envolvimento_max_dias_visitante:
            raise ValueError(
                "Envolvimento: o limite de frequentador deve ser maior que o de visitante."
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


class InvolvementRulesUpdate(BaseModel):
    """Corpo JSON para gravar apenas as regras de envolvimento (sem o restante da configuracao)."""

    envolvimento_janela_dias: int = Field(ge=7, le=120)
    envolvimento_max_dias_visitante: int = Field(ge=1, le=120)
    envolvimento_max_dias_frequentador: int = Field(ge=2, le=120)

    @model_validator(mode="after")
    def validate_tiers(self) -> "InvolvementRulesUpdate":
        if self.envolvimento_max_dias_frequentador <= self.envolvimento_max_dias_visitante:
            raise ValueError(
                "Envolvimento: o limite de frequentador deve ser maior que o de visitante."
            )
        return self


class CameraDeviceSelect(BaseModel):
    """Atualiza so o dispositivo V4L2/indice usado pelo preview e pela configuracao."""

    camera_device: str = Field(min_length=1, max_length=255)


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


class ReconciliationPersonComputed(BaseModel):
    """Uma linha de service_event_people apos recomputo."""

    culto_id: str = Field(default="__global__", max_length=120)
    person_id: str = Field(min_length=1, max_length=120)
    first_seen_at: str
    last_seen_at: str
    entries_count: int = Field(ge=0)
    exits_count: int = Field(ge=0)
    returns_count: int = Field(ge=0)
    age_band: AgeBand | None = None
    gender: GenderBand | None = None
    last_direction: Literal["entrada", "saida"]
    last_exit_at: str | None = None


class ReconciliationStatsComputed(BaseModel):
    entries_count: int = Field(ge=0)
    exits_count: int = Field(ge=0)
    returns_count: int = Field(ge=0)
    unique_people_count: int = Field(ge=0)
    current_occupancy: int = Field(ge=0)
    peak_occupancy: int = Field(ge=0)
    crianca_count: int = Field(ge=0)
    junior_count: int = Field(ge=0)
    adolescente_count: int = Field(ge=0)
    jovem_count: int = Field(ge=0)
    adulto_count: int = Field(ge=0)
    idoso_count: int = Field(ge=0)
    homem_count: int = Field(ge=0)
    mulher_count: int = Field(ge=0)


class ReconciliationApplyRequest(BaseModel):
    """Resultado calculado no browser para gravar no servidor (so escrita na BD)."""

    stats: ReconciliationStatsComputed
    people: list[ReconciliationPersonComputed] = Field(default_factory=list, max_length=200_000)


class EventIngestRequest(BaseModel):
    person_id: str = Field(min_length=1, max_length=120)
    direction: Literal["entrada", "saida"]
    event_ts: datetime | None = None
    age_band: AgeBand | None = None
    age_estimate: int | None = Field(default=None, ge=0, le=120)
    gender: GenderBand | None = None


class PersonasResetRequest(BaseModel):
    """
    Reset operacional das personas identificadas.

    Mantem os eventos, mas remove identificadores de pessoa (temp_id) para impedir
    que um dia ruidoso contamine unicos/envolvimento.
    """

    reset_personas_day: str | None = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Data YYYY-MM-DD para limpar temp_id dos eventos desse dia.",
    )
    delete_day_events: bool = False
    wipe_all_personas: bool = False


class SyncAutoSetupRequest(BaseModel):
    """Configura Google Sheets com minimo de interacao no painel."""

    spreadsheet: str = Field(min_length=3, max_length=800)
    worksheet_name: str = Field(default="", max_length=120)
    enable_sync: bool = True
    run_test_sync: bool = True
