from typing import Literal, Optional, Callable, Tuple, List, get_args, Type

from pydantic import AliasGenerator, SecretStr, AliasChoices, Field
from pydantic.functional_validators import BeforeValidator
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    PydanticBaseSettingsSource,
)
from typing_extensions import Self, Annotated

__all__ = ("ConnectionConfig",)

MECHANISM_ALIAS = AliasChoices("sasl_mechanism", "sasl_mechanisms")


class ConnectionConfig(BaseSettings):
    """
    Provides an interface for all librdkafka connection-based configs.

    Allows converting to or from a librdkafka dictionary.

    Also obscures secrets and handles any case sensitivity issues.
    """

    model_config = SettingsConfigDict(
        alias_generator=AliasGenerator(
            # used during model_dumps
            serialization_alias=lambda field_name: field_name.replace("_", "."),
        ),
    )

    bootstrap_servers: str
    security_protocol: Annotated[
        Optional[Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"]],
        BeforeValidator(lambda v: v.lower() if v is not None else v),
    ] = None

    # ----------------- SASL SETTINGS -----------------
    # Allows "sasl_mechanisms" (or "." if librdkafka) and sets it to sasl_mechanism
    sasl_mechanism: Annotated[
        Optional[
            Literal["GSSAPI", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"]
        ],
        Field(validation_alias=MECHANISM_ALIAS),
        BeforeValidator(lambda v: v.upper() if v is not None else v),
    ] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[SecretStr] = None
    sasl_kerberos_kinit_cmd: Optional[str] = None
    sasl_kerberos_keytab: Optional[str] = None
    sasl_kerberos_min_time_before_relogin: Optional[int] = None
    sasl_kerberos_service_name: Optional[str] = None
    sasl_kerberos_principal: Optional[str] = None
    # for oauth_cb, see https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-configuration
    oauth_cb: Optional[Callable[[str], Tuple[str, float]]] = None
    sasl_oauthbearer_config: Optional[str] = None
    enable_sasl_oauthbearer_unsecure_jwt: Optional[bool] = None
    oauthbearer_token_refresh_cb: Optional[Callable] = None
    sasl_oauthbearer_method: Annotated[
        Optional[Literal["default", "oidc"]],
        BeforeValidator(lambda v: v.lower() if v is not None else v),
    ] = None
    sasl_oauthbearer_client_id: Optional[str] = None
    sasl_oauthbearer_client_secret: Optional[SecretStr] = None
    sasl_oauthbearer_scope: Optional[str] = None
    sasl_oauthbearer_extensions: Optional[str] = None
    sasl_oauthbearer_token_endpoint_url: Optional[str] = None

    # ----------------- SSL SETTINGS -----------------
    ssl_cipher_suites: Optional[str] = None
    ssl_curves_list: Optional[str] = None
    ssl_sigalgs_list: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None
    ssl_key_pem: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_certificate_pem: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_ca_pem: Optional[str] = None
    ssl_ca_certificate_stores: Optional[str] = None
    ssl_crl_location: Optional[str] = None
    ssl_keystore_location: Optional[str] = None
    ssl_keystore_password: Optional[SecretStr] = None
    ssl_providers: Optional[str] = None
    ssl_engine_location: Optional[str] = None
    ssl_engine_id: Optional[str] = None
    enable_ssl_certificate_verification: Optional[bool] = None
    ssl_endpoint_identification_algorithm: Annotated[
        Optional[Literal["none", "https"]],
        BeforeValidator(lambda v: v.lower() if v is not None else v),
    ] = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Included to ignore reading/setting values from the environment
        """
        return (init_settings,)

    @classmethod
    def from_librdkafka_dict(cls, config: dict, ignore_extras: bool = False) -> Self:
        """
        Create a `ConnectionConfig` from a librdkafka config dictionary.

        :param config: a dict of configs (like {"bootstrap.servers": "url"})
        :param ignore_extras: Ignore non-connection settings (else raise exception)

        :return: a ConnectionConfig
        """
        config = {name.replace(".", "_"): v for name, v in config.items()}
        if ignore_extras:
            valid_keys = set(cls.model_fields.keys()) | set(MECHANISM_ALIAS.choices)
            config = {k: v for k, v in config.items() if k in valid_keys}
        return cls(**config)

    @property
    def _secret_fields(self) -> List[str]:
        """
        Get all the fields that are of the type "SecretStr" (passwords)

        :return: a list of secret field names
        """
        fields = []
        for name, info in self.model_fields.items():
            if SecretStr in get_args(info.annotation):
                fields.append(name)
        return fields

    def as_librdkafka_dict(self, plaintext_secrets=True) -> dict:
        """
        Dump any non-empty config values as a librdkafka dictionary.

        >***NOTE***: All secret values will be dumped in PLAINTEXT by default.

        :param plaintext_secrets: whether secret values are plaintext or obscured (***)
        :return: a librdkafka-compatible dictionary
        """
        dump = self.model_dump(by_alias=True, exclude_none=True)
        if plaintext_secrets:
            for field in self._secret_fields:
                field = field.replace("_", ".")
                if dump.get(field):
                    dump[field] = dump[field].get_secret_value()
        return dump

    def __str__(self) -> str:
        return str(self.as_librdkafka_dict(plaintext_secrets=False))
