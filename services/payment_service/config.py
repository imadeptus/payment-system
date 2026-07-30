"""Environment-driven Payment Service settings."""

from pydantic import PositiveInt, SecretStr

from libs.settings import RuntimeSettings


class PaymentSettings(RuntimeSettings):
    payment_database_url: SecretStr
    payment_commands_topic: str = "payments.commands.v1"
    payment_events_topic: str = "payments.events.v1"
    dlq_topic: str = "saga.dlq.v1"
    reject_order_ids: str = ""
    refund_failure_order_ids: str = ""
    refund_transient_failure_amount_minor: PositiveInt | None = None
