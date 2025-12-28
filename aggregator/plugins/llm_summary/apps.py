from aggregator.core.apps import AppConfig


class LlmSummaryConfig(AppConfig):
    name = "llm_summary"
    verbose_name = "LLM Summary"
    service_class_path = "aggregator.plugins.llm_summary.services.LlmSummaryService"
