"""
Great Expectations тесты для контроля качества данных
"""

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataQualityTests:
    """Класс для выполнения тестов качества данных"""
    
    def __init__(self, data_context_config=None):
        """Инициализация контекста Great Expectations"""
        if data_context_config is None:
            data_context_config = DataContextConfig(
                config_version=3.0,
                datasources={
                    "pandas_datasource": {
                        "class_name": "Datasource",
                        "execution_engine": {
                            "class_name": "PandasExecutionEngine"
                        },
                        "data_connectors": {
                            "default_runtime_data_connector": {
                                "class_name": "RuntimeDataConnector",
                                "batch_identifiers": ["default_identifier_name"]
                            }
                        }
                    }
                },
                stores={
                    "expectations_store": {
                        "class_name": "ExpectationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": "/opt/airflow/data/expectations"
                        }
                    },
                    "validations_store": {
                        "class_name": "ValidationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": "/opt/airflow/data/validations"
                        }
                    },
                    "evaluation_parameter_store": {
                        "class_name": "EvaluationParameterStore"
                    }
                },
                expectations_store_name="expectations_store",
                validations_store_name="validations_store",
                evaluation_parameter_store_name="evaluation_parameter_store",
                checkpoint_store_name="checkpoint_store",
                data_docs_sites={
                    "local_site": {
                        "class_name": "SiteBuilder",
                        "show_how_to_buttons": True,
                        "site_index_builder": {
                            "class_name": "DefaultSiteIndexBuilder"
                        },
                        "renderers": {
                            "class_name": "DefaultSiteRenderer"
                        }
                    }
                },
                anonymous_usage_statistics={
                    "enabled": False
                }
            }
        
        self.context = BaseDataContext(data_context_config)
    
    def test_reestr_po_quality(self, df):
        """Тесты качества данных реестра ПО"""
        logger.info("Running quality tests for reestr_po data")
        
        # Создание batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="reestr_po",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "reestr_po_batch"}
        )
        
        # Создание suite
        suite = self.context.create_expectation_suite(
            expectation_suite_name="reestr_po_suite",
            overwrite_existing=True
        )
        
        # Добавление ожиданий
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="reestr_po_suite"
        )
        
        # Тесты обязательных полей
        validator.expect_column_to_exist("solution_code")
        validator.expect_column_to_exist("solution_name")
        validator.expect_column_to_exist("vendor_inn")
        validator.expect_column_to_exist("is_domestic")
        
        # Тесты уникальности
        validator.expect_column_values_to_be_unique("solution_code")
        
        # Тесты на null
        validator.expect_column_values_to_not_be_null("solution_code")
        validator.expect_column_values_to_not_be_null("solution_name")
        validator.expect_column_values_to_not_be_null("vendor_inn")
        validator.expect_column_values_to_not_be_null("is_domestic")
        
        # Тесты формата ИНН
        validator.expect_column_values_to_match_regex(
            "vendor_inn", 
            r"^\d{10}$|^\d{12}$",
            mostly=0.95  # 95% записей должны соответствовать
        )
        
        # Тесты булевых значений
        validator.expect_column_values_to_be_in_set(
            "is_domestic", 
            [True, False]
        )
        
        # Тесты дат
        validator.expect_column_values_to_be_between(
            "register_decision_date",
            min_value="2015-01-01",
            max_value="2025-12-31"
        )
        
        # Сохранение suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        
        # Выполнение валидации
        checkpoint = self.context.add_checkpoint(
            name="reestr_po_checkpoint",
            config={
                "class_name": "SimpleCheckpoint",
                "validations": [
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": "reestr_po_suite"
                    }
                ]
            }
        )
        
        result = checkpoint.run()
        return result
    
    def test_eis_procurement_quality(self, df):
        """Тесты качества данных ЕИС"""
        logger.info("Running quality tests for eis_procurement data")
        
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="eis_procurement",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "eis_procurement_batch"}
        )
        
        suite = self.context.create_expectation_suite(
            expectation_suite_name="eis_procurement_suite",
            overwrite_existing=True
        )
        
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="eis_procurement_suite"
        )
        
        # Обязательные поля
        validator.expect_column_to_exist("notice_id")
        validator.expect_column_to_exist("customer_inn")
        validator.expect_column_to_exist("sum_rub")
        validator.expect_column_to_exist("publish_date")
        
        # Уникальность
        validator.expect_column_values_to_be_unique("notice_id")
        
        # Тесты на null
        validator.expect_column_values_to_not_be_null("notice_id")
        validator.expect_column_values_to_not_be_null("customer_inn")
        validator.expect_column_values_to_not_be_null("sum_rub")
        validator.expect_column_values_to_not_be_null("publish_date")
        
        # Тесты сумм
        validator.expect_column_values_to_be_between(
            "sum_rub",
            min_value=0,
            max_value=1000000000000  # 1 трлн рублей
        )
        
        # Тесты дат
        validator.expect_column_values_to_be_between(
            "publish_date",
            min_value="2015-01-01",
            max_value="2025-12-31"
        )
        
        # Тесты ИНН
        validator.expect_column_values_to_match_regex(
            "customer_inn",
            r"^\d{10}$|^\d{12}$",
            mostly=0.95
        )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        
        checkpoint = self.context.add_checkpoint(
            name="eis_procurement_checkpoint",
            config={
                "class_name": "SimpleCheckpoint",
                "validations": [
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": "eis_procurement_suite"
                    }
                ]
            }
        )
        
        result = checkpoint.run()
        return result
    
    def test_fedstat_macro_quality(self, df):
        """Тесты качества данных ЕМИСС"""
        logger.info("Running quality tests for fedstat_macro data")
        
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="fedstat_macro",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "fedstat_macro_batch"}
        )
        
        suite = self.context.create_expectation_suite(
            expectation_suite_name="fedstat_macro_suite",
            overwrite_existing=True
        )
        
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="fedstat_macro_suite"
        )
        
        # Обязательные поля
        validator.expect_column_to_exist("indicator_code")
        validator.expect_column_to_exist("indicator_name")
        validator.expect_column_to_exist("value")
        validator.expect_column_to_exist("period")
        
        # Тесты на null
        validator.expect_column_values_to_not_be_null("indicator_code")
        validator.expect_column_values_to_not_be_null("indicator_name")
        validator.expect_column_values_to_not_be_null("value")
        validator.expect_column_values_to_not_be_null("period")
        
        # Тесты значений
        validator.expect_column_values_to_be_between(
            "value",
            min_value=-999999999999,
            max_value=999999999999
        )
        
        # Тесты дат
        validator.expect_column_values_to_be_between(
            "period",
            min_value="2015-01-01",
            max_value="2025-12-31"
        )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        
        checkpoint = self.context.add_checkpoint(
            name="fedstat_macro_checkpoint",
            config={
                "class_name": "SimpleCheckpoint",
                "validations": [
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": "fedstat_macro_suite"
                    }
                ]
            }
        )
        
        result = checkpoint.run()
        return result
    
    def test_gisp_support_quality(self, df):
        """Тесты качества данных ГИСП"""
        logger.info("Running quality tests for gisp_support data")
        
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="gisp_support",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "gisp_support_batch"}
        )
        
        suite = self.context.create_expectation_suite(
            expectation_suite_name="gisp_support_suite",
            overwrite_existing=True
        )
        
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="gisp_support_suite"
        )
        
        # Обязательные поля
        validator.expect_column_to_exist("support_id")
        validator.expect_column_to_exist("program_code")
        validator.expect_column_to_exist("recipient_inn")
        validator.expect_column_to_exist("amount_rub")
        validator.expect_column_to_exist("approval_date")
        
        # Уникальность
        validator.expect_column_values_to_be_unique("support_id")
        
        # Тесты на null
        validator.expect_column_values_to_not_be_null("support_id")
        validator.expect_column_values_to_not_be_null("program_code")
        validator.expect_column_values_to_not_be_null("recipient_inn")
        validator.expect_column_values_to_not_be_null("amount_rub")
        validator.expect_column_values_to_not_be_null("approval_date")
        
        # Тесты сумм
        validator.expect_column_values_to_be_between(
            "amount_rub",
            min_value=0,
            max_value=10000000000  # 10 млрд рублей
        )
        
        # Тесты дат
        validator.expect_column_values_to_be_between(
            "approval_date",
            min_value="2015-01-01",
            max_value="2025-12-31"
        )
        
        # Тесты ИНН
        validator.expect_column_values_to_match_regex(
            "recipient_inn",
            r"^\d{10}$|^\d{12}$",
            mostly=0.95
        )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        
        checkpoint = self.context.add_checkpoint(
            name="gisp_support_checkpoint",
            config={
                "class_name": "SimpleCheckpoint",
                "validations": [
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": "gisp_support_suite"
                    }
                ]
            }
        )
        
        result = checkpoint.run()
        return result
    
    def run_all_tests(self, data_dict):
        """Запуск всех тестов качества данных"""
        results = {}
        
        for source_name, df in data_dict.items():
            try:
                if source_name == "reestr_po":
                    results[source_name] = self.test_reestr_po_quality(df)
                elif source_name == "eis_procurement":
                    results[source_name] = self.test_eis_procurement_quality(df)
                elif source_name == "fedstat_macro":
                    results[source_name] = self.test_fedstat_macro_quality(df)
                elif source_name == "gisp_support":
                    results[source_name] = self.test_gisp_support_quality(df)
                else:
                    logger.warning(f"No quality tests defined for {source_name}")
            except Exception as e:
                logger.error(f"Error running quality tests for {source_name}: {str(e)}")
                results[source_name] = {"success": False, "error": str(e)}
        
        return results
    
    def generate_quality_report(self, results):
        """Генерация отчета о качестве данных"""
        report = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "total_sources": len(results),
            "successful_tests": sum(1 for r in results.values() if r.get("success", False)),
            "failed_tests": sum(1 for r in results.values() if not r.get("success", False)),
            "details": {}
        }
        
        for source_name, result in results.items():
            if result.get("success", False):
                validation_result = result.get("run_results", {}).get("validation_result", {})
                statistics = validation_result.get("statistics", {})
                
                report["details"][source_name] = {
                    "status": "PASSED",
                    "evaluated_expectations": statistics.get("evaluated_expectations", 0),
                    "successful_expectations": statistics.get("successful_expectations", 0),
                    "unsuccessful_expectations": statistics.get("unsuccessful_expectations", 0),
                    "success_percent": statistics.get("success_percent", 0)
                }
            else:
                report["details"][source_name] = {
                    "status": "FAILED",
                    "error": result.get("error", "Unknown error")
                }
        
        return report
